import os
import json
import time
import hmac
import hashlib
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta, timezone

import httpx
from fastapi import FastAPI, Request, Query, Header, HTTPException
from fastapi.responses import PlainTextResponse, JSONResponse

# =========================
# ENV
# =========================
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "")

ACCOUNT_ID = os.environ.get("DOTA_ACCOUNT_ID")  # 32-bit account_id (не steam64)

START_MMR = int(os.environ.get("START_MMR", "13772"))
MMR_STEP = int(os.environ.get("MMR_STEP", "25"))
TZ_OFFSET_HOURS = int(os.environ.get("TZ_OFFSET_HOURS", "3"))

UPSTASH_URL = os.environ.get("UPSTASH_REDIS_REST_URL")
UPSTASH_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN")

STEAM_API_KEY = os.environ.get("STEAM_API_KEY", "")

TWITCH_CLIENT_ID = os.environ.get("TWITCH_CLIENT_ID", "")
TWITCH_CLIENT_SECRET = os.environ.get("TWITCH_CLIENT_SECRET", "")
TWITCH_BROADCASTER_LOGIN = os.environ.get("TWITCH_BROADCASTER_LOGIN", "")  # debustie
EVENTSUB_SECRET = os.environ.get("EVENTSUB_SECRET", "")

# =========================
# CONSTANTS
# =========================
STEAM_BASE = "https://api.steampowered.com"
TWITCH_OAUTH = "https://id.twitch.tv/oauth2/token"
TWITCH_API = "https://api.twitch.tv/helix"

CACHE_TTL = 10  # сек
MAX_MATCHES_PER_CALL = 5  # чтоб не ловить таймауты/429

app = FastAPI()
_cache_text: Optional[str] = None
_cache_ts: float = 0.0


# =========================
# TIME HELPERS
# =========================
def tz_msk():
    return timezone(timedelta(hours=TZ_OFFSET_HOURS))


def fmt_signed(n: int) -> str:
    return f"+{n}" if n >= 0 else str(n)


def iso_to_unix(iso_str: str) -> int:
    # Twitch started_at: "2024-01-01T12:34:56Z"
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return int(dt.timestamp())
    except Exception:
        return 0


# =========================
# AUTH HELPERS
# =========================
def require_admin(token: str):
    if not ADMIN_TOKEN:
        raise HTTPException(status_code=500, detail="ADMIN_TOKEN not set")
    if token != ADMIN_TOKEN:
        raise HTTPException(status_code=403, detail="Forbidden")


# =========================
# UPSTASH REDIS
# =========================
async def redis_get_json(key: str):
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(
            f"{UPSTASH_URL}/get/{key}",
            headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
        )
        r.raise_for_status()
        data = r.json()
        val = data.get("result")
        if val is None:
            return None
        return json.loads(val)


async def redis_set_json(key: str, obj: dict):
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(
            f"{UPSTASH_URL}/set/{key}",
            headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
            data=json.dumps(obj, ensure_ascii=False),
        )
        r.raise_for_status()


# =========================
# STATE
# =========================
def default_state() -> dict:
    return {
        "start_mmr": START_MMR,
        "mmr": START_MMR,
        "last_start_time": 0,      # последний обработанный ranked матч по start_time
        "processed_ids": [],       # match_id, чтобы не пересчитывать
        # stream session stats
        "stream_active": False,
        "stream_start_time": 0,    # unix
        "stream_win": 0,
        "stream_lose": 0,
        "stream_delta": 0,
    }


def ensure_state_fields(state: dict) -> dict:
    base = default_state()
    for k, v in base.items():
        if k not in state:
            state[k] = v
    if not isinstance(state.get("processed_ids", []), list):
        state["processed_ids"] = []
    return state


# =========================
# STEAM DOTA API
# =========================
async def steam_get_match_history(account_id: str, matches_requested: int = 15) -> dict:
    url = f"{STEAM_BASE}/IDOTA2Match_570/GetMatchHistory/v1/"
    params = {
        "key": STEAM_API_KEY,
        "account_id": account_id,
        "matches_requested": matches_requested,
    }
    async with httpx.AsyncClient(timeout=25) as client:
        r = await client.get(url, params=params)
        r.raise_for_status()
        return r.json()


async def steam_get_match_details(match_id: int) -> Dict[str, Any]:
    # безопасно: не роняем /mmr из-за 429/плохого json
    url = f"{STEAM_BASE}/IDOTA2Match_570/GetMatchDetails/v1/"
    params = {"key": STEAM_API_KEY, "match_id": match_id}
    async with httpx.AsyncClient(timeout=25) as client:
        r = await client.get(url, params=params)
        if r.status_code != 200:
            return {"_error": f"status={r.status_code}", "_text": r.text[:300]}
        try:
            data = r.json()
        except Exception:
            return {"_error": "bad_json", "_text": r.text[:300]}
        return (data or {}).get("result") or {}


def match_is_ranked_lobby7(m: dict) -> bool:
    # lobby_type=7 - ranked matchmaking
    try:
        return int(m.get("lobby_type", -1)) == 7
    except Exception:
        return False


def did_player_win(details: dict, account_id: int) -> Optional[bool]:
    # details: { radiant_win: bool, players: [ {account_id, player_slot, ...}, ... ] }
    if not details or details.get("_error"):
        return None
    radiant_win = details.get("radiant_win")
    players = details.get("players") or []
    if radiant_win is None or not players:
        return None

    player_slot = None
    for p in players:
        if int(p.get("account_id", -1)) == int(account_id):
            player_slot = p.get("player_slot")
            break

    if player_slot is None:
        return None

    is_radiant = int(player_slot) < 128
    return bool(radiant_win) if is_radiant else (not bool(radiant_win))


async def fetch_latest_ranked_start_time(account_id: str) -> int:
    # baseline: последний ranked матч (чтоб не пересчитывать всю историю при last_start_time=0)
    try:
        mh = await steam_get_match_history(account_id, matches_requested=15)
        result = (mh or {}).get("result") or {}
        matches = result.get("matches") or []
        ranked = [m for m in matches if match_is_ranked_lobby7(m)]
        if not ranked:
            return 0
        # matches обычно идут от новых к старым
        st = int(ranked[0].get("start_time", 0))
        return st
    except Exception:
        return 0


# =========================
# TWITCH HELPERS (EventSub)
# =========================
async def twitch_app_token() -> str:
    if not (TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET):
        raise HTTPException(status_code=500, detail="Twitch client not configured")
    async with httpx.AsyncClient(timeout=25) as client:
        r = await client.post(
            TWITCH_OAUTH,
            params={
                "client_id": TWITCH_CLIENT_ID,
                "client_secret": TWITCH_CLIENT_SECRET,
                "grant_type": "client_credentials",
            },
        )
        r.raise_for_status()
        return r.json().get("access_token", "")


async def twitch_get_user_id(login: str, token: str) -> str:
    async with httpx.AsyncClient(timeout=25) as client:
        r = await client.get(
            f"{TWITCH_API}/users",
            params={"login": login},
            headers={"Client-Id": TWITCH_CLIENT_ID, "Authorization": f"Bearer {token}"},
        )
        r.raise_for_status()
        data = r.json()
        arr = data.get("data") or []
        return arr[0]["id"] if arr else ""


async def twitch_list_subscriptions(token: str) -> List[dict]:
    async with httpx.AsyncClient(timeout=25) as client:
        r = await client.get(
            f"{TWITCH_API}/eventsub/subscriptions",
            headers={"Client-Id": TWITCH_CLIENT_ID, "Authorization": f"Bearer {token}"},
        )
        r.raise_for_status()
        return (r.json() or {}).get("data") or []


async def twitch_create_subscription(token: str, sub_type: str, broadcaster_id: str, callback_url: str):
    body = {
        "type": sub_type,
        "version": "1",
        "condition": {"broadcaster_user_id": broadcaster_id},
        "transport": {
            "method": "webhook",
            "callback": callback_url,
            "secret": EVENTSUB_SECRET,
        },
    }
    async with httpx.AsyncClient(timeout=25) as client:
        r = await client.post(
            f"{TWITCH_API}/eventsub/subscriptions",
            headers={
                "Client-Id": TWITCH_CLIENT_ID,
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json=body,
        )
        # если уже есть — Twitch может вернуть 409, это не критично
        if r.status_code not in (202, 409):
            raise HTTPException(status_code=500, detail=f"Failed create sub {sub_type}: {r.status_code} {r.text[:200]}")


def verify_eventsub_signature(
    secret: str,
    message_id: str,
    message_ts: str,
    body_bytes: bytes,
    signature_header: str,
) -> bool:
    if not (secret and message_id and message_ts and signature_header):
        return False
    msg = message_id.encode() + message_ts.encode() + body_bytes
    digest = hmac.new(secret.encode(), msg, hashlib.sha256).hexdigest()
    expected = f"sha256={digest}"
    return hmac.compare_digest(expected, signature_header)


# =========================
# ROUTES
# =========================
@app.get("/health", response_class=PlainTextResponse)
async def health():
    # UptimeRobot может слать HEAD — FastAPI сам обработает.
    return "ok"


@app.get("/eventsub_setup", response_class=PlainTextResponse)
async def eventsub_setup(request: Request, token: str = Query("")):
    require_admin(token)

    if not EVENTSUB_SECRET:
        return "EVENTSUB_SECRET not set"
    if not TWITCH_BROADCASTER_LOGIN:
        return "TWITCH_BROADCASTER_LOGIN not set"
    if not (TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET):
        return "TWITCH_CLIENT_ID / TWITCH_CLIENT_SECRET not set"

    app_token = await twitch_app_token()
    broadcaster_id = await twitch_get_user_id(TWITCH_BROADCASTER_LOGIN, app_token)
    if not broadcaster_id:
        return "broadcaster_id not found"

    base = str(request.base_url).rstrip("/")
    callback = f"{base}/eventsub"

    subs = await twitch_list_subscriptions(app_token)
    have_online = any(s.get("type") == "stream.online" and (s.get("condition") or {}).get("broadcaster_user_id") == broadcaster_id for s in subs)
    have_offline = any(s.get("type") == "stream.offline" and (s.get("condition") or {}).get("broadcaster_user_id") == broadcaster_id for s in subs)

    if not have_online:
        await twitch_create_subscription(app_token, "stream.online", broadcaster_id, callback)
    if not have_offline:
        await twitch_create_subscription(app_token, "stream.offline", broadcaster_id, callback)

    return f"OK\ncallback: {callback}\nonline: {'exists' if have_online else 'created'}\noffline: {'exists' if have_offline else 'created'}\n"


@app.post("/eventsub")
async def eventsub_webhook(
    request: Request,
    twitch_eventsub_message_id: str = Header(default=""),
    twitch_eventsub_message_timestamp: str = Header(default=""),
    twitch_eventsub_message_signature: str = Header(default=""),
    twitch_eventsub_message_type: str = Header(default=""),
):
    body = await request.body()

    # Verify signature
    if twitch_eventsub_message_type != "webhook_callback_verification":
        ok = verify_eventsub_signature(
            EVENTSUB_SECRET,
            twitch_eventsub_message_id,
            twitch_eventsub_message_timestamp,
            body,
            twitch_eventsub_message_signature,
        )
        if not ok:
            raise HTTPException(status_code=403, detail="Bad signature")

    payload = {}
    try:
        payload = json.loads(body.decode("utf-8"))
    except Exception:
        payload = {}

    # Challenge
    if twitch_eventsub_message_type == "webhook_callback_verification":
        challenge = payload.get("challenge", "")
        return PlainTextResponse(challenge)

    # Notifications
    sub = payload.get("subscription") or {}
    ev = payload.get("event") or {}
    sub_type = sub.get("type", "")

    # State in Redis
    if not (UPSTASH_URL and UPSTASH_TOKEN and ACCOUNT_ID):
        return JSONResponse({"ok": True})

    state_key = f"mmr:{ACCOUNT_ID}"
    state = await redis_get_json(state_key) or {}
    state = ensure_state_fields(state)

    now_unix = int(time.time())

    if sub_type == "stream.online":
        # stream started_at может быть в event.started_at
        started_at = ev.get("started_at", "")
        st = iso_to_unix(started_at) or now_unix

        state["stream_active"] = True
        state["stream_start_time"] = st
        state["stream_win"] = 0
        state["stream_lose"] = 0
        state["stream_delta"] = 0

        # ВАЖНО: чтобы не пересчитывать древнее, но и чтобы новые матчи не пропускать:
        # baseline ставим на максимум из текущего last_start_time и (stream_start_time - 1)
        state["last_start_time"] = max(int(state.get("last_start_time", 0)), st - 1)

        await redis_set_json(state_key, state)

    elif sub_type == "stream.offline":
        state["stream_active"] = False
        await redis_set_json(state_key, state)

    return JSONResponse({"ok": True})


@app.get("/streamstatus", response_class=PlainTextResponse)
async def streamstatus(token: str = Query("")):
    require_admin(token)

    if not (UPSTASH_URL and UPSTASH_TOKEN and ACCOUNT_ID):
        return "not configured"

    state_key = f"mmr:{ACCOUNT_ID}"
    state = await redis_get_json(state_key) or {}
    state = ensure_state_fields(state)

    lines = [
        f"stream_active={state.get('stream_active')}",
        f"stream_start_time={state.get('stream_start_time')}",
        f"stream_win={state.get('stream_win')}",
        f"stream_lose={state.get('stream_lose')}",
        f"stream_delta={state.get('stream_delta')}",
        f"mmr={state.get('mmr')}",
        f"last_start_time={state.get('last_start_time')}",
        f"processed_ids_count={len(state.get('processed_ids', []))}",
    ]
    return "\n".join(lines)


@app.get("/debug_last_matches", response_class=PlainTextResponse)
async def debug_last_matches(token: str = Query("")):
    require_admin(token)

    if not ACCOUNT_ID:
        return "DOTA_ACCOUNT_ID not set"
    if not STEAM_API_KEY:
        return "STEAM_API_KEY not set"
    if not (UPSTASH_URL and UPSTASH_TOKEN):
        return "Redis not set"

    mh = await steam_get_match_history(ACCOUNT_ID, matches_requested=15)
    result = (mh or {}).get("result") or {}
    matches = result.get("matches") or []

    state_key = f"mmr:{ACCOUNT_ID}"
    state = await redis_get_json(state_key) or {}
    state = ensure_state_fields(state)

    out = []
    out.append(f"account_id={ACCOUNT_ID}")
    out.append(f"got_matches={len(matches)}")
    out.append("last_15:")
    for m in matches:
        out.append(
            f"- match_id={m.get('match_id')} start_time={m.get('start_time')} lobby_type={m.get('lobby_type')}"
        )

    out.append("\nstate:")
    out.append(f"last_start_time={state.get('last_start_time')}")
    out.append(f"processed_ids_count={len(state.get('processed_ids', []))}")
    out.append(f"stream_active={state.get('stream_active')}")
    out.append(f"stream_start_time={state.get('stream_start_time')}")
    out.append(f"mmr={state.get('mmr')}")

    return "\n".join(out)


@app.get("/mmr", response_class=PlainTextResponse)
async def mmr():
    global _cache_text, _cache_ts

    now = time.time()
    if _cache_text and (now - _cache_ts) < CACHE_TTL:
        return _cache_text

    # basic checks
    if not ACCOUNT_ID:
        return "DOTA_ACCOUNT_ID не установлен"
    if not STEAM_API_KEY:
        return "STEAM_API_KEY не установлен"
    if not UPSTASH_URL or not UPSTASH_TOKEN:
        return "Redis не настроен"

    state_key = f"mmr:{ACCOUNT_ID}"
    state = await redis_get_json(state_key)
    if not state:
        state = default_state()
        await redis_set_json(state_key, state)
    state = ensure_state_fields(state)

    # если state совсем пустой/старый — ставим baseline на последний ranked матч
    if int(state.get("last_start_time", 0)) == 0 and len(state.get("processed_ids", [])) == 0:
        baseline = await fetch_latest_ranked_start_time(ACCOUNT_ID)
        state["last_start_time"] = baseline
        await redis_set_json(state_key, state)

    # грузим последние матчи
    mh = await steam_get_match_history(ACCOUNT_ID, matches_requested=15)
    result = (mh or {}).get("result") or {}
    matches = result.get("matches") or []

    processed = set(int(x) for x in (state.get("processed_ids") or []) if str(x).isdigit())
    last_time = int(state.get("last_start_time", 0))

    stream_start = int(state.get("stream_start_time", 0))
    stream_active = bool(state.get("stream_active", False))

    # собираем новые ranked матчи по start_time
    new_list: List[Tuple[int, int]] = []
    for m in matches:
        if not match_is_ranked_lobby7(m):
            continue
        mid = m.get("match_id")
        st = m.get("start_time")
        if not mid or not st:
            continue
        mid = int(mid)
        st = int(st)

        if mid in processed:
            continue
        if st <= last_time:
            continue

        # если есть старт стрима — считаем только матчи после него
        if stream_start > 0 and st < stream_start:
            continue

        new_list.append((st, mid))

    new_list.sort(key=lambda x: x[0])

    # чтобы не ловить 500 — ограничиваем обработку
    for st, mid in new_list[:MAX_MATCHES_PER_CALL]:
        details = await steam_get_match_details(mid)
        if details.get("_error"):
            # Steam мог вернуть 429/ошибку — просто пропускаем
            continue

        won = did_player_win(details, int(ACCOUNT_ID))
        if won is None:
            continue

        delta = MMR_STEP if won else -MMR_STEP

        # общий mmr всегда меняем
        state["mmr"] = int(state.get("mmr", START_MMR)) + delta

        # stream stats меняем только если есть stream_start_time (или активен стрим)
        if stream_start > 0 and st >= stream_start:
            if won:
                state["stream_win"] = int(state.get("stream_win", 0)) + 1
            else:
                state["stream_lose"] = int(state.get("stream_lose", 0)) + 1
            state["stream_delta"] = int(state.get("stream_delta", 0)) + delta

        processed.add(mid)
        state["last_start_time"] = max(int(state.get("last_start_time", 0)), st)

    # обрезаем историю processed
    state["processed_ids"] = list(processed)[-300:]

    await redis_set_json(state_key, state)

    cur = int(state.get("mmr", START_MMR))
    sw = int(state.get("stream_win", 0))
    sl = int(state.get("stream_lose", 0))
    sd = int(state.get("stream_delta", 0))

    text = f"MMR: {cur} • Today -> Win: {sw} Lose: {sl} • Total: {fmt_signed(sd)}"
    _cache_text, _cache_ts = text, now
    return text


# =========================
# ADMIN TEST/TOOLS
# =========================
@app.get("/testwin", response_class=PlainTextResponse)
async def testwin(token: str = Query("")):
    require_admin(token)
    if not (UPSTASH_URL and UPSTASH_TOKEN and ACCOUNT_ID):
        return "not configured"

    state_key = f"mmr:{ACCOUNT_ID}"
    state = await redis_get_json(state_key) or {}
    state = ensure_state_fields(state)

    state["mmr"] = int(state.get("mmr", START_MMR)) + MMR_STEP
    state["stream_win"] = int(state.get("stream_win", 0)) + 1
    state["stream_delta"] = int(state.get("stream_delta", 0)) + MMR_STEP

    await redis_set_json(state_key, state)
    return "WIN added"


@app.get("/testlose", response_class=PlainTextResponse)
async def testlose(token: str = Query("")):
    require_admin(token)
    if not (UPSTASH_URL and UPSTASH_TOKEN and ACCOUNT_ID):
        return "not configured"

    state_key = f"mmr:{ACCOUNT_ID}"
    state = await redis_get_json(state_key) or {}
    state = ensure_state_fields(state)

    state["mmr"] = int(state.get("mmr", START_MMR)) - MMR_STEP
    state["stream_lose"] = int(state.get("stream_lose", 0)) + 1
    state["stream_delta"] = int(state.get("stream_delta", 0)) - MMR_STEP

    await redis_set_json(state_key, state)
    return "LOSE added"


@app.get("/reset_stream", response_class=PlainTextResponse)
async def reset_stream(token: str = Query("")):
    """
    Сбрасывает только счетчики стрима (Win/Lose/Total),
    MMR НЕ трогает.
    """
    require_admin(token)
    if not (UPSTASH_URL and UPSTASH_TOKEN and ACCOUNT_ID):
        return "not configured"

    state_key = f"mmr:{ACCOUNT_ID}"
    state = await redis_get_json(state_key) or {}
    state = ensure_state_fields(state)

    state["stream_win"] = 0
    state["stream_lose"] = 0
    state["stream_delta"] = 0
    # stream_start_time не трогаем (чтобы не поломать текущий стрим),
    # если хочешь обнулить полностью — ниже:
    # state["stream_start_time"] = 0

    await redis_set_json(state_key, state)
    return "stream counters reset"


@app.get("/force_stream_start", response_class=PlainTextResponse)
async def force_stream_start(token: str = Query(""), minutes: int = Query(60)):
    """
    Для экстренных случаев: вручную поставить старт стрима назад на N минут,
    чтобы подтянуть матчи текущего стрима.
    """
    require_admin(token)
    if not (UPSTASH_URL and UPSTASH_TOKEN and ACCOUNT_ID):
        return "not configured"

    minutes = max(1, min(int(minutes), 24 * 60))
    st = int(time.time()) - minutes * 60

    state_key = f"mmr:{ACCOUNT_ID}"
    state = await redis_get_json(state_key) or {}
    state = ensure_state_fields(state)

    state["stream_active"] = True
    state["stream_start_time"] = st
    state["stream_win"] = 0
    state["stream_lose"] = 0
    state["stream_delta"] = 0

    # baseline чтоб не пересчитывать древние
    state["last_start_time"] = max(int(state.get("last_start_time", 0)), st - 1)

    await redis_set_json(state_key, state)
    return f"stream_start_time forced to {st} (now-{minutes}min)"
