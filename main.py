import os
import json
import time
import hmac
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import httpx
from fastapi import FastAPI, Request, Query, HTTPException
from fastapi.responses import PlainTextResponse

# =========================
# ENV
# =========================
ACCOUNT_ID = os.environ.get("DOTA_ACCOUNT_ID")  # Dota2 account_id (32-bit)
START_MMR = int(os.environ.get("START_MMR", "13772"))
MMR_STEP = int(os.environ.get("MMR_STEP", "25"))
TZ_OFFSET_HOURS = int(os.environ.get("TZ_OFFSET_HOURS", "3"))

UPSTASH_URL = os.environ.get("UPSTASH_REDIS_REST_URL")
UPSTASH_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN")

ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "")

# Twitch EventSub (stream.online/offline) + Helix fallback (auto-bootstrap)
TWITCH_CLIENT_ID = os.environ.get("TWITCH_CLIENT_ID", "")
TWITCH_CLIENT_SECRET = os.environ.get("TWITCH_CLIENT_SECRET", "")
TWITCH_BROADCASTER_LOGIN = os.environ.get("TWITCH_BROADCASTER_LOGIN", "")  # e.g. debustie
EVENTSUB_SECRET = os.environ.get("EVENTSUB_SECRET", "")
PUBLIC_BASE_URL = os.environ.get("PUBLIC_BASE_URL", "").rstrip("/")  # e.g. https://mmr-bot.onrender.com

OPENDOTA = "https://api.opendota.com/api"
TWITCH_TOKEN_URL = "https://id.twitch.tv/oauth2/token"
TWITCH_API_BASE = "https://api.twitch.tv/helix"

CACHE_TTL = 5  # seconds
HTTP_TIMEOUT = 15

app = FastAPI()
_cache_text: Optional[str] = None
_cache_ts: float = 0.0


# =========================
# Helpers
# =========================
def tz_msk():
    return timezone(timedelta(hours=TZ_OFFSET_HOURS))


def fmt_signed(n: int) -> str:
    return f"+{n}" if n >= 0 else str(n)


def now_unix() -> int:
    return int(time.time())


def clear_cache():
    global _cache_text, _cache_ts
    _cache_text = None
    _cache_ts = 0.0


def is_win_for_player(radiant_win: bool, player_slot: int) -> bool:
    # player_slot < 128 => Radiant, else Dire
    is_radiant = int(player_slot) < 128
    return bool(radiant_win) if is_radiant else (not bool(radiant_win))


def require_admin(token: str):
    if not ADMIN_TOKEN:
        raise HTTPException(status_code=500, detail="ADMIN_TOKEN not set")
    if token != ADMIN_TOKEN:
        raise HTTPException(status_code=403, detail="Forbidden")


def redis_key_state() -> str:
    # One service = one tracked account
    return f"mmrbot:state:{ACCOUNT_ID}"


async def redis_get_json(key: str) -> Optional[Dict[str, Any]]:
    if not UPSTASH_URL or not UPSTASH_TOKEN:
        raise HTTPException(status_code=500, detail="Redis not configured")
    k = quote(key, safe="")
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        r = await client.get(
            f"{UPSTASH_URL}/get/{k}",
            headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
        )
        r.raise_for_status()
        data = r.json()
        val = data.get("result")
        if val is None:
            return None
        return json.loads(val)


async def redis_set_json(key: str, obj: Dict[str, Any]):
    if not UPSTASH_URL or not UPSTASH_TOKEN:
        raise HTTPException(status_code=500, detail="Redis not configured")
    k = quote(key, safe="")
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        r = await client.post(
            f"{UPSTASH_URL}/set/{k}",
            headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
            data=json.dumps(obj, ensure_ascii=False),
        )
        r.raise_for_status()


def default_state() -> Dict[str, Any]:
    # stream_* = окно текущего/последнего стрима. Сбрасывается на stream.online или ручной форс.
    return {
        "mmr": START_MMR,

        "stream_active": False,
        "stream_start_time": 0,   # unix
        "stream_end_time": 0,     # unix (0 => still active / not ended)
        "stream_win": 0,
        "stream_lose": 0,
        "stream_delta": 0,

        # only for current/last stream: prevent recounts
        "processed_ids_stream": [],  # match_ids counted for stream window

        # twitch:
        "twitch_broadcaster_id": "",

        # debug:
        "last_errors": [],  # list[str]
    }


def add_error(state: Dict[str, Any], msg: str):
    arr = state.get("last_errors", [])
    if not isinstance(arr, list):
        arr = []
    ts = now_unix()
    arr.append(f"{ts}: {msg}")
    state["last_errors"] = arr[-30:]


def get_public_base_url(request: Request) -> str:
    if PUBLIC_BASE_URL:
        return PUBLIC_BASE_URL
    proto = request.headers.get("x-forwarded-proto") or request.url.scheme
    host = request.headers.get("x-forwarded-host") or request.headers.get("host") or request.url.netloc
    return f"{proto}://{host}".rstrip("/")


async def fetch_ranked_matches(account_id: str, limit: int = 40) -> List[Dict[str, Any]]:
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        r = await client.get(
            f"{OPENDOTA}/players/{account_id}/matches",
            params={"lobby_type": 7, "limit": limit},
        )
        r.raise_for_status()
        data = r.json() or []
        if not isinstance(data, list):
            return []
        return data


# =========================
# Core logic: count matches in stream window
# =========================
def in_stream_window(st: int, start: int, end: int) -> bool:
    if start <= 0:
        return False
    if end and end > 0:
        return start <= st <= end
    return st >= start


async def update_from_opendota(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Counts ranked matches that fall into current/last stream window:
      stream_start_time <= start_time <= stream_end_time (if end_time>0)
      stream_start_time <= start_time              (if end_time==0)

    - MMR always accumulates
    - stream_* counts accumulate for this window
    """
    start = int(state.get("stream_start_time", 0) or 0)
    end = int(state.get("stream_end_time", 0) or 0)

    if start <= 0:
        return state

    matches = await fetch_ranked_matches(ACCOUNT_ID, limit=40)
    processed = set(state.get("processed_ids_stream", []) or [])

    candidates = []
    for m in matches:
        mid = m.get("match_id")
        st = m.get("start_time")
        if not mid or not st:
            continue
        try:
            mid_i = int(mid)
            st_i = int(st)
        except Exception:
            continue

        if mid_i in processed:
            continue
        if not in_stream_window(st_i, start, end):
            continue
        if m.get("radiant_win") is None or m.get("player_slot") is None:
            continue

        candidates.append(m)

    candidates.sort(key=lambda x: int(x.get("start_time", 0)))

    for m in candidates:
        mid = int(m["match_id"])
        radiant_win = bool(m["radiant_win"])
        player_slot = int(m["player_slot"])

        won = is_win_for_player(radiant_win, player_slot)
        delta = MMR_STEP if won else -MMR_STEP

        state["mmr"] = int(state.get("mmr", START_MMR)) + delta
        if won:
            state["stream_win"] = int(state.get("stream_win", 0)) + 1
        else:
            state["stream_lose"] = int(state.get("stream_lose", 0)) + 1
        state["stream_delta"] = int(state.get("stream_delta", 0)) + delta

        processed.add(mid)

    state["processed_ids_stream"] = list(processed)[-400:]
    return state


# =========================
# Twitch Helix fallback (auto-bootstrap)
# =========================
async def twitch_app_token() -> str:
    if not TWITCH_CLIENT_ID or not TWITCH_CLIENT_SECRET:
        raise HTTPException(status_code=500, detail="TWITCH_CLIENT_ID/SECRET not set")

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        r = await client.post(
            TWITCH_TOKEN_URL,
            params={
                "client_id": TWITCH_CLIENT_ID,
                "client_secret": TWITCH_CLIENT_SECRET,
                "grant_type": "client_credentials",
            },
        )
        r.raise_for_status()
        data = r.json()
        token = data.get("access_token")
        if not token:
            raise HTTPException(status_code=500, detail="Failed to get twitch app token")
        return token


async def twitch_get_user_id(login: str, access_token: str) -> str:
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        r = await client.get(
            f"{TWITCH_API_BASE}/users",
            params={"login": login},
            headers={
                "Client-Id": TWITCH_CLIENT_ID,
                "Authorization": f"Bearer {access_token}",
            },
        )
        r.raise_for_status()
        data = r.json()
        arr = data.get("data", [])
        if not arr:
            raise HTTPException(status_code=500, detail=f"Twitch user not found: {login}")
        return arr[0]["id"]


async def twitch_is_live_and_started_at(broadcaster_id: str, access_token: str) -> (bool, int):
    """
    Returns (is_live, started_at_unix)
    """
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        r = await client.get(
            f"{TWITCH_API_BASE}/streams",
            params={"user_id": broadcaster_id},
            headers={
                "Client-Id": TWITCH_CLIENT_ID,
                "Authorization": f"Bearer {access_token}",
            },
        )
        r.raise_for_status()
        data = r.json()
        arr = data.get("data", []) or []
        if not arr:
            return False, 0
        started_at = arr[0].get("started_at")  # RFC3339
        if started_at:
            try:
                dt = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
                return True, int(dt.timestamp())
            except Exception:
                return True, 0
        return True, 0


async def maybe_bootstrap_stream_from_helix(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Если EventSub не успел прислать stream.online (стрим уже шёл),
    то на /mmr мы один раз сами проверяем Helix:
      - если сейчас LIVE и stream_active=False -> включаем стрим, ставим start_time=started_at, обнуляем счётчики
      - если сейчас OFFLINE и stream_active=True -> выключаем (end_time=now)
    """
    if not TWITCH_CLIENT_ID or not TWITCH_CLIENT_SECRET or not TWITCH_BROADCASTER_LOGIN:
        return state

    try:
        access_token = await twitch_app_token()

        broadcaster_id = state.get("twitch_broadcaster_id", "") or ""
        if not broadcaster_id:
            broadcaster_id = await twitch_get_user_id(TWITCH_BROADCASTER_LOGIN, access_token)
            state["twitch_broadcaster_id"] = broadcaster_id

        is_live, started_at_unix = await twitch_is_live_and_started_at(broadcaster_id, access_token)

        if is_live and not bool(state.get("stream_active", False)):
            # авто-старт окна стрима
            st = started_at_unix or now_unix()
            state["stream_active"] = True
            state["stream_start_time"] = st
            state["stream_end_time"] = 0
            state["stream_win"] = 0
            state["stream_lose"] = 0
            state["stream_delta"] = 0
            state["processed_ids_stream"] = []
            clear_cache()

        if (not is_live) and bool(state.get("stream_active", False)):
            # авто-стоп окна стрима
            if int(state.get("stream_start_time", 0) or 0) > 0:
                state["stream_end_time"] = now_unix()
            state["stream_active"] = False
            clear_cache()

    except Exception as e:
        add_error(state, f"helix_bootstrap_error: {type(e).__name__}: {e}")

    return state


# =========================
# Endpoints
# =========================
@app.get("/health", response_class=PlainTextResponse)
async def health():
    return "ok"


@app.get("/mmr", response_class=PlainTextResponse)
async def mmr():
    global _cache_text, _cache_ts

    now = time.time()
    if _cache_text and (now - _cache_ts) < CACHE_TTL:
        return _cache_text

    if not ACCOUNT_ID:
        return "DOTA_ACCOUNT_ID не установлен"
    if not UPSTASH_URL or not UPSTASH_TOKEN:
        return "Redis не настроен"

    key = redis_key_state()
    state = await redis_get_json(key)
    if not state:
        state = default_state()
        await redis_set_json(key, state)

    # Auto bootstrap from Twitch Helix (если стрим уже идёт)
    state = await maybe_bootstrap_stream_from_helix(state)

    # update from OpenDota (counts only stream window)
    try:
        state = await update_from_opendota(state)
        await redis_set_json(key, state)
    except Exception as e:
        add_error(state, f"opendota_update_error: {type(e).__name__}: {e}")
        await redis_set_json(key, state)

    cur = int(state.get("mmr", START_MMR))
    w = int(state.get("stream_win", 0))
    l = int(state.get("stream_lose", 0))
    d = int(state.get("stream_delta", 0))

    text = f"MMR: {cur} • Today -> Win: {w} Lose: {l} • Total: {fmt_signed(d)}"
    _cache_text, _cache_ts = text, now
    return text


@app.get("/streamstatus", response_class=PlainTextResponse)
async def streamstatus(token: str = Query("")):
    require_admin(token)

    key = redis_key_state()
    state = await redis_get_json(key) or default_state()

    lines = []
    lines.append(f"account_id={ACCOUNT_ID}")
    lines.append(f"mmr={state.get('mmr', START_MMR)}")
    lines.append(f"stream_active={state.get('stream_active', False)}")
    lines.append(f"stream_start_time={state.get('stream_start_time', 0)}")
    lines.append(f"stream_end_time={state.get('stream_end_time', 0)}")
    lines.append(f"stream_win={state.get('stream_win', 0)}")
    lines.append(f"stream_lose={state.get('stream_lose', 0)}")
    lines.append(f"stream_delta={state.get('stream_delta', 0)}")
    lines.append(f"processed_ids_stream_count={len(state.get('processed_ids_stream', []) or [])}")
    lines.append(f"twitch_broadcaster_id={state.get('twitch_broadcaster_id', '')}")

    errs = state.get("last_errors", []) or []
    lines.append("")
    lines.append("last_errors:")
    for e in errs[-20:]:
        lines.append(f"- {e}")

    return "\n".join(lines)


@app.get("/force_stream_on", response_class=PlainTextResponse)
async def force_stream_on(
    token: str = Query(""),
    hours_ago: int = Query(0),
    ts: int = Query(0),
):
    """
    Ручной форс включения стрима + сброс счётчиков.
    - ts=UNIX (приоритет)
    - hours_ago=N -> start = now - N*3600
    - иначе start = now
    """
    require_admin(token)

    key = redis_key_state()
    state = await redis_get_json(key) or default_state()

    if ts and ts > 0:
        start_ts = int(ts)
    elif hours_ago and hours_ago > 0:
        start_ts = now_unix() - int(hours_ago) * 3600
    else:
        start_ts = now_unix()

    state["stream_active"] = True
    state["stream_start_time"] = start_ts
    state["stream_end_time"] = 0
    state["stream_win"] = 0
    state["stream_lose"] = 0
    state["stream_delta"] = 0
    state["processed_ids_stream"] = []
    clear_cache()

    await redis_set_json(key, state)
    return f"OK force_stream_on start_time={start_ts}"


@app.get("/force_stream_off", response_class=PlainTextResponse)
async def force_stream_off(token: str = Query("")):
    """
    Ручной форс выключения стрима. Счётчики не сбрасывает, просто закрывает окно.
    """
    require_admin(token)

    key = redis_key_state()
    state = await redis_get_json(key) or default_state()

    state["stream_active"] = False
    if int(state.get("stream_start_time", 0) or 0) > 0:
        state["stream_end_time"] = now_unix()
    clear_cache()

    await redis_set_json(key, state)
    return f"OK force_stream_off end_time={state.get('stream_end_time', 0)}"


@app.get("/reset", response_class=PlainTextResponse)
async def reset(
    token: str = Query(""),
    mode: str = Query("stream"),  # stream | all
):
    """
    mode=stream: clears stream window + stream stats (MMR preserved)
    mode=all: resets everything including MMR back to START_MMR
    """
    require_admin(token)

    key = redis_key_state()
    state = await redis_get_json(key) or default_state()

    if mode == "all":
        state = default_state()
    else:
        mmr_val = int(state.get("mmr", START_MMR))
        state["mmr"] = mmr_val

        state["stream_active"] = False
        state["stream_start_time"] = 0
        state["stream_end_time"] = 0
        state["stream_win"] = 0
        state["stream_lose"] = 0
        state["stream_delta"] = 0
        state["processed_ids_stream"] = []

    clear_cache()
    await redis_set_json(key, state)
    return f"OK reset mode={mode} mmr={state.get('mmr')}"


# =========================
# Twitch EventSub
# =========================
def verify_eventsub_signature(secret: str, headers: Dict[str, str], body: bytes) -> bool:
    msg_id = headers.get("Twitch-Eventsub-Message-Id", "")
    ts = headers.get("Twitch-Eventsub-Message-Timestamp", "")
    sig = headers.get("Twitch-Eventsub-Message-Signature", "")
    if not msg_id or not ts or not sig:
        return False
    if not sig.startswith("sha256="):
        return False

    mac = hmac.new(secret.encode("utf-8"), digestmod=hashlib.sha256)
    mac.update((msg_id + ts).encode("utf-8"))
    mac.update(body)
    expected = "sha256=" + mac.hexdigest()
    return hmac.compare_digest(expected, sig)


async def twitch_list_subs(access_token: str) -> List[Dict[str, Any]]:
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        r = await client.get(
            f"{TWITCH_API_BASE}/eventsub/subscriptions",
            headers={
                "Client-Id": TWITCH_CLIENT_ID,
                "Authorization": f"Bearer {access_token}",
            },
        )
        r.raise_for_status()
        data = r.json()
        return data.get("data", []) or []


async def twitch_create_sub(access_token: str, sub_type: str, broadcaster_user_id: str, callback: str):
    body = {
        "type": sub_type,
        "version": "1",
        "condition": {"broadcaster_user_id": broadcaster_user_id},
        "transport": {
            "method": "webhook",
            "callback": callback,
            "secret": EVENTSUB_SECRET,
        },
    }
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        r = await client.post(
            f"{TWITCH_API_BASE}/eventsub/subscriptions",
            headers={
                "Client-Id": TWITCH_CLIENT_ID,
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            },
            json=body,
        )
        r.raise_for_status()
        return r.json()


@app.post("/eventsub", response_class=PlainTextResponse)
async def eventsub(request: Request):
    if not EVENTSUB_SECRET:
        raise HTTPException(status_code=500, detail="EVENTSUB_SECRET not set")

    body = await request.body()

    if not verify_eventsub_signature(EVENTSUB_SECRET, dict(request.headers), body):
        raise HTTPException(status_code=403, detail="Bad signature")

    data = json.loads(body.decode("utf-8") or "{}")

    msg_type = request.headers.get("Twitch-Eventsub-Message-Type", "")
    if msg_type == "webhook_callback_verification":
        challenge = data.get("challenge", "")
        return challenge

    if msg_type != "notification":
        return "ok"

    sub = data.get("subscription", {}) or {}
    event = data.get("event", {}) or {}
    typ = sub.get("type", "")

    key = redis_key_state()
    state = await redis_get_json(key) or default_state()

    try:
        if typ == "stream.online":
            started_at = event.get("started_at")  # RFC3339
            st = 0
            if started_at:
                try:
                    dt = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
                    st = int(dt.timestamp())
                except Exception:
                    st = now_unix()
            else:
                st = now_unix()

            state["stream_active"] = True
            state["stream_start_time"] = st
            state["stream_end_time"] = 0

            # reset stream counters
            state["stream_win"] = 0
            state["stream_lose"] = 0
            state["stream_delta"] = 0
            state["processed_ids_stream"] = []
            clear_cache()

        elif typ == "stream.offline":
            state["stream_active"] = False
            if int(state.get("stream_start_time", 0) or 0) > 0:
                state["stream_end_time"] = now_unix()
            clear_cache()

        await redis_set_json(key, state)
    except Exception as e:
        add_error(state, f"eventsub_handle_error: {type(e).__name__}: {e}")
        await redis_set_json(key, state)

    return "ok"


@app.get("/eventsub_setup", response_class=PlainTextResponse)
async def eventsub_setup(request: Request, token: str = Query("")):
    require_admin(token)

    if not TWITCH_BROADCASTER_LOGIN:
        return "TWITCH_BROADCASTER_LOGIN not set"
    if not EVENTSUB_SECRET:
        return "EVENTSUB_SECRET not set"
    if not TWITCH_CLIENT_ID or not TWITCH_CLIENT_SECRET:
        return "TWITCH_CLIENT_ID/SECRET not set"

    base = get_public_base_url(request)
    callback = f"{base}/eventsub"

    access_token = await twitch_app_token()
    broadcaster_id = await twitch_get_user_id(TWITCH_BROADCASTER_LOGIN, access_token)

    key = redis_key_state()
    state = await redis_get_json(key) or default_state()
    state["twitch_broadcaster_id"] = broadcaster_id
    await redis_set_json(key, state)

    subs = await twitch_list_subs(access_token)
    have_online = any(
        s.get("type") == "stream.online" and s.get("condition", {}).get("broadcaster_user_id") == broadcaster_id
        for s in subs
    )
    have_offline = any(
        s.get("type") == "stream.offline" and s.get("condition", {}).get("broadcaster_user_id") == broadcaster_id
        for s in subs
    )

    out = []
    out.append("ok")
    out.append(f"callback: {callback}")

    if not have_online:
        await twitch_create_sub(access_token, "stream.online", broadcaster_id, callback)
        out.append("online: created")
    else:
        out.append("online: exists")

    if not have_offline:
        await twitch_create_sub(access_token, "stream.offline", broadcaster_id, callback)
        out.append("offline: created")
    else:
        out.append("offline: exists")

    return "\n".join(out)
