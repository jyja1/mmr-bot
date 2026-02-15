import os
import json
import time
import hmac
import hashlib
from datetime import timedelta, timezone
from typing import Optional, List, Set

import httpx
from fastapi import FastAPI, Request, Query
from fastapi.responses import PlainTextResponse

# ========= ENV =========
ACCOUNT_ID = os.environ.get("DOTA_ACCOUNT_ID")  # 32-bit steam account id
STEAM_API_KEY = os.environ.get("STEAM_API_KEY")

START_MMR = int(os.environ.get("START_MMR", "13772"))
MMR_STEP = int(os.environ.get("MMR_STEP", "25"))
TZ_OFFSET_HOURS = int(os.environ.get("TZ_OFFSET_HOURS", "3"))

UPSTASH_URL = os.environ.get("UPSTASH_REDIS_REST_URL")
UPSTASH_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN")

ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "")

TWITCH_CLIENT_ID = os.environ.get("TWITCH_CLIENT_ID")
TWITCH_CLIENT_SECRET = os.environ.get("TWITCH_CLIENT_SECRET")
TWITCH_BROADCASTER_LOGIN = os.environ.get("TWITCH_BROADCASTER_LOGIN", "debustie")
EVENTSUB_SECRET = os.environ.get("EVENTSUB_SECRET")

# небольшой “запас назад” при старте стрима, чтобы не пропустить матч,
# если ты начал игру чуть раньше уведомления stream.online
STREAM_LOOKBACK_MIN = int(os.environ.get("STREAM_LOOKBACK_MIN", "120"))

CACHE_TTL = 10  # сек кеша для /mmr

STEAM_MATCH_HISTORY = "https://api.steampowered.com/IDOTA2Match_570/GetMatchHistory/V001/"
STEAM_MATCH_DETAILS = "https://api.steampowered.com/IDOTA2Match_570/GetMatchDetails/V001/"

TWITCH_TOKEN_URL = "https://id.twitch.tv/oauth2/token"
TWITCH_HELIX = "https://api.twitch.tv/helix"

app = FastAPI()

_cache_text: Optional[str] = None
_cache_ts: float = 0.0


# ========= HELPERS =========
def tz_msk():
    return timezone(timedelta(hours=TZ_OFFSET_HOURS))


def fmt_signed(n: int) -> str:
    return f"+{n}" if n >= 0 else str(n)


def clear_cache():
    global _cache_text, _cache_ts
    _cache_text = None
    _cache_ts = 0.0


def require_admin(token: Optional[str]) -> Optional[str]:
    if not ADMIN_TOKEN:
        return "ADMIN_TOKEN не установлен"
    if not token or token != ADMIN_TOKEN:
        return "Forbidden"
    return None


async def redis_get_json(key: str) -> Optional[dict]:
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


async def redis_set_json(key: str, obj: dict) -> None:
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(
            f"{UPSTASH_URL}/set/{key}",
            headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
            data=json.dumps(obj, ensure_ascii=False),
        )
        r.raise_for_status()


def default_state(now_ts: int) -> dict:
    return {
        "start_mmr": START_MMR,
        "mmr": START_MMR,

        "last_start_time": now_ts,   # не считаем “старые” матчи до запуска
        "processed_ids": [],

        "stream_active": False,
        "stream_start_time": 0,
        "stream_win": 0,
        "stream_lose": 0,
        "stream_delta": 0,
    }


def ensure_state_fields(state: dict) -> dict:
    """
    Миграция состояния: если раньше state создавался без полей stream_*,
    мы добавляем их, не трогая текущий mmr.
    """
    state.setdefault("start_mmr", START_MMR)
    state.setdefault("mmr", START_MMR)

    state.setdefault("last_start_time", int(time.time()))
    state.setdefault("processed_ids", [])

    state.setdefault("stream_active", False)
    state.setdefault("stream_start_time", 0)
    state.setdefault("stream_win", 0)
    state.setdefault("stream_lose", 0)
    state.setdefault("stream_delta", 0)
    return state


def is_win_for_player(radiant_win: bool, player_slot: int) -> bool:
    is_radiant = int(player_slot) < 128
    return bool(radiant_win) if is_radiant else (not bool(radiant_win))


async def steam_get_ranked_matches(limit: int = 30) -> List[dict]:
    params = {
        "key": STEAM_API_KEY,
        "account_id": int(ACCOUNT_ID),
        "matches_requested": limit,
    }
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(STEAM_MATCH_HISTORY, params=params)
        r.raise_for_status()
        data = r.json()

    result = data.get("result", {}) or {}
    matches = result.get("matches", []) or []

    # ranked only: lobby_type == 7
    ranked = [m for m in matches if int(m.get("lobby_type", -1)) == 7]
    return ranked


async def steam_get_match_details(match_id: int) -> Optional[dict]:
    params = {"key": STEAM_API_KEY, "match_id": int(match_id)}
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(STEAM_MATCH_DETAILS, params=params)
        r.raise_for_status()
        data = r.json()
    return data.get("result") or None


def find_player_slot(details: dict, account_id: int) -> Optional[int]:
    for p in details.get("players", []) or []:
        if int(p.get("account_id", -1)) == int(account_id):
            return int(p.get("player_slot"))
    return None


# ========= HEALTH (для UptimeRobot на бесплатном: HEAD-only) =========
@app.api_route("/health", methods=["GET", "HEAD"], response_class=PlainTextResponse)
async def health():
    return "ok"


# ========= MMR =========
@app.get("/mmr", response_class=PlainTextResponse)
async def mmr():
    global _cache_text, _cache_ts

    now = time.time()
    if _cache_text and (now - _cache_ts) < CACHE_TTL:
        return _cache_text

    if not ACCOUNT_ID:
        return "DOTA_ACCOUNT_ID не установлен"
    if not STEAM_API_KEY:
        return "STEAM_API_KEY не установлен"
    if not UPSTASH_URL or not UPSTASH_TOKEN:
        return "Redis не настроен"

    state_key = f"mmr:{ACCOUNT_ID}"
    state = await redis_get_json(state_key)

    if not state:
        state = default_state(int(time.time()))
        await redis_set_json(state_key, state)

    state = ensure_state_fields(state)

    processed: Set[int] = set(int(x) for x in (state.get("processed_ids") or []))
    last_time = int(state.get("last_start_time", 0))

    matches = await steam_get_ranked_matches(limit=30)

    new_matches = []
    for m in matches:
        mid = m.get("match_id")
        st = m.get("start_time")
        if not mid or not st:
            continue
        mid = int(mid)
        st = int(st)
        if st > last_time and mid not in processed:
            new_matches.append(m)

    new_matches.sort(key=lambda x: int(x.get("start_time", 0)))

    for m in new_matches:
        mid = int(m["match_id"])
        st = int(m["start_time"])

        details = await steam_get_match_details(mid)
        if not details:
            continue

        radiant_win = details.get("radiant_win")
        if radiant_win is None:
            continue

        player_slot = find_player_slot(details, int(ACCOUNT_ID))
        if player_slot is None:
            continue

        won = is_win_for_player(bool(radiant_win), int(player_slot))
        delta = MMR_STEP if won else -MMR_STEP

        # общий mmr всегда копится
        state["mmr"] = int(state.get("mmr", START_MMR)) + delta

        # “Today” = только за текущий стрим
        if state.get("stream_active") and st >= int(state.get("stream_start_time", 0)):
            if won:
                state["stream_win"] = int(state.get("stream_win", 0)) + 1
            else:
                state["stream_lose"] = int(state.get("stream_lose", 0)) + 1
            state["stream_delta"] = int(state.get("stream_delta", 0)) + delta

        processed.add(mid)
        state["last_start_time"] = max(int(state.get("last_start_time", 0)), st)

    state["processed_ids"] = list(processed)[-300:]
    await redis_set_json(state_key, state)

    cur = int(state.get("mmr", START_MMR))
    tw = int(state.get("stream_win", 0))
    tl = int(state.get("stream_lose", 0))
    td = int(state.get("stream_delta", 0))

    text = f"MMR: {cur} • Today -> Win: {tw} Lose: {tl} • Total: {fmt_signed(td)}"
    _cache_text, _cache_ts = text, now
    return text


# ========= ADMIN DEBUG / CONTROL =========
@app.get("/streamstatus", response_class=PlainTextResponse)
async def streamstatus(token: str = Query(default="")):
    err = require_admin(token)
    if err:
        return err

    state_key = f"mmr:{ACCOUNT_ID}"
    state = await redis_get_json(state_key)
    if not state:
        return "State not initialized"

    state = ensure_state_fields(state)
    await redis_set_json(state_key, state)

    return (
        f"stream_active={state.get('stream_active')}\n"
        f"stream_start_time={state.get('stream_start_time')}\n"
        f"stream_win={state.get('stream_win')}\n"
        f"stream_lose={state.get('stream_lose')}\n"
        f"stream_delta={state.get('stream_delta')}\n"
        f"mmr={state.get('mmr')}\n"
    )


@app.get("/testwin", response_class=PlainTextResponse)
async def testwin(token: str = Query(default="")):
    err = require_admin(token)
    if err:
        return err

    state_key = f"mmr:{ACCOUNT_ID}"
    state = await redis_get_json(state_key)
    if not state:
        return "State not initialized"

    state = ensure_state_fields(state)

    state["mmr"] = int(state.get("mmr", START_MMR)) + MMR_STEP
    state["stream_win"] = int(state.get("stream_win", 0)) + 1
    state["stream_delta"] = int(state.get("stream_delta", 0)) + MMR_STEP

    await redis_set_json(state_key, state)
    clear_cache()
    return "WIN added"


@app.get("/testlose", response_class=PlainTextResponse)
async def testlose(token: str = Query(default="")):
    err = require_admin(token)
    if err:
        return err

    state_key = f"mmr:{ACCOUNT_ID}"
    state = await redis_get_json(state_key)
    if not state:
        return "State not initialized"

    state = ensure_state_fields(state)

    state["mmr"] = int(state.get("mmr", START_MMR)) - MMR_STEP
    state["stream_lose"] = int(state.get("stream_lose", 0)) + 1
    state["stream_delta"] = int(state.get("stream_delta", 0)) - MMR_STEP

    await redis_set_json(state_key, state)
    clear_cache()
    return "LOSE added"


@app.get("/reset_stream", response_class=PlainTextResponse)
async def reset_stream(token: str = Query(default="")):
    """
    Обнуляет Today/Win/Lose/Total (стримовые), НЕ трогает общий mmr.
    """
    err = require_admin(token)
    if err:
        return err

    state_key = f"mmr:{ACCOUNT_ID}"
    state = await redis_get_json(state_key) or default_state(int(time.time()))
    state = ensure_state_fields(state)

    state["stream_win"] = 0
    state["stream_lose"] = 0
    state["stream_delta"] = 0

    await redis_set_json(state_key, state)
    clear_cache()
    return "Stream counters reset"

@app.get("/force_stream_start", response_class=PlainTextResponse)
async def force_stream_start(token: str = Query(default=""), minutes: int = Query(default=360)):
    err = require_admin(token)
    if err:
        return err

    state_key = f"mmr:{ACCOUNT_ID}"
    state = await redis_get_json(state_key) or default_state(int(time.time()))
    state = ensure_state_fields(state)

    now_ts = int(time.time())
    baseline = now_ts - int(minutes) * 60  # считаем “стрим” как будто начался minutes минут назад

    state["stream_active"] = True
    state["stream_start_time"] = baseline
    state["stream_win"] = 0
    state["stream_lose"] = 0
    state["stream_delta"] = 0

    await redis_set_json(state_key, state)
    clear_cache()
    return f"OK stream forced. start_time={baseline}"

# ========= EVENTSUB (Twitch) =========
def verify_eventsub_signature(req: Request, body: bytes) -> bool:
    if not EVENTSUB_SECRET:
        return False

    msg_id = req.headers.get("Twitch-Eventsub-Message-Id", "")
    msg_ts = req.headers.get("Twitch-Eventsub-Message-Timestamp", "")
    their_sig = req.headers.get("Twitch-Eventsub-Message-Signature", "")

    if not msg_id or not msg_ts or not their_sig:
        return False

    message = (msg_id + msg_ts).encode("utf-8") + body
    digest = hmac.new(EVENTSUB_SECRET.encode("utf-8"), message, hashlib.sha256).hexdigest()
    our_sig = "sha256=" + digest
    return hmac.compare_digest(our_sig, their_sig)


@app.post("/eventsub")
async def eventsub(request: Request):
    body = await request.body()

    if not verify_eventsub_signature(request, body):
        return PlainTextResponse("invalid signature", status_code=403)

    payload = json.loads(body.decode("utf-8"))
    msg_type = request.headers.get("Twitch-Eventsub-Message-Type", "")

    # verification challenge
    if msg_type == "webhook_callback_verification":
        challenge = payload.get("challenge", "")
        return PlainTextResponse(challenge)

    # notifications
    if msg_type == "notification":
        sub = payload.get("subscription") or {}
        sub_type = sub.get("type")

        state_key = f"mmr:{ACCOUNT_ID}"
        state = await redis_get_json(state_key) or default_state(int(time.time()))
        state = ensure_state_fields(state)

        now_ts = int(time.time())

        if sub_type == "stream.online":
            baseline = now_ts - STREAM_LOOKBACK_MIN * 60

            state["stream_active"] = True
            state["stream_start_time"] = baseline
            state["stream_win"] = 0
            state["stream_lose"] = 0
            state["stream_delta"] = 0

            # чтобы не прихватить старое
            state["last_start_time"] = max(int(state.get("last_start_time", 0)), baseline)
            state["processed_ids"] = []

            await redis_set_json(state_key, state)
            clear_cache()

        elif sub_type == "stream.offline":
            state["stream_active"] = False
            await redis_set_json(state_key, state)
            clear_cache()

        return PlainTextResponse("ok")

    # revocation / anything else
    return PlainTextResponse("ok")


async def twitch_app_token() -> str:
    params = {
        "client_id": TWITCH_CLIENT_ID,
        "client_secret": TWITCH_CLIENT_SECRET,
        "grant_type": "client_credentials",
    }
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(TWITCH_TOKEN_URL, params=params)
        r.raise_for_status()
        data = r.json()
    return data["access_token"]


async def twitch_get_user_id(login: str, token: str) -> str:
    headers = {"Authorization": f"Bearer {token}", "Client-Id": TWITCH_CLIENT_ID}
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(f"{TWITCH_HELIX}/users", headers=headers, params={"login": login})
        r.raise_for_status()
        data = r.json()
    arr = data.get("data") or []
    if not arr:
        raise RuntimeError("Twitch user not found")
    return arr[0]["id"]


async def twitch_create_subscription(sub_type: str, broadcaster_id: str, callback_url: str, token: str) -> dict:
    headers = {
        "Authorization": f"Bearer {token}",
        "Client-Id": TWITCH_CLIENT_ID,
        "Content-Type": "application/json",
    }
    payload = {
        "type": sub_type,
        "version": "1",
        "condition": {"broadcaster_user_id": broadcaster_id},
        "transport": {"method": "webhook", "callback": callback_url, "secret": EVENTSUB_SECRET},
    }

    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(f"{TWITCH_HELIX}/eventsub/subscriptions", headers=headers, json=payload)

        # если уже существует — не падаем
        if r.status_code == 409:
            return {"status": "exists"}

        if r.status_code >= 400:
            return {"status": "error", "code": r.status_code, "body": r.text}

        return r.json()


@app.get("/eventsub_setup", response_class=PlainTextResponse)
async def eventsub_setup(request: Request, token: str = Query(default="")):
    """
    Один раз открыть:
    https://mmr-bot.onrender.com/eventsub_setup?token=ADMIN_TOKEN
    """
    err = require_admin(token)
    if err:
        return err

    if not (TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET and EVENTSUB_SECRET and TWITCH_BROADCASTER_LOGIN):
        return "Twitch env vars missing"

    app_token = await twitch_app_token()
    broadcaster_id = await twitch_get_user_id(TWITCH_BROADCASTER_LOGIN, app_token)

    base = str(request.base_url).rstrip("/")
    callback_url = f"{base}/eventsub"

    online = await twitch_create_subscription("stream.online", broadcaster_id, callback_url, app_token)
    offline = await twitch_create_subscription("stream.offline", broadcaster_id, callback_url, app_token)

    return PlainTextResponse(
        "OK\n"
        f"callback: {callback_url}\n"
        f"online: {online.get('status', 'created')}\n"
        f"offline: {offline.get('status', 'created')}\n"
    )
