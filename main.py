import os
import json
import time
import hmac
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, List, Tuple

import httpx
from fastapi import FastAPI, Request, Query, HTTPException
from fastapi.responses import PlainTextResponse

# =========================
# ENV
# =========================
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "").strip()

DOTA_ACCOUNT_ID = os.environ.get("DOTA_ACCOUNT_ID", "").strip()
START_MMR = int(os.environ.get("START_MMR", "13772"))
MMR_STEP = int(os.environ.get("MMR_STEP", "25"))
TZ_OFFSET_HOURS = int(os.environ.get("TZ_OFFSET_HOURS", "3"))

UPSTASH_URL = os.environ.get("UPSTASH_REDIS_REST_URL", "").strip()
UPSTASH_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "").strip()

STEAM_API_KEY = os.environ.get("STEAM_API_KEY", "").strip()

TWITCH_CLIENT_ID = os.environ.get("TWITCH_CLIENT_ID", "").strip()
TWITCH_CLIENT_SECRET = os.environ.get("TWITCH_CLIENT_SECRET", "").strip()
TWITCH_BROADCASTER_LOGIN = os.environ.get("TWITCH_BROADCASTER_LOGIN", "").strip()
EVENTSUB_SECRET = os.environ.get("EVENTSUB_SECRET", "").strip()

# IMPORTANT: base URL должен совпадать с твоим доменом на Render
APP_BASE_URL = os.environ.get("APP_BASE_URL", "https://mmr-bot.onrender.com").strip()

# =========================
# CONST
# =========================
STEAM_MATCH_HISTORY = "https://api.steampowered.com/IDOTA2Match_570/GetMatchHistory/v1/"
STEAM_MATCH_DETAILS = "https://api.steampowered.com/IDOTA2Match_570/GetMatchDetails/v1/"

TWITCH_TOKEN_URL = "https://id.twitch.tv/oauth2/token"
TWITCH_HELIX = "https://api.twitch.tv/helix/eventsub/subscriptions"
TWITCH_USERS = "https://api.twitch.tv/helix/users"

CACHE_TTL = 5  # секунд (не ставь 15-60, иначе в чате будет казаться "не меняется")

app = FastAPI()

_cache_text = None
_cache_ts = 0

# для дебага ошибок (пишем в state)
MAX_ERRORS = 40


# =========================
# TIME / FORMAT
# =========================
def tz_msk():
    return timezone(timedelta(hours=TZ_OFFSET_HOURS))


def fmt_signed(n: int) -> str:
    return f"+{n}" if n >= 0 else str(n)


def now_unix() -> int:
    return int(time.time())


# =========================
# REDIS (Upstash REST)
# =========================
async def redis_get_json(key: str) -> Optional[dict]:
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(
            f"{UPSTASH_URL}/get/{key}",
            headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
        )
        r.raise_for_status()
        data = r.json()
        val = data.get("result")
        if val is None:
            return None
        try:
            return json.loads(val)
        except Exception:
            return None


async def redis_set_json(key: str, obj: dict):
    async with httpx.AsyncClient(timeout=10) as client:
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

        # stream session stats
        "stream_active": False,
        "stream_start_time": 0,      # unix
        "stream_win": 0,
        "stream_lose": 0,
        "stream_delta": 0,

        # processed match ids (for stream only)
        "processed_ids_stream": [],

        # debug
        "last_errors": [],
    }


def push_error(state: dict, msg: str):
    arr = state.get("last_errors") or []
    arr.append(f"{now_unix()}: {msg}")
    state["last_errors"] = arr[-MAX_ERRORS:]


def auth_ok(token: str) -> bool:
    return bool(ADMIN_TOKEN) and token == ADMIN_TOKEN


# =========================
# STEAM API (Ranked matches + details)
# =========================
async def steam_get_match_history(account_id: str, matches_requested: int = 15) -> dict:
    params = {
        "key": STEAM_API_KEY,
        "account_id": account_id,
        "matches_requested": matches_requested,
        "lobby_type": 7,  # ranked
    }
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(STEAM_MATCH_HISTORY, params=params)
        r.raise_for_status()
        return r.json()


async def steam_get_match_details(match_id: int) -> dict:
    params = {
        "key": STEAM_API_KEY,
        "match_id": str(match_id),
    }
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(STEAM_MATCH_DETAILS, params=params)
        # иногда Steam может отдавать 500/503 — делаем простейший retry
        if r.status_code >= 500:
            await asyncio_sleep(1.2)
            r = await client.get(STEAM_MATCH_DETAILS, params=params)
        r.raise_for_status()
        return r.json()


def is_win_for_player(radiant_win: bool, player_slot: int) -> bool:
    is_radiant = int(player_slot) < 128
    return bool(radiant_win) if is_radiant else (not bool(radiant_win))


async def asyncio_sleep(seconds: float):
    # без импорта asyncio сверху — чтобы файл был короче
    import asyncio
    await asyncio.sleep(seconds)


async def apply_new_stream_matches(state: dict) -> Tuple[int, int, int]:
    """
    Считает новые ranked матчи, которые начались >= stream_start_time.
    Возвращает (added, wins, loses)
    """
    if not state.get("stream_active"):
        return (0, 0, 0)

    stream_start = int(state.get("stream_start_time", 0))
    if stream_start <= 0:
        return (0, 0, 0)

    processed = set(state.get("processed_ids_stream", []) or [])

    history = await steam_get_match_history(DOTA_ACCOUNT_ID, matches_requested=30)
    result = history.get("result", {})
    matches = result.get("matches", []) or []

    # отбираем ranked матчи после старта стрима
    candidates = []
    for m in matches:
        mid = m.get("match_id")
        st = m.get("start_time")
        lobby_type = m.get("lobby_type")
        if not mid or not st:
            continue
        if int(lobby_type or 0) != 7:
            continue
        mid = int(mid)
        st = int(st)
        if st >= stream_start and mid not in processed:
            candidates.append((st, mid))

    candidates.sort(key=lambda x: x[0])

    added = 0
    wins = 0
    loses = 0

    for st, mid in candidates:
        try:
            details = await steam_get_match_details(mid)
            d = (details.get("result") or {})
            # important fields
            radiant_win = d.get("radiant_win", None)
            players = d.get("players", []) or []
            if radiant_win is None or not players:
                push_error(state, f"details missing fields match_id={mid}")
                continue

            # find our player_slot
            our_slot = None
            for p in players:
                if str(p.get("account_id")) == str(DOTA_ACCOUNT_ID):
                    our_slot = p.get("player_slot")
                    break
            if our_slot is None:
                push_error(state, f"no player_slot for account_id in match_id={mid}")
                continue

            won = is_win_for_player(bool(radiant_win), int(our_slot))
            delta = MMR_STEP if won else -MMR_STEP

            state["mmr"] = int(state.get("mmr", START_MMR)) + delta

            if won:
                state["stream_win"] = int(state.get("stream_win", 0)) + 1
                wins += 1
            else:
                state["stream_lose"] = int(state.get("stream_lose", 0)) + 1
                loses += 1

            state["stream_delta"] = int(state.get("stream_delta", 0)) + delta

            processed.add(mid)
            added += 1

        except httpx.HTTPStatusError as e:
            push_error(state, f"details HTTP error match_id={mid} status={e.response.status_code}")
        except Exception as e:
            push_error(state, f"details error match_id={mid} err={type(e).__name__}")

    state["processed_ids_stream"] = list(processed)[-400:]
    return (added, wins, loses)


# =========================
# TWITCH (EventSub)
# =========================
_twitch_app_token_cache = {"token": None, "exp": 0}


async def twitch_get_app_token() -> str:
    now = time.time()
    if _twitch_app_token_cache["token"] and now < _twitch_app_token_cache["exp"]:
        return _twitch_app_token_cache["token"]

    params = {
        "client_id": TWITCH_CLIENT_ID,
        "client_secret": TWITCH_CLIENT_SECRET,
        "grant_type": "client_credentials",
    }
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(TWITCH_TOKEN_URL, params=params)
        r.raise_for_status()
        data = r.json()
        token = data.get("access_token")
        expires_in = int(data.get("expires_in", 3600))
        if not token:
            raise RuntimeError("No Twitch app token returned")
        _twitch_app_token_cache["token"] = token
        _twitch_app_token_cache["exp"] = now + max(60, expires_in - 60)
        return token


async def twitch_get_user_id_by_login(login: str) -> str:
    token = await twitch_get_app_token()
    headers = {
        "Client-ID": TWITCH_CLIENT_ID,
        "Authorization": f"Bearer {token}",
    }
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(TWITCH_USERS, headers=headers, params={"login": login})
        r.raise_for_status()
        data = r.json()
        arr = data.get("data") or []
        if not arr:
            raise RuntimeError("Broadcaster login not found on Twitch")
        return arr[0]["id"]


async def twitch_list_subscriptions() -> dict:
    token = await twitch_get_app_token()
    headers = {"Client-ID": TWITCH_CLIENT_ID, "Authorization": f"Bearer {token}"}
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(TWITCH_HELIX, headers=headers)
        r.raise_for_status()
        return r.json()


async def twitch_create_subscription(sub_type: str, broadcaster_id: str) -> dict:
    token = await twitch_get_app_token()
    headers = {
        "Client-ID": TWITCH_CLIENT_ID,
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    payload = {
        "type": sub_type,
        "version": "1",
        "condition": {"broadcaster_user_id": broadcaster_id},
        "transport": {
            "method": "webhook",
            "callback": f"{APP_BASE_URL}/eventsub",
            "secret": EVENTSUB_SECRET,
        },
    }
    async with httpx.AsyncClient(timeout=25) as client:
        r = await client.post(TWITCH_HELIX, headers=headers, json=payload)
        r.raise_for_status()
        return r.json()


def verify_eventsub_signature(headers: dict, body: bytes) -> bool:
    # Twitch headers
    msg_id = headers.get("Twitch-Eventsub-Message-Id", "")
    msg_ts = headers.get("Twitch-Eventsub-Message-Timestamp", "")
    sig = headers.get("Twitch-Eventsub-Message-Signature", "")
    if not (msg_id and msg_ts and sig and EVENTSUB_SECRET):
        return False

    h = hmac.new(
        EVENTSUB_SECRET.encode("utf-8"),
        (msg_id + msg_ts).encode("utf-8") + body,
        hashlib.sha256,
    ).hexdigest()
    expected = "sha256=" + h
    return hmac.compare_digest(expected, sig)


# =========================
# ROUTES
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

    # basic checks
    if not DOTA_ACCOUNT_ID:
        return "DOTA_ACCOUNT_ID не установлен"
    if not UPSTASH_URL or not UPSTASH_TOKEN:
        return "Redis не настроен"
    if not STEAM_API_KEY:
        return "STEAM_API_KEY не установлен"

    state_key = f"mmr:{DOTA_ACCOUNT_ID}"
    state = await redis_get_json(state_key)
    if not state:
        state = default_state()
        await redis_set_json(state_key, state)

    # если активный стрим — догоняем новые матчи
    if state.get("stream_active"):
        try:
            await apply_new_stream_matches(state)
        except httpx.HTTPStatusError as e:
            push_error(state, f"history HTTP error status={e.response.status_code}")
        except Exception as e:
            push_error(state, f"history error err={type(e).__name__}")

    await redis_set_json(state_key, state)

    cur = int(state.get("mmr", START_MMR))
    sw = int(state.get("stream_win", 0)) if state.get("stream_active") else 0
    sl = int(state.get("stream_lose", 0)) if state.get("stream_active") else 0
    sd = int(state.get("stream_delta", 0)) if state.get("stream_active") else 0

    text = f"MMR: {cur} • Today -> Win: {sw} Lose: {sl} • Total: {fmt_signed(sd)}"
    _cache_text, _cache_ts = text, now
    return text


@app.get("/streamstatus", response_class=PlainTextResponse)
async def streamstatus(token: str = Query("")):
    if not auth_ok(token):
        return "Forbidden"

    if not UPSTASH_URL or not UPSTASH_TOKEN:
        return "Redis не настроен"
    if not DOTA_ACCOUNT_ID:
        return "DOTA_ACCOUNT_ID не установлен"

    state_key = f"mmr:{DOTA_ACCOUNT_ID}"
    state = await redis_get_json(state_key) or default_state()

    lines = []
    lines.append(f"account_id={DOTA_ACCOUNT_ID}")
    lines.append(f"stream_active={state.get('stream_active')}")
    lines.append(f"stream_start_time={state.get('stream_start_time')}")
    lines.append(f"stream_win={state.get('stream_win')}")
    lines.append(f"stream_lose={state.get('stream_lose')}")
    lines.append(f"stream_delta={state.get('stream_delta')}")
    lines.append(f"mmr={state.get('mmr')}")
    lines.append(f"processed_ids_stream_count={len(state.get('processed_ids_stream') or [])}")

    errs = state.get("last_errors") or []
    if errs:
        lines.append("\nlast_errors:")
        for e in errs[-20:]:
            lines.append(f"- {e}")

    return "\n".join(lines)


@app.get("/reset_stream", response_class=PlainTextResponse)
async def reset_stream(token: str = Query("")):
    if not auth_ok(token):
        return "Forbidden"

    state_key = f"mmr:{DOTA_ACCOUNT_ID}"
    state = await redis_get_json(state_key) or default_state()

    # обнуляем только стрим-статы, mmr не трогаем
    state["stream_win"] = 0
    state["stream_lose"] = 0
    state["stream_delta"] = 0
    state["processed_ids_stream"] = []
    state["stream_start_time"] = now_unix()
    state["stream_active"] = True

    await redis_set_json(state_key, state)
    return "stream stats reset (mmr сохранён)"


@app.get("/stream_backdate", response_class=PlainTextResponse)
async def stream_backdate(token: str = Query(""), hours_ago: int = Query(7, ge=1, le=72)):
    """
    Сдвигает stream_start_time назад на N часов, чтобы засчитать матчи,
    сыгранные до того как EventSub поймал stream.online.
    """
    if not auth_ok(token):
        return "Forbidden"

    state_key = f"mmr:{DOTA_ACCOUNT_ID}"
    state = await redis_get_json(state_key) or default_state()

    back_ts = now_unix() - int(hours_ago) * 3600

    state["stream_active"] = True
    state["stream_start_time"] = back_ts

    # важно: очищаем processed, чтобы можно было пересчитать матчи "за стрим"
    state["processed_ids_stream"] = []
    state["stream_win"] = 0
    state["stream_lose"] = 0
    state["stream_delta"] = 0

    await redis_set_json(state_key, state)
    return f"ok: stream_start_time backdated by {hours_ago}h -> {back_ts}"


@app.get("/testwin", response_class=PlainTextResponse)
async def testwin(token: str = Query("")):
    if not auth_ok(token):
        return "Forbidden"
    state_key = f"mmr:{DOTA_ACCOUNT_ID}"
    state = await redis_get_json(state_key) or default_state()
    state["mmr"] = int(state.get("mmr", START_MMR)) + MMR_STEP
    state["stream_active"] = True
    if int(state.get("stream_start_time", 0)) == 0:
        state["stream_start_time"] = now_unix()
    state["stream_win"] = int(state.get("stream_win", 0)) + 1
    state["stream_delta"] = int(state.get("stream_delta", 0)) + MMR_STEP
    await redis_set_json(state_key, state)
    return "WIN added"


@app.get("/testlose", response_class=PlainTextResponse)
async def testlose(token: str = Query("")):
    if not auth_ok(token):
        return "Forbidden"
    state_key = f"mmr:{DOTA_ACCOUNT_ID}"
    state = await redis_get_json(state_key) or default_state()
    state["mmr"] = int(state.get("mmr", START_MMR)) - MMR_STEP
    state["stream_active"] = True
    if int(state.get("stream_start_time", 0)) == 0:
        state["stream_start_time"] = now_unix()
    state["stream_lose"] = int(state.get("stream_lose", 0)) + 1
    state["stream_delta"] = int(state.get("stream_delta", 0)) - MMR_STEP
    await redis_set_json(state_key, state)
    return "LOSE added"


@app.get("/eventsub_setup", response_class=PlainTextResponse)
async def eventsub_setup(token: str = Query("")):
    if not auth_ok(token):
        return "Forbidden"

    # checks
    missing = []
    if not TWITCH_CLIENT_ID: missing.append("TWITCH_CLIENT_ID")
    if not TWITCH_CLIENT_SECRET: missing.append("TWITCH_CLIENT_SECRET")
    if not TWITCH_BROADCASTER_LOGIN: missing.append("TWITCH_BROADCASTER_LOGIN")
    if not EVENTSUB_SECRET: missing.append("EVENTSUB_SECRET")
    if missing:
        return "Missing env: " + ", ".join(missing)

    broadcaster_id = await twitch_get_user_id_by_login(TWITCH_BROADCASTER_LOGIN)

    # list existing subs and create if missing
    existing = await twitch_list_subscriptions()
    data = existing.get("data") or []

    def has_type(t: str) -> bool:
        for s in data:
            if s.get("type") == t and (s.get("condition") or {}).get("broadcaster_user_id") == broadcaster_id:
                return True
        return False

    created = []
    if not has_type("stream.online"):
        await twitch_create_subscription("stream.online", broadcaster_id)
        created.append("stream.online")
    if not has_type("stream.offline"):
        await twitch_create_subscription("stream.offline", broadcaster_id)
        created.append("stream.offline")

    lines = []
    lines.append("ok")
    lines.append(f"callback: {APP_BASE_URL}/eventsub")
    lines.append(f"online: {'created' if 'stream.online' in created else 'exists'}")
    lines.append(f"offline: {'created' if 'stream.offline' in created else 'exists'}")
    return "\n".join(lines)


@app.post("/eventsub")
async def eventsub(request: Request):
    body = await request.body()

    msg_type = request.headers.get("Twitch-Eventsub-Message-Type", "")
    # 1) verification
    if msg_type == "webhook_callback_verification":
        data = await request.json()
        challenge = data.get("challenge", "")
        return PlainTextResponse(challenge)

    # 2) signature verify
    if not verify_eventsub_signature(dict(request.headers), body):
        raise HTTPException(status_code=403, detail="bad signature")

    # 3) notification
    if msg_type == "notification":
        payload = await request.json()
        sub = payload.get("subscription") or {}
        ev = payload.get("event") or {}
        sub_type = sub.get("type")

        state_key = f"mmr:{DOTA_ACCOUNT_ID}"
        state = await redis_get_json(state_key) or default_state()

        if sub_type == "stream.online":
            # если стрим уже был “по факту” начат раньше, но событие пришло сейчас — позже сделаем backdate ручкой
            state["stream_active"] = True
            if not state.get("stream_start_time"):
                state["stream_start_time"] = now_unix()
            # обнуляем stats на новый стрим
            state["stream_win"] = 0
            state["stream_lose"] = 0
            state["stream_delta"] = 0
            state["processed_ids_stream"] = []

        elif sub_type == "stream.offline":
            state["stream_active"] = False

        await redis_set_json(state_key, state)

    return PlainTextResponse("ok")


# =========================
# DEBUG (optional)
# =========================
@app.get("/debug_last_matches", response_class=PlainTextResponse)
async def debug_last_matches(token: str = Query("")):
    if not auth_ok(token):
        return "Forbidden"

    state_key = f"mmr:{DOTA_ACCOUNT_ID}"
    state = await redis_get_json(state_key) or default_state()

    history = await steam_get_match_history(DOTA_ACCOUNT_ID, matches_requested=15)
    result = history.get("result", {})
    matches = result.get("matches", []) or []

    lines = []
    lines.append(f"account_id={DOTA_ACCOUNT_ID}")
    lines.append(f"got_matches={len(matches)}")
    lines.append("last_15:")
    for m in matches:
        lines.append(f"- match_id={m.get('match_id')} start_time={m.get('start_time')} lobby_type={m.get('lobby_type')}")

    lines.append("\nstate:")
    lines.append(f"stream_active={state.get('stream_active')}")
    lines.append(f"stream_start_time={state.get('stream_start_time')}")
    lines.append(f"processed_ids_stream_count={len(state.get('processed_ids_stream') or [])}")
    lines.append(f"mmr={state.get('mmr')}")

    return "\n".join(lines)
