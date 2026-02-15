import os
import json
import time
import hmac
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, List

import httpx
from fastapi import FastAPI, Request, Query
from fastapi.responses import PlainTextResponse

# -------------------------
# ENV
# -------------------------
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "")

DOTA_ACCOUNT_ID = os.environ.get("DOTA_ACCOUNT_ID")  # 32-bit id (например 1258333388)
START_MMR = int(os.environ.get("START_MMR", "13772"))
MMR_STEP = int(os.environ.get("MMR_STEP", "25"))
TZ_OFFSET_HOURS = int(os.environ.get("TZ_OFFSET_HOURS", "3"))

UPSTASH_URL = os.environ.get("UPSTASH_REDIS_REST_URL")
UPSTASH_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN")

STEAM_API_KEY = os.environ.get("STEAM_API_KEY")  # Steam Web API key

TWITCH_CLIENT_ID = os.environ.get("TWITCH_CLIENT_ID")
TWITCH_CLIENT_SECRET = os.environ.get("TWITCH_CLIENT_SECRET")
TWITCH_BROADCASTER_LOGIN = os.environ.get("TWITCH_BROADCASTER_LOGIN", "debustie")
EVENTSUB_SECRET = os.environ.get("EVENTSUB_SECRET", "")  # 20-40 символов

# -------------------------
# CONST
# -------------------------
STEAM_BASE = "https://api.steampowered.com"
STEAM_MATCH_HISTORY = f"{STEAM_BASE}/IDOTA2Match_570/GetMatchHistory/v1"
STEAM_MATCH_DETAILS = f"{STEAM_BASE}/IDOTA2Match_570/GetMatchDetails/v1"

TWITCH_OAUTH = "https://id.twitch.tv/oauth2/token"
TWITCH_HELIX = "https://api.twitch.tv/helix"

CACHE_TTL = 5  # чтобы Fossabot не долбил апи 10 раз подряд
DETAILS_TIMEOUT = 20
HISTORY_TIMEOUT = 20

app = FastAPI()

_cache_text: Optional[str] = None
_cache_ts: float = 0.0


# -------------------------
# TIME
# -------------------------
def tz_msk():
    return timezone(timedelta(hours=TZ_OFFSET_HOURS))


def fmt_signed(n: int) -> str:
    return f"+{n}" if n >= 0 else str(n)


def now_unix() -> int:
    return int(time.time())


# -------------------------
# REDIS (Upstash REST)
# -------------------------
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


# -------------------------
# STATE
# -------------------------
def state_key() -> str:
    return f"mmr:{DOTA_ACCOUNT_ID}"


def default_state() -> dict:
    return {
        "mmr": START_MMR,

        # stream session
        "stream_active": False,
        "stream_start_time": 0,   # unix when stream.online received
        "stream_win": 0,
        "stream_lose": 0,
        "stream_delta": 0,

        # processed matches for CURRENT stream (match_id set)
        "processed_ids_stream": [],

        # debug: last errors
        "last_errors": [],  # list of strings
    }


def push_error(st: dict, msg: str) -> None:
    errs = st.get("last_errors", [])
    ts = now_unix()
    errs.append(f"{ts}: {msg}")
    st["last_errors"] = errs[-30:]


# -------------------------
# AUTH
# -------------------------
def require_admin(token: str) -> bool:
    return bool(ADMIN_TOKEN) and token == ADMIN_TOKEN


# -------------------------
# STEAM API
# -------------------------
async def steam_get_match_history(limit: int = 20) -> List[dict]:
    """
    Возвращает матчи для account_id. Внутри есть start_time и lobby_type.
    """
    params = {
        "key": STEAM_API_KEY,
        "account_id": int(DOTA_ACCOUNT_ID),
        "matches_requested": limit,
    }
    async with httpx.AsyncClient(timeout=HISTORY_TIMEOUT) as client:
        r = await client.get(STEAM_MATCH_HISTORY, params=params)
        r.raise_for_status()
        data = r.json()
    result = data.get("result", {})
    return result.get("matches", []) or []


async def steam_get_match_details(match_id: int) -> Optional[dict]:
    """
    Детали матча: кто победил и в какой команде игрок.
    """
    params = {"key": STEAM_API_KEY, "match_id": int(match_id)}
    async with httpx.AsyncClient(timeout=DETAILS_TIMEOUT) as client:
        r = await client.get(STEAM_MATCH_DETAILS, params=params)

        # Steam иногда реально отвечает 500/503 — ловим и не валим всё.
        if r.status_code != 200:
            return {"_error_status": r.status_code, "_error_text": (r.text or "")[:200]}

        data = r.json()
    return data.get("result") or None


def determine_win_for_account(details: dict, account_id: int) -> Optional[bool]:
    """
    True = win, False = lose, None = cannot determine
    """
    # в Steam details обычно есть radiant_win: true/false
    radiant_win = details.get("radiant_win")
    players = details.get("players") or []
    if radiant_win is None or not players:
        return None

    # team_number: 0 radiant, 1 dire
    team_number = None
    for p in players:
        if int(p.get("account_id", -1)) == int(account_id):
            team_number = int(p.get("team_number", -1))
            break

    if team_number not in (0, 1):
        return None

    if team_number == 0:
        return bool(radiant_win)
    else:
        return not bool(radiant_win)


# -------------------------
# TWITCH EventSub (online/offline)
# -------------------------
async def twitch_app_token() -> str:
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(
            TWITCH_OAUTH,
            params={
                "client_id": TWITCH_CLIENT_ID,
                "client_secret": TWITCH_CLIENT_SECRET,
                "grant_type": "client_credentials",
            },
        )
        r.raise_for_status()
        return r.json()["access_token"]


async def twitch_get_user_id(login: str, token: str) -> Optional[str]:
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(
            f"{TWITCH_HELIX}/users",
            params={"login": login},
            headers={"Client-ID": TWITCH_CLIENT_ID, "Authorization": f"Bearer {token}"},
        )
        r.raise_for_status()
        data = r.json().get("data") or []
        if not data:
            return None
        return data[0]["id"]


async def twitch_list_eventsub(token: str) -> List[dict]:
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(
            f"{TWITCH_HELIX}/eventsub/subscriptions",
            headers={"Client-ID": TWITCH_CLIENT_ID, "Authorization": f"Bearer {token}"},
        )
        r.raise_for_status()
        return r.json().get("data") or []


async def twitch_create_eventsub(token: str, typ: str, broadcaster_user_id: str, callback: str) -> dict:
    body = {
        "type": typ,
        "version": "1",
        "condition": {"broadcaster_user_id": broadcaster_user_id},
        "transport": {
            "method": "webhook",
            "callback": callback,
            "secret": EVENTSUB_SECRET,
        },
    }
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(
            f"{TWITCH_HELIX}/eventsub/subscriptions",
            headers={
                "Client-ID": TWITCH_CLIENT_ID,
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json=body,
        )
        r.raise_for_status()
        return r.json()


def verify_eventsub_signature(headers: dict, body: bytes) -> bool:
    """
    Twitch подписывает:
    message = id + timestamp + body
    signature = "sha256=" + HMAC_SHA256(secret, message)
    """
    msg_id = headers.get("Twitch-Eventsub-Message-Id", "")
    msg_ts = headers.get("Twitch-Eventsub-Message-Timestamp", "")
    sig = headers.get("Twitch-Eventsub-Message-Signature", "")

    if not (msg_id and msg_ts and sig and EVENTSUB_SECRET):
        return False

    message = (msg_id + msg_ts).encode("utf-8") + body
    expected = hmac.new(EVENTSUB_SECRET.encode("utf-8"), message, hashlib.sha256).hexdigest()
    return sig == f"sha256={expected}"


# -------------------------
# ROUTES
# -------------------------
@app.get("/health", response_class=PlainTextResponse)
async def health():
    return "ok"


@app.get("/mmr", response_class=PlainTextResponse)
async def mmr():
    """
    Fossabot дергает это: $(customapi https://mmr-bot.onrender.com/mmr)
    """
    global _cache_text, _cache_ts

    now = time.time()
    if _cache_text and (now - _cache_ts) < CACHE_TTL:
        return _cache_text

    # basic checks
    if not DOTA_ACCOUNT_ID:
        return "DOTA_ACCOUNT_ID не установлен"
    if not (UPSTASH_URL and UPSTASH_TOKEN):
        return "Redis не настроен"
    if not STEAM_API_KEY:
        return "STEAM_API_KEY не установлен"

    st = await redis_get_json(state_key())
    if not st:
        st = default_state()
        await redis_set_json(state_key(), st)

    # если стрим не активен — просто показываем нули по стриму, MMR как есть
    if not st.get("stream_active", False):
        cur = int(st.get("mmr", START_MMR))
        text = f"MMR: {cur} • Today -> Win: 0 Lose: 0 • Total: +0"
        _cache_text, _cache_ts = text, now
        return text

    # стрим активен — подтягиваем матчи и считаем только с начала стрима
    stream_start = int(st.get("stream_start_time", 0))
    processed_stream = set(st.get("processed_ids_stream", []))

    try:
        matches = await steam_get_match_history(limit=20)
    except Exception as e:
        push_error(st, f"history_error: {type(e).__name__}: {e}")
        await redis_set_json(state_key(), st)
        cur = int(st.get("mmr", START_MMR))
        text = f"MMR: {cur} • Today -> Win: {int(st.get('stream_win',0))} Lose: {int(st.get('stream_lose',0))} • Total: {fmt_signed(int(st.get('stream_delta',0)))}"
        _cache_text, _cache_ts = text, now
        return text

    # берём только ranked и только те, что начались после stream_start
    ranked_after_stream = []
    for m in matches:
        lobby_type = int(m.get("lobby_type", -1))
        start_time = int(m.get("start_time", 0) or 0)
        match_id = int(m.get("match_id", 0) or 0)
        if lobby_type != 7:
            continue
        if start_time < stream_start:
            continue
        if match_id <= 0:
            continue
        ranked_after_stream.append({"match_id": match_id, "start_time": start_time})

    # сортируем по времени (старые -> новые)
    ranked_after_stream.sort(key=lambda x: x["start_time"])

    # обрабатываем новые матчи
    for item in ranked_after_stream:
        mid = item["match_id"]
        if mid in processed_stream:
            continue

        details = await steam_get_match_details(mid)
        if not details:
            push_error(st, f"details_empty match_id={mid}")
            continue

        if "_error_status" in details:
            push_error(st, f"details_error match_id={mid} status={details.get('_error_status')} body={details.get('_error_text','')}")
            continue

        won = determine_win_for_account(details, int(DOTA_ACCOUNT_ID))
        if won is None:
            push_error(st, f"cannot_determine match_id={mid}")
            continue

        delta = MMR_STEP if won else -MMR_STEP

        st["mmr"] = int(st.get("mmr", START_MMR)) + delta
        if won:
            st["stream_win"] = int(st.get("stream_win", 0)) + 1
        else:
            st["stream_lose"] = int(st.get("stream_lose", 0)) + 1
        st["stream_delta"] = int(st.get("stream_delta", 0)) + delta

        processed_stream.add(mid)

    st["processed_ids_stream"] = list(processed_stream)[-200:]
    await redis_set_json(state_key(), st)

    cur = int(st.get("mmr", START_MMR))
    tw = int(st.get("stream_win", 0))
    tl = int(st.get("stream_lose", 0))
    td = int(st.get("stream_delta", 0))

    text = f"MMR: {cur} • Today -> Win: {tw} Lose: {tl} • Total: {fmt_signed(td)}"
    _cache_text, _cache_ts = text, now
    return text


# -------------------------
# ADMIN / DEBUG
# -------------------------
@app.get("/streamstatus", response_class=PlainTextResponse)
async def streamstatus(token: str = Query("")):
    if not require_admin(token):
        return "Forbidden"

    st = await redis_get_json(state_key()) or default_state()

    lines = [
        f"account_id={DOTA_ACCOUNT_ID}",
        f"stream_active={st.get('stream_active', False)}",
        f"stream_start_time={st.get('stream_start_time', 0)}",
        f"stream_win={st.get('stream_win', 0)}",
        f"stream_lose={st.get('stream_lose', 0)}",
        f"stream_delta={st.get('stream_delta', 0)}",
        f"mmr={st.get('mmr', START_MMR)}",
        f"processed_ids_stream_count={len(st.get('processed_ids_stream', []))}",
        "",
        "last_errors:",
    ]
    for e in st.get("last_errors", []):
        lines.append(f"- {e}")
    return "\n".join(lines)


@app.get("/reset_stream", response_class=PlainTextResponse)
async def reset_stream(token: str = Query("")):
    if not require_admin(token):
        return "Forbidden"

    st = await redis_get_json(state_key()) or default_state()
    st["stream_win"] = 0
    st["stream_lose"] = 0
    st["stream_delta"] = 0
    st["processed_ids_stream"] = []
    await redis_set_json(state_key(), st)
    return "ok: stream counters reset"


@app.get("/force_stream_on", response_class=PlainTextResponse)
async def force_stream_on(token: str = Query("")):
    if not require_admin(token):
        return "Forbidden"

    st = await redis_get_json(state_key()) or default_state()
    st["stream_active"] = True
    st["stream_start_time"] = now_unix()
    st["stream_win"] = 0
    st["stream_lose"] = 0
    st["stream_delta"] = 0
    st["processed_ids_stream"] = []
    await redis_set_json(state_key(), st)
    return "ok: stream_active=True (forced)"


@app.get("/force_stream_off", response_class=PlainTextResponse)
async def force_stream_off(token: str = Query("")):
    if not require_admin(token):
        return "Forbidden"

    st = await redis_get_json(state_key()) or default_state()
    st["stream_active"] = False
    await redis_set_json(state_key(), st)
    return "ok: stream_active=False (forced)"


@app.get("/testwin", response_class=PlainTextResponse)
async def testwin(token: str = Query("")):
    if not require_admin(token):
        return "Forbidden"

    st = await redis_get_json(state_key()) or default_state()
    st["mmr"] = int(st.get("mmr", START_MMR)) + MMR_STEP
    if st.get("stream_active", False):
        st["stream_win"] = int(st.get("stream_win", 0)) + 1
        st["stream_delta"] = int(st.get("stream_delta", 0)) + MMR_STEP
    await redis_set_json(state_key(), st)
    return "ok: WIN added"


@app.get("/testlose", response_class=PlainTextResponse)
async def testlose(token: str = Query("")):
    if not require_admin(token):
        return "Forbidden"

    st = await redis_get_json(state_key()) or default_state()
    st["mmr"] = int(st.get("mmr", START_MMR)) - MMR_STEP
    if st.get("stream_active", False):
        st["stream_lose"] = int(st.get("stream_lose", 0)) + 1
        st["stream_delta"] = int(st.get("stream_delta", 0)) - MMR_STEP
    await redis_set_json(state_key(), st)
    return "ok: LOSE added"


@app.get("/debug_last_matches", response_class=PlainTextResponse)
async def debug_last_matches(token: str = Query("")):
    if not require_admin(token):
        return "Forbidden"

    st = await redis_get_json(state_key()) or default_state()

    matches = await steam_get_match_history(limit=15)
    lines = [
        f"account_id={DOTA_ACCOUNT_ID}",
        f"got_matches={len(matches)}",
        "last_15:",
    ]
    for m in matches:
        lines.append(f"- match_id={m.get('match_id')} start_time={m.get('start_time')} lobby_type={m.get('lobby_type')}")
    lines.append("")
    lines.append("state:")
    lines.append(f"stream_active={st.get('stream_active', False)}")
    lines.append(f"stream_start_time={st.get('stream_start_time', 0)}")
    lines.append(f"processed_ids_stream_count={len(st.get('processed_ids_stream', []))}")
    lines.append(f"mmr={st.get('mmr', START_MMR)}")
    return "\n".join(lines)


@app.get("/debug_match", response_class=PlainTextResponse)
async def debug_match(match_id: int, token: str = Query("")):
    if not require_admin(token):
        return "Forbidden"

    details = await steam_get_match_details(int(match_id))
    if not details:
        return "details: None"

    if "_error_status" in details:
        return f"details_error: status={details.get('_error_status')}\n{details.get('_error_text','')}"

    won = determine_win_for_account(details, int(DOTA_ACCOUNT_ID))
    return f"won={won}\nkeys={list(details.keys())}"


# -------------------------
# EventSub setup + callback
# -------------------------
@app.get("/eventsub_setup", response_class=PlainTextResponse)
async def eventsub_setup(token: str = Query("")):
    if not require_admin(token):
        return "Forbidden"

    if not (TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET and EVENTSUB_SECRET):
        return "Missing TWITCH_CLIENT_ID/TWITCH_CLIENT_SECRET/EVENTSUB_SECRET"

    app_token = await twitch_app_token()
    broadcaster_id = await twitch_get_user_id(TWITCH_BROADCASTER_LOGIN, app_token)
    if not broadcaster_id:
        return "Cannot find broadcaster user id"

    existing = await twitch_list_eventsub(app_token)
    want_types = {"stream.online", "stream.offline"}

    has_online = any(s.get("type") == "stream.online" and (s.get("condition") or {}).get("broadcaster_user_id") == broadcaster_id for s in existing)
    has_offline = any(s.get("type") == "stream.offline" and (s.get("condition") or {}).get("broadcaster_user_id") == broadcaster_id for s in existing)

    callback = "https://mmr-bot.onrender.com/eventsub"

    # создаём недостающие
    if not has_online:
        await twitch_create_eventsub(app_token, "stream.online", broadcaster_id, callback)
    if not has_offline:
        await twitch_create_eventsub(app_token, "stream.offline", broadcaster_id, callback)

    return "\n".join([
        "ok",
        f"callback: {callback}",
        f"online: {'exists' if has_online else 'created'}",
        f"offline: {'exists' if has_offline else 'created'}",
    ])


@app.post("/eventsub", response_class=PlainTextResponse)
async def eventsub(request: Request):
    body = await request.body()
    headers = request.headers

    # проверка подписи
    if not verify_eventsub_signature(headers, body):
        return "Forbidden"

    msg_type = headers.get("Twitch-Eventsub-Message-Type", "")
    payload = {}
    try:
        payload = json.loads(body.decode("utf-8"))
    except Exception:
        payload = {}

    # verification challenge
    if msg_type == "webhook_callback_verification":
        challenge = payload.get("challenge", "")
        return challenge or "ok"

    # notifications
    if msg_type == "notification":
        sub = payload.get("subscription", {}) or {}
        typ = sub.get("type")

        st = await redis_get_json(state_key()) or default_state()

        # stream.online => начинаем новый "счетчик стрима"
        if typ == "stream.online":
            st["stream_active"] = True
            st["stream_start_time"] = now_unix()
            st["stream_win"] = 0
            st["stream_lose"] = 0
            st["stream_delta"] = 0
            st["processed_ids_stream"] = []
            await redis_set_json(state_key(), st)
            return "ok"

        # stream.offline => просто выключаем
        if typ == "stream.offline":
            st["stream_active"] = False
            await redis_set_json(state_key(), st)
            return "ok"

        return "ok"

    return "ok"
