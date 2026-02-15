import os
import json
import time
import hmac
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, List

import httpx
from fastapi import FastAPI, Request, Query, HTTPException
from fastapi.responses import PlainTextResponse

# =========================
# ENV
# =========================
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "")

DOTA_ACCOUNT_ID = os.environ.get("DOTA_ACCOUNT_ID")  # 32-bit (пример: 1258333388)

START_MMR = int(os.environ.get("START_MMR", "13772"))
MMR_STEP = int(os.environ.get("MMR_STEP", "25"))
TZ_OFFSET_HOURS = int(os.environ.get("TZ_OFFSET_HOURS", "3"))

UPSTASH_URL = os.environ.get("UPSTASH_REDIS_REST_URL")
UPSTASH_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN")

# Twitch EventSub
TWITCH_CLIENT_ID = os.environ.get("TWITCH_CLIENT_ID", "")
TWITCH_CLIENT_SECRET = os.environ.get("TWITCH_CLIENT_SECRET", "")
TWITCH_BROADCASTER_LOGIN = os.environ.get("TWITCH_BROADCASTER_LOGIN", "")  # debustie
EVENTSUB_SECRET = os.environ.get("EVENTSUB_SECRET", "")  # 20-40 символов

# =========================
# CONST
# =========================
OPENDOTA = "https://api.opendota.com/api"
TWITCH_OAUTH = "https://id.twitch.tv/oauth2/token"
TWITCH_API = "https://api.twitch.tv/helix"

CACHE_TTL = 10  # сек, кэш ответа /mmr

STATE_KEY_PREFIX = "mmr_state:"
TWITCH_KEY_PREFIX = "twitch_state:"

app = FastAPI()

_cache_text: Optional[str] = None
_cache_ts: float = 0.0


# =========================
# Helpers
# =========================
def tz_msk():
    return timezone(timedelta(hours=TZ_OFFSET_HOURS))


def now_unix() -> int:
    return int(time.time())


def fmt_signed(n: int) -> str:
    return f"+{n}" if n >= 0 else str(n)


def require_admin(token: str):
    if not ADMIN_TOKEN:
        raise HTTPException(status_code=500, detail="ADMIN_TOKEN not set")
    if token != ADMIN_TOKEN:
        raise HTTPException(status_code=403, detail="Forbidden")


def is_win_for_player(radiant_win: bool, player_slot: int) -> bool:
    # player_slot < 128 => Radiant
    is_radiant = int(player_slot) < 128
    return bool(radiant_win) if is_radiant else (not bool(radiant_win))


async def redis_get_json(key: str) -> Optional[Dict[str, Any]]:
    if not UPSTASH_URL or not UPSTASH_TOKEN:
        return None
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
        try:
            return json.loads(val)
        except Exception:
            return None


async def redis_set_json(key: str, obj: Dict[str, Any]) -> None:
    if not UPSTASH_URL or not UPSTASH_TOKEN:
        return
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(
            f"{UPSTASH_URL}/set/{key}",
            headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
            data=json.dumps(obj, ensure_ascii=False),
        )
        r.raise_for_status()


def default_state() -> Dict[str, Any]:
    return {
        "mmr": START_MMR,

        # stream stats (считаем только в рамках активного стрима)
        "stream_active": False,
        "stream_start_time": 0,   # unix (сек)
        "stream_win": 0,
        "stream_lose": 0,
        "stream_delta": 0,
        "processed_ids_stream": [],  # match_id которые уже посчитали в этом стриме

        # для дебага/ошибок
        "last_errors": [],  # список строк
    }


def migrate_state(state: Dict[str, Any]) -> Dict[str, Any]:
    # чтобы после старых версий ничего не ломалось
    base = default_state()
    base.update(state or {})
    # гарантируем типы
    base["mmr"] = int(base.get("mmr", START_MMR))

    base["stream_active"] = bool(base.get("stream_active", False))
    base["stream_start_time"] = int(base.get("stream_start_time", 0))
    base["stream_win"] = int(base.get("stream_win", 0))
    base["stream_lose"] = int(base.get("stream_lose", 0))
    base["stream_delta"] = int(base.get("stream_delta", 0))

    p = base.get("processed_ids_stream", [])
    if not isinstance(p, list):
        p = []
    base["processed_ids_stream"] = p[-500:]

    le = base.get("last_errors", [])
    if not isinstance(le, list):
        le = []
    base["last_errors"] = le[-40:]
    return base


def push_error(state: Dict[str, Any], msg: str):
    state.setdefault("last_errors", [])
    state["last_errors"].append(f"{now_unix()}: {msg}")
    state["last_errors"] = state["last_errors"][-40:]


async def fetch_ranked_matches_opendota(account_id: str, limit: int = 50) -> List[Dict[str, Any]]:
    # lobby_type=7 => Ranked matchmaking
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(
            f"{OPENDOTA}/players/{account_id}/matches",
            params={"lobby_type": 7, "limit": limit},
        )
        r.raise_for_status()
        return r.json() or []


def get_callback_base(request: Request) -> str:
    # Render проксирует https, поэтому берём host и форсим https
    host = request.headers.get("x-forwarded-host") or request.headers.get("host")
    scheme = request.headers.get("x-forwarded-proto") or "https"
    return f"{scheme}://{host}"


# =========================
# Health
# =========================
@app.get("/health", response_class=PlainTextResponse)
async def health():
    return "ok"


# =========================
# MMR endpoint (public)
# =========================
@app.get("/mmr", response_class=PlainTextResponse)
async def mmr():
    """
    Вывод: MMR: 13772 • Today -> Win: 0 Lose: 0 • Total: +0
    Здесь Today = статистика ТОЛЬКО ЗА ТЕКУЩИЙ СТРИМ.
    """
    global _cache_text, _cache_ts

    now = time.time()
    if _cache_text and (now - _cache_ts) < CACHE_TTL:
        return _cache_text

    if not DOTA_ACCOUNT_ID:
        return "DOTA_ACCOUNT_ID не установлен"
    if not UPSTASH_URL or not UPSTASH_TOKEN:
        return "Redis не настроен"

    state_key = f"{STATE_KEY_PREFIX}{DOTA_ACCOUNT_ID}"
    state = await redis_get_json(state_key) or {}
    state = migrate_state(state)

    # Если стрим активен — подтягиваем матчи и считаем новые (только начиная со stream_start_time)
    if state["stream_active"] and state["stream_start_time"] > 0:
        try:
            matches = await fetch_ranked_matches_opendota(DOTA_ACCOUNT_ID, limit=60)
        except Exception as e:
            push_error(state, f"opendota_error: {type(e).__name__}: {e}")
            await redis_set_json(state_key, state)
            # даже если API упал — всё равно отдаём текущие цифры
            matches = []

        processed = set(int(x) for x in state.get("processed_ids_stream", []) if str(x).isdigit())

        # берём только матчи, которые начались ПОСЛЕ старта стрима
        candidates = []
        for m in matches:
            mid = m.get("match_id")
            st = m.get("start_time")
            if not mid or not st:
                continue
            mid = int(mid)
            st = int(st)
            if st < int(state["stream_start_time"]):
                continue
            if mid in processed:
                continue
            # нужно чтобы были поля для определения победы
            if m.get("radiant_win") is None or m.get("player_slot") is None:
                continue
            candidates.append(m)

        # сортируем по времени
        candidates.sort(key=lambda x: int(x.get("start_time", 0)))

        for m in candidates:
            mid = int(m["match_id"])
            st = int(m["start_time"])
            won = is_win_for_player(bool(m["radiant_win"]), int(m["player_slot"]))
            delta = MMR_STEP if won else -MMR_STEP

            state["mmr"] = int(state["mmr"]) + delta
            if won:
                state["stream_win"] += 1
            else:
                state["stream_lose"] += 1
            state["stream_delta"] += delta

            processed.add(mid)

        state["processed_ids_stream"] = list(processed)[-500:]
        await redis_set_json(state_key, state)

    # вывод
    cur = int(state["mmr"])
    w = int(state["stream_win"])
    l = int(state["stream_lose"])
    d = int(state["stream_delta"])
    text = f"MMR: {cur} • Today -> Win: {w} Lose: {l} • Total: {fmt_signed(d)}"

    _cache_text, _cache_ts = text, now
    return text


# =========================
# Admin endpoints
# =========================
def clear_cache():
    global _cache_text, _cache_ts
    _cache_text = None
    _cache_ts = 0.0


@app.get("/streamstatus", response_class=PlainTextResponse)
async def streamstatus(token: str = Query("")):
    require_admin(token)
    if not DOTA_ACCOUNT_ID:
        return "DOTA_ACCOUNT_ID not set"
    state_key = f"{STATE_KEY_PREFIX}{DOTA_ACCOUNT_ID}"
    state = await redis_get_json(state_key) or {}
    state = migrate_state(state)

    lines = [
        f"account_id={DOTA_ACCOUNT_ID}",
        f"stream_active={state['stream_active']}",
        f"stream_start_time={state['stream_start_time']}",
        f"stream_win={state['stream_win']}",
        f"stream_lose={state['stream_lose']}",
        f"stream_delta={state['stream_delta']}",
        f"mmr={state['mmr']}",
        f"processed_ids_stream_count={len(state.get('processed_ids_stream', []))}",
        "",
        "last_errors:",
    ]
    for e in state.get("last_errors", [])[-20:]:
        lines.append(f"- {e}")
    return "\n".join(lines)


@app.get("/reset_stream", response_class=PlainTextResponse)
async def reset_stream(token: str = Query("")):
    require_admin(token)
    clear_cache()

    state_key = f"{STATE_KEY_PREFIX}{DOTA_ACCOUNT_ID}"
    state = await redis_get_json(state_key) or {}
    state = migrate_state(state)

    # Сброс только стрим-статы, MMR оставляем как есть
    state["stream_win"] = 0
    state["stream_lose"] = 0
    state["stream_delta"] = 0
    state["processed_ids_stream"] = []

    await redis_set_json(state_key, state)
    return "ok: stream stats reset (mmr preserved)"


@app.get("/hard_reset", response_class=PlainTextResponse)
async def hard_reset(token: str = Query("")):
    require_admin(token)
    clear_cache()

    state_key = f"{STATE_KEY_PREFIX}{DOTA_ACCOUNT_ID}"
    state = await redis_get_json(state_key) or {}
    state = migrate_state(state)

    state["mmr"] = START_MMR
    state["stream_win"] = 0
    state["stream_lose"] = 0
    state["stream_delta"] = 0
    state["processed_ids_stream"] = []

    await redis_set_json(state_key, state)
    return f"ok: mmr reset to {START_MMR}"


@app.get("/testwin", response_class=PlainTextResponse)
async def testwin(token: str = Query("")):
    require_admin(token)
    clear_cache()
    state_key = f"{STATE_KEY_PREFIX}{DOTA_ACCOUNT_ID}"
    state = await redis_get_json(state_key) or {}
    state = migrate_state(state)

    state["mmr"] += MMR_STEP
    state["stream_win"] += 1
    state["stream_delta"] += MMR_STEP

    await redis_set_json(state_key, state)
    return "ok: WIN added"


@app.get("/testlose", response_class=PlainTextResponse)
async def testlose(token: str = Query("")):
    require_admin(token)
    clear_cache()
    state_key = f"{STATE_KEY_PREFIX}{DOTA_ACCOUNT_ID}"
    state = await redis_get_json(state_key) or {}
    state = migrate_state(state)

    state["mmr"] -= MMR_STEP
    state["stream_lose"] += 1
    state["stream_delta"] -= MMR_STEP

    await redis_set_json(state_key, state)
    return "ok: LOSE added"


# =========================
# Twitch EventSub
# =========================
async def twitch_get_app_token() -> str:
    """
    Кладём app token в Redis, чтобы не получать его каждый раз.
    """
    key = f"{TWITCH_KEY_PREFIX}app_token"
    cached = await redis_get_json(key)
    if cached and cached.get("token") and cached.get("exp", 0) > now_unix() + 60:
        return cached["token"]

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
        data = r.json()
        token = data["access_token"]
        exp = now_unix() + int(data.get("expires_in", 0))
        await redis_set_json(key, {"token": token, "exp": exp})
        return token


async def twitch_api_get(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    token = await twitch_get_app_token()
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(
            f"{TWITCH_API}{path}",
            params=params,
            headers={
                "Client-ID": TWITCH_CLIENT_ID,
                "Authorization": f"Bearer {token}",
            },
        )
        r.raise_for_status()
        return r.json()


async def twitch_api_post(path: str, json_body: Dict[str, Any]) -> Dict[str, Any]:
    token = await twitch_get_app_token()
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(
            f"{TWITCH_API}{path}",
            json=json_body,
            headers={
                "Client-ID": TWITCH_CLIENT_ID,
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
        )
        # Twitch может вернуть 409 если уже существует — это не критично, разрулим в вызывающем
        if r.status_code >= 400:
            try:
                return {"_status": r.status_code, "_body": r.text}
            except Exception:
                return {"_status": r.status_code, "_body": ""}
        return r.json()


async def twitch_get_user_id_by_login(login: str) -> str:
    data = await twitch_api_get("/users", {"login": login})
    arr = data.get("data") or []
    if not arr:
        raise RuntimeError(f"twitch user not found: {login}")
    return arr[0]["id"]


async def twitch_stream_is_live(login: str) -> bool:
    data = await twitch_api_get("/streams", {"user_login": login})
    arr = data.get("data") or []
    return len(arr) > 0


def eventsub_verify(request: Request, body: bytes) -> None:
    # Twitch headers
    msg_id = request.headers.get("Twitch-Eventsub-Message-Id", "")
    msg_ts = request.headers.get("Twitch-Eventsub-Message-Timestamp", "")
    msg_sig = request.headers.get("Twitch-Eventsub-Message-Signature", "")

    if not (msg_id and msg_ts and msg_sig):
        raise HTTPException(status_code=403, detail="Missing EventSub headers")

    if not EVENTSUB_SECRET:
        raise HTTPException(status_code=500, detail="EVENTSUB_SECRET not set")

    to_sign = (msg_id + msg_ts).encode("utf-8") + body
    mac = hmac.new(EVENTSUB_SECRET.encode("utf-8"), msg=to_sign, digestmod=hashlib.sha256)
    expected = "sha256=" + mac.hexdigest()

    # сравнение по времени
    if not hmac.compare_digest(expected, msg_sig):
        raise HTTPException(status_code=403, detail="Bad signature")


async def set_stream_active(active: bool):
    """
    При включении стрима:
      - stream_active=True
      - stream_start_time=now
      - сброс win/lose/delta и processed_ids_stream
    При выключении:
      - stream_active=False (цифры остаются)
    """
    clear_cache()
    state_key = f"{STATE_KEY_PREFIX}{DOTA_ACCOUNT_ID}"
    state = await redis_get_json(state_key) or {}
    state = migrate_state(state)

    if active:
        state["stream_active"] = True
        state["stream_start_time"] = now_unix()
        state["stream_win"] = 0
        state["stream_lose"] = 0
        state["stream_delta"] = 0
        state["processed_ids_stream"] = []
    else:
        state["stream_active"] = False

    await redis_set_json(state_key, state)


@app.get("/sync_stream", response_class=PlainTextResponse)
async def sync_stream(token: str = Query("")):
    """
    Если EventSub не пришёл, можно вручную синхронизировать:
    - проверяем Twitch API (streams) и ставим stream_active True/False.
    """
    require_admin(token)

    if not (TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET and TWITCH_BROADCASTER_LOGIN):
        return "twitch env not set"

    live = await twitch_stream_is_live(TWITCH_BROADCASTER_LOGIN)
    await set_stream_active(live)
    return f"ok: stream_active={live}"


@app.get("/eventsub_setup", response_class=PlainTextResponse)
async def eventsub_setup(request: Request, token: str = Query("")):
    """
    Создаёт подписки stream.online и stream.offline на webhook.
    """
    require_admin(token)

    if not (TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET and TWITCH_BROADCASTER_LOGIN and EVENTSUB_SECRET):
        return "missing TWITCH_CLIENT_ID/TWITCH_CLIENT_SECRET/TWITCH_BROADCASTER_LOGIN/EVENTSUB_SECRET"

    callback_base = get_callback_base(request)
    callback_url = f"{callback_base}/eventsub"

    broadcaster_id = await twitch_get_user_id_by_login(TWITCH_BROADCASTER_LOGIN)

    # stream.online
    online_body = {
        "type": "stream.online",
        "version": "1",
        "condition": {"broadcaster_user_id": broadcaster_id},
        "transport": {
            "method": "webhook",
            "callback": callback_url,
            "secret": EVENTSUB_SECRET,
        },
    }
    online_res = await twitch_api_post("/eventsub/subscriptions", online_body)

    # stream.offline
    offline_body = {
        "type": "stream.offline",
        "version": "1",
        "condition": {"broadcaster_user_id": broadcaster_id},
        "transport": {
            "method": "webhook",
            "callback": callback_url,
            "secret": EVENTSUB_SECRET,
        },
    }
    offline_res = await twitch_api_post("/eventsub/subscriptions", offline_body)

    def fmt(res):
        if isinstance(res, dict) and "_status" in res:
            return f"status={res['_status']} body={res.get('_body','')[:200]}"
        return "created"

    return (
        "ok\n"
        f"callback: {callback_url}\n"
        f"online: {fmt(online_res)}\n"
        f"offline: {fmt(offline_res)}\n"
    )


@app.post("/eventsub")
async def eventsub(request: Request):
    """
    Twitch EventSub webhook.
    """
    body = await request.body()
    eventsub_verify(request, body)
    payload = json.loads(body.decode("utf-8"))

    msg_type = request.headers.get("Twitch-Eventsub-Message-Type", "")

    # 1) challenge
    if msg_type == "webhook_callback_verification":
        challenge = payload.get("challenge", "")
        return PlainTextResponse(challenge)

    # 2) notification
    if msg_type == "notification":
        sub = payload.get("subscription", {}) or {}
        typ = sub.get("type", "")
        if typ == "stream.online":
            await set_stream_active(True)
        elif typ == "stream.offline":
            await set_stream_active(False)
        return PlainTextResponse("ok")

    # 3) revocation и т.п.
    return PlainTextResponse("ok")


# =========================
# Debug endpoints (admin)
# =========================
@app.get("/debug_last_matches", response_class=PlainTextResponse)
async def debug_last_matches(token: str = Query("")):
    require_admin(token)
    if not DOTA_ACCOUNT_ID:
        return "DOTA_ACCOUNT_ID not set"

    state_key = f"{STATE_KEY_PREFIX}{DOTA_ACCOUNT_ID}"
    state = await redis_get_json(state_key) or {}
    state = migrate_state(state)

    matches = await fetch_ranked_matches_opendota(DOTA_ACCOUNT_ID, limit=15)

    lines = [
        f"account_id={DOTA_ACCOUNT_ID}",
        f"got_matches={len(matches)}",
        "last_15:",
    ]
    for m in matches:
        lines.append(
            f"- match_id={m.get('match_id')} start_time={m.get('start_time')} lobby_type={m.get('lobby_type')}"
        )

    lines += [
        "",
        "state:",
        f"stream_active={state['stream_active']}",
        f"stream_start_time={state['stream_start_time']}",
        f"processed_ids_stream_count={len(state.get('processed_ids_stream', []))}",
        f"mmr={state['mmr']}",
    ]
    return "\n".join(lines)
