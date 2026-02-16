import os
import json
import time
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta, timezone

import httpx
from fastapi import FastAPI, Query, Request
from fastapi.responses import PlainTextResponse

# =========================
# ENV
# =========================
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "")

ACCOUNT_ID = os.environ.get("DOTA_ACCOUNT_ID")  # 32-bit steam account id (например 1258333388)
STEAM_API_KEY = os.environ.get("STEAM_API_KEY")

START_MMR = int(os.environ.get("START_MMR", "13772"))
MMR_STEP = int(os.environ.get("MMR_STEP", "25"))
TZ_OFFSET_HOURS = int(os.environ.get("TZ_OFFSET_HOURS", "3"))

UPSTASH_URL = os.environ.get("UPSTASH_REDIS_REST_URL")
UPSTASH_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN")

# Twitch EventSub (если используешь)
TWITCH_BROADCASTER_LOGIN = os.environ.get("TWITCH_BROADCASTER_LOGIN", "")
TWITCH_CLIENT_ID = os.environ.get("TWITCH_CLIENT_ID", "")
TWITCH_CLIENT_SECRET = os.environ.get("TWITCH_CLIENT_SECRET", "")
EVENTSUB_SECRET = os.environ.get("EVENTSUB_SECRET", "")

CACHE_TTL = 10

STEAM_MATCH_HISTORY = "https://api.steampowered.com/IDOTA2Match_570/GetMatchHistory/V001/"
STEAM_MATCH_DETAILS = "https://api.steampowered.com/IDOTA2Match_570/GetMatchDetails/V001/"
TWITCH_OAUTH_TOKEN = "https://id.twitch.tv/oauth2/token"
TWITCH_API_BASE = "https://api.twitch.tv/helix"
TWITCH_EVENTSUB = f"{TWITCH_API_BASE}/eventsub/subscriptions"

app = FastAPI()
_cache_text = None
_cache_ts = 0


# =========================
# Helpers
# =========================
def tz_msk():
    return timezone(timedelta(hours=TZ_OFFSET_HOURS))


def fmt_signed(n: int) -> str:
    return f"+{n}" if n >= 0 else str(n)


def now_unix() -> int:
    return int(time.time())


def admin_ok(token: str) -> bool:
    return bool(ADMIN_TOKEN) and token == ADMIN_TOKEN


def state_key() -> str:
    # привязываем состояние строго к аккаунту
    return f"mmr:{ACCOUNT_ID}"


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


async def redis_set_json(key: str, obj: dict):
    async with httpx.AsyncClient(timeout=15) as client:
        # Upstash принимает raw value
        r = await client.post(
            f"{UPSTASH_URL}/set/{key}",
            headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
            data=json.dumps(obj, ensure_ascii=False),
        )
        r.raise_for_status()


def default_state() -> dict:
    return {
        "start_mmr": START_MMR,
        "mmr": START_MMR,

        # Stream session
        "stream_active": False,
        "stream_start_time": 0,          # unix seconds
        "stream_win": 0,
        "stream_lose": 0,
        "stream_delta": 0,
        "processed_ids_stream": [],      # match ids already counted in stream

        # debug / errors
        "pending_ids": [],               # ids ждём (если матч уже в history, но details не готовы)
        "last_errors": [],               # last 30 errors strings

        "updated_at": now_unix(),
    }


async def get_state() -> dict:
    st = await redis_get_json(state_key())
    if not st:
        st = default_state()
        await redis_set_json(state_key(), st)
    return st


async def save_state(st: dict):
    st["updated_at"] = now_unix()
    await redis_set_json(state_key(), st)


def push_error(st: dict, msg: str):
    arr = st.get("last_errors", [])
    arr.append(f"{now_unix()}: {msg}")
    st["last_errors"] = arr[-30:]


def is_win_for_player(radiant_win: bool, player_slot: int) -> bool:
    # player_slot < 128 => radiant
    is_radiant = int(player_slot) < 128
    return bool(radiant_win) if is_radiant else (not bool(radiant_win))


# =========================
# Steam API
# =========================
async def steam_get_match_history(limit: int = 25) -> List[dict]:
    params = {
        "key": STEAM_API_KEY,
        "account_id": int(ACCOUNT_ID),
        "matches_requested": int(limit),
        "lobby_type": 7,  # ranked matchmaking
    }
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(STEAM_MATCH_HISTORY, params=params)
        r.raise_for_status()
        data = r.json()
    matches = (((data or {}).get("result") or {}).get("matches")) or []
    return matches


async def steam_get_match_details(match_id: int) -> Optional[dict]:
    params = {
        "key": STEAM_API_KEY,
        "match_id": int(match_id),
    }
    async with httpx.AsyncClient(timeout=25) as client:
        r = await client.get(STEAM_MATCH_DETAILS, params=params)
        # бывает: 503/500 пока матч “не прогрузился”
        if r.status_code != 200:
            return {"_http_status": r.status_code, "_body": (r.text or "")[:500]}
        data = r.json()
    res = (data or {}).get("result")
    return res


def find_player_in_details(details: dict) -> Optional[dict]:
    players = details.get("players") or []
    aid = int(ACCOUNT_ID)
    for p in players:
        if int(p.get("account_id") or 0) == aid:
            return p
    return None


# =========================
# Core logic: update stream stats
# =========================
async def update_from_stream_matches(st: dict):
    # если стрим не активен — ничего не считаем
    if not st.get("stream_active"):
        return

    stream_start = int(st.get("stream_start_time", 0))
    if stream_start <= 0:
        return

    # берём последние матчи ranked и фильтруем по start_time >= stream_start
    matches = await steam_get_match_history(limit=25)

    # кандидаты — матчи после старта стрима
    candidates = []
    for m in matches:
        mid = int(m.get("match_id") or 0)
        stime = int(m.get("start_time") or 0)
        lobby_type = int(m.get("lobby_type") or 0)
        if mid and stime and lobby_type == 7 and stime >= stream_start:
            candidates.append({"match_id": mid, "start_time": stime})

    candidates.sort(key=lambda x: x["start_time"])

    processed = set(st.get("processed_ids_stream", []))
    pending = set(st.get("pending_ids", []))

    for m in candidates:
        mid = int(m["match_id"])
        if mid in processed:
            continue

        details = await steam_get_match_details(mid)

        # если details не готовы / ошибка — в pending, попробуем позже
        if not details or details.get("_http_status"):
            status = (details or {}).get("_http_status")
            push_error(st, f"steam details error match_id={mid} status={status}")
            pending.add(mid)
            continue

        # иногда Steam возвращает result без нужных полей — тоже в pending
        radiant_win = details.get("radiant_win")
        if radiant_win is None:
            push_error(st, f"steam details missing radiant_win match_id={mid}")
            pending.add(mid)
            continue

        player = find_player_in_details(details)
        if not player:
            # если вдруг матч не содержит аккаунта — пропускаем
            push_error(st, f"player not found in details match_id={mid}")
            processed.add(mid)
            continue

        player_slot = player.get("player_slot")
        if player_slot is None:
            push_error(st, f"missing player_slot match_id={mid}")
            pending.add(mid)
            continue

        won = is_win_for_player(bool(radiant_win), int(player_slot))
        delta = MMR_STEP if won else -MMR_STEP

        # общий mmr всегда копится
        st["mmr"] = int(st.get("mmr", START_MMR)) + delta

        # стримовые счетчики
        if won:
            st["stream_win"] = int(st.get("stream_win", 0)) + 1
        else:
            st["stream_lose"] = int(st.get("stream_lose", 0)) + 1
        st["stream_delta"] = int(st.get("stream_delta", 0)) + delta

        processed.add(mid)
        if mid in pending:
            pending.remove(mid)

    # сохраняем
    st["processed_ids_stream"] = list(processed)[-500:]
    st["pending_ids"] = list(pending)[-200:]


# =========================
# Endpoints
# =========================
@app.get("/health", response_class=PlainTextResponse)
async def health():
    return "ok"


@app.head("/health")
async def health_head():
    # UptimeRobot free tier может слать HEAD
    return PlainTextResponse("ok")


@app.get("/mmr", response_class=PlainTextResponse)
async def mmr():
    global _cache_text, _cache_ts

    now = time.time()
    if _cache_text and (now - _cache_ts) < CACHE_TTL:
        return _cache_text

    # базовые проверки
    if not ACCOUNT_ID:
        return "DOTA_ACCOUNT_ID не установлен"
    if not STEAM_API_KEY:
        return "STEAM_API_KEY не установлен"
    if not UPSTASH_URL or not UPSTASH_TOKEN:
        return "Redis не настроен"

    st = await get_state()

    # обновляем по матчам стрима
    try:
        await update_from_stream_matches(st)
    except Exception as e:
        push_error(st, f"update exception: {type(e).__name__}: {e}")

    await save_state(st)

    cur = int(st.get("mmr", START_MMR))
    sw = int(st.get("stream_win", 0))
    sl = int(st.get("stream_lose", 0))
    sd = int(st.get("stream_delta", 0))

    text = f"MMR: {cur} • Today -> Win: {sw} Lose: {sl} • Total: {fmt_signed(sd)}"
    _cache_text, _cache_ts = text, now
    return text


@app.get("/streamstatus", response_class=PlainTextResponse)
async def streamstatus(token: str = Query("")):
    if not admin_ok(token):
        return "Forbidden"

    st = await get_state()
    lines = [
        f"account_id={ACCOUNT_ID}",
        f"stream_active={st.get('stream_active')}",
        f"stream_start_time={st.get('stream_start_time')}",
        f"stream_win={st.get('stream_win')}",
        f"stream_lose={st.get('stream_lose')}",
        f"stream_delta={st.get('stream_delta')}",
        f"mmr={st.get('mmr')}",
        f"processed_ids_stream_count={len(st.get('processed_ids_stream', []))}",
        f"pending_count={len(st.get('pending_ids', []))}",
        "",
        "last_errors:",
    ]
    for e in st.get("last_errors", []):
        lines.append(f"- {e}")
    return "\n".join(lines)


@app.get("/set_stream_start", response_class=PlainTextResponse)
async def set_stream_start(token: str = Query(""), ts: int = Query(0)):
    if not admin_ok(token):
        return "Forbidden"
    if ts <= 0:
        return "ts must be unix seconds"

    st = await get_state()
    st["stream_active"] = True
    st["stream_start_time"] = int(ts)
    st["stream_win"] = 0
    st["stream_lose"] = 0
    st["stream_delta"] = 0
    st["processed_ids_stream"] = []
    st["pending_ids"] = []
    st["last_errors"] = []
    await save_state(st)

    return f"ok\nstream_start_time={ts}"


@app.get("/stream_on", response_class=PlainTextResponse)
async def stream_on(token: str = Query(""), hours_ago: int = Query(0)):
    """
    Включить стрим вручную.
    Если hours_ago>0 — выставит stream_start_time = now - hours_ago*3600
    """
    if not admin_ok(token):
        return "Forbidden"

    st = await get_state()
    st["stream_active"] = True

    if hours_ago > 0:
        st["stream_start_time"] = now_unix() - int(hours_ago) * 3600
    elif int(st.get("stream_start_time", 0)) <= 0:
        st["stream_start_time"] = now_unix()

    st["stream_win"] = 0
    st["stream_lose"] = 0
    st["stream_delta"] = 0
    st["processed_ids_stream"] = []
    st["pending_ids"] = []
    st["last_errors"] = []
    await save_state(st)

    return f"ok\nstream_active=True\nstream_start_time={st['stream_start_time']}"


@app.get("/stream_off", response_class=PlainTextResponse)
async def stream_off(token: str = Query("")):
    if not admin_ok(token):
        return "Forbidden"

    st = await get_state()
    st["stream_active"] = False
    await save_state(st)
    return "ok\nstream_active=False"


@app.get("/testwin", response_class=PlainTextResponse)
async def testwin(token: str = Query("")):
    if not admin_ok(token):
        return "Forbidden"
    st = await get_state()
    st["mmr"] = int(st.get("mmr", START_MMR)) + MMR_STEP
    st["stream_win"] = int(st.get("stream_win", 0)) + 1
    st["stream_delta"] = int(st.get("stream_delta", 0)) + MMR_STEP
    await save_state(st)
    return "WIN added"


@app.get("/testlose", response_class=PlainTextResponse)
async def testlose(token: str = Query("")):
    if not admin_ok(token):
        return "Forbidden"
    st = await get_state()
    st["mmr"] = int(st.get("mmr", START_MMR)) - MMR_STEP
    st["stream_lose"] = int(st.get("stream_lose", 0)) + 1
    st["stream_delta"] = int(st.get("stream_delta", 0)) - MMR_STEP
    await save_state(st)
    return "LOSE added"


@app.get("/reset_stream", response_class=PlainTextResponse)
async def reset_stream(token: str = Query("")):
    if not admin_ok(token):
        return "Forbidden"
    st = await get_state()
    st["stream_win"] = 0
    st["stream_lose"] = 0
    st["stream_delta"] = 0
    st["processed_ids_stream"] = []
    st["pending_ids"] = []
    st["last_errors"] = []
    await save_state(st)
    return "ok\nstream counters reset"


@app.get("/debug_last_matches", response_class=PlainTextResponse)
async def debug_last_matches(token: str = Query("")):
    if not admin_ok(token):
        return "Forbidden"

    matches = await steam_get_match_history(limit=15)
    lines = [
        f"account_id={ACCOUNT_ID}",
        f"got_matches={len(matches)}",
        "last_15:",
    ]
    for m in matches:
        mid = m.get("match_id")
        stime = m.get("start_time")
        lobby_type = m.get("lobby_type")
        lines.append(f"- match_id={mid} start_time={stime} lobby_type={lobby_type}")

    st = await get_state()
    lines += [
        "",
        "state:",
        f"stream_active={st.get('stream_active')}",
        f"stream_start_time={st.get('stream_start_time')}",
        f"processed_ids_stream_count={len(st.get('processed_ids_stream', []))}",
        f"pending_count={len(st.get('pending_ids', []))}",
        f"mmr={st.get('mmr')}",
    ]
    return "\n".join(lines)


# =========================
# EventSub заглушки (если у тебя уже настроено — оставляем)
# =========================
@app.post("/eventsub")
async def eventsub(request: Request):
    """
    Тут обычно принимают Twitch EventSub.
    Сейчас оставляем как "не ломайся": просто 200.
    Если у тебя уже есть рабочая логика — можешь вставить проверку подписи и обработку online/offline.
    """
    body = await request.body()
    # ничего не делаем — чтобы не крашило
    return PlainTextResponse("ok")


@app.get("/eventsub_setup", response_class=PlainTextResponse)
async def eventsub_setup(token: str = Query("")):
    """
    Оставляем как заглушку, чтобы не падало.
    Реальная настройка EventSub требует OAuth app token и создания subscription.
    """
    if not admin_ok(token):
        return "Forbidden"

    # Просто подтверждаем callback, чтобы ты видел что эндпоинт живой.
    return "ok\ncallback: https://mmr-bot.onrender.com/eventsub\nnote: setup via api is not implemented in this file"
