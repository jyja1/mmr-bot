import os
import json
import time
import hmac
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import FastAPI, Request, Query
from fastapi.responses import PlainTextResponse

# -------------------------
# ENV
# -------------------------
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "")
ACCOUNT_ID = os.environ.get("DOTA_ACCOUNT_ID")  # 32-bit account_id (не steam64)
STEAM_API_KEY = os.environ.get("STEAM_API_KEY", "")

START_MMR = int(os.environ.get("START_MMR", "13772"))
MMR_STEP = int(os.environ.get("MMR_STEP", "25"))
TZ_OFFSET_HOURS = int(os.environ.get("TZ_OFFSET_HOURS", "3"))

UPSTASH_URL = os.environ.get("UPSTASH_REDIS_REST_URL")
UPSTASH_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN")

# Twitch EventSub (если используешь)
TWITCH_CLIENT_ID = os.environ.get("TWITCH_CLIENT_ID", "")
TWITCH_CLIENT_SECRET = os.environ.get("TWITCH_CLIENT_SECRET", "")
TWITCH_BROADCASTER_LOGIN = os.environ.get("TWITCH_BROADCASTER_LOGIN", "")
EVENTSUB_SECRET = os.environ.get("EVENTSUB_SECRET", "")

# -------------------------
# CONST
# -------------------------
STEAM_BASE = "https://api.steampowered.com"
CACHE_TTL = 10  # чтобы быстрее проверять
STATE_KEY_PREFIX = "mmr:"

app = FastAPI()
_cache_text = None
_cache_ts = 0


# -------------------------
# Helpers
# -------------------------
def clear_cache():
    global _cache_text, _cache_ts
    _cache_text = None
    _cache_ts = 0


def tz_msk():
    return timezone(timedelta(hours=TZ_OFFSET_HOURS))


def fmt_signed(n: int) -> str:
    return f"+{n}" if n >= 0 else str(n)


def require_admin(token: str) -> Optional[str]:
    if not ADMIN_TOKEN:
        return "ADMIN_TOKEN не установлен"
    if token != ADMIN_TOKEN:
        return "Forbidden"
    return None


def ensure_state_fields(state: Dict[str, Any]) -> Dict[str, Any]:
    # Миграция старых данных: ничего не ломаем, только добавляем недостающее
    state.setdefault("start_mmr", START_MMR)
    state.setdefault("mmr", START_MMR)
    state.setdefault("last_start_time", 0)
    state.setdefault("processed_ids", [])

    # стрим-сессия (это твой Today -> Win/Lose/Total)
    state.setdefault("stream_active", False)
    state.setdefault("stream_start_time", 0)
    state.setdefault("stream_win", 0)
    state.setdefault("stream_lose", 0)
    state.setdefault("stream_delta", 0)

    return state


def default_state(baseline_start_time: int) -> Dict[str, Any]:
    return ensure_state_fields({
        "start_mmr": START_MMR,
        "mmr": START_MMR,
        "last_start_time": baseline_start_time,
        "processed_ids": [],
        "stream_active": False,
        "stream_start_time": 0,
        "stream_win": 0,
        "stream_lose": 0,
        "stream_delta": 0,
    })


# -------------------------
# Upstash Redis REST
# -------------------------
async def redis_get_json(key: str) -> Optional[Dict[str, Any]]:
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(
            f"{UPSTASH_URL}/get/{key}",
            headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
        )
        r.raise_for_status()
        data = r.json()
        val = data.get("result")
        if not val:
            return None
        try:
            return json.loads(val)
        except Exception:
            return None


async def redis_set_json(key: str, obj: Dict[str, Any]):
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(
            f"{UPSTASH_URL}/set/{key}",
            headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
            data=json.dumps(obj, ensure_ascii=False),
        )
        r.raise_for_status()


# -------------------------
# Steam API
# -------------------------
async def steam_get_match_history(account_id: str, matches_requested: int = 25) -> List[Dict[str, Any]]:
    # GetMatchHistory returns match_id, start_time, lobby_type, etc.
    url = f"{STEAM_BASE}/IDOTA2Match_570/GetMatchHistory/v1/"
    params = {
        "key": STEAM_API_KEY,
        "account_id": account_id,
        "matches_requested": matches_requested,
    }
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(url, params=params)
        r.raise_for_status()
        data = r.json()
        result = (data or {}).get("result") or {}
        matches = result.get("matches") or []
        return matches


async def steam_get_match_details(match_id: int) -> Dict[str, Any]:
    url = f"{STEAM_BASE}/IDOTA2Match_570/GetMatchDetails/v1/"
    params = {"key": STEAM_API_KEY, "match_id": match_id}
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(url, params=params)
        r.raise_for_status()
        data = r.json()
        return (data or {}).get("result") or {}


def is_win_for_player(radiant_win: bool, player_slot: int) -> bool:
    is_radiant = int(player_slot) < 128
    return bool(radiant_win) if is_radiant else (not bool(radiant_win))


def find_player_slot(details: Dict[str, Any], account_id: int) -> Optional[int]:
    players = details.get("players") or []
    for p in players:
        if int(p.get("account_id", -1)) == int(account_id):
            ps = p.get("player_slot")
            if ps is None:
                return None
            return int(ps)
    return None


async def fetch_latest_ranked_start_time(account_id: str) -> int:
    # чтобы при первом запуске не считать прошлые игры
    matches = await steam_get_match_history(account_id, matches_requested=10)
    ranked = [m for m in matches if int(m.get("lobby_type", -1)) == 7 and m.get("start_time")]
    if not ranked:
        return 0
    return int(ranked[0]["start_time"])


# -------------------------
# Health
# -------------------------
@app.get("/health", response_class=PlainTextResponse)
async def health():
    return "ok"


# -------------------------
# Debug endpoint (важно для “почему не считает”)
# -------------------------
@app.get("/debug_last_matches", response_class=PlainTextResponse)
async def debug_last_matches(token: str = Query(default="")):
    err = require_admin(token)
    if err:
        return err

    if not ACCOUNT_ID:
        return "DOTA_ACCOUNT_ID не установлен"
    if not STEAM_API_KEY:
        return "STEAM_API_KEY не установлен"

    matches = await steam_get_match_history(ACCOUNT_ID, matches_requested=15)

    lines = []
    lines.append(f"account_id={ACCOUNT_ID}")
    lines.append(f"got_matches={len(matches)}")
    lines.append("last_15:")
    for m in matches[:15]:
        mid = m.get("match_id")
        st = m.get("start_time")
        lt = m.get("lobby_type")
        lines.append(f"- match_id={mid} start_time={st} lobby_type={lt}")

    # покажем ещё state
    if UPSTASH_URL and UPSTASH_TOKEN:
        state = await redis_get_json(f"{STATE_KEY_PREFIX}{ACCOUNT_ID}")
        if state:
            state = ensure_state_fields(state)
            lines.append("")
            lines.append("state:")
            lines.append(f"last_start_time={state.get('last_start_time')}")
            lines.append(f"processed_ids_count={len(state.get('processed_ids', []))}")
            lines.append(f"stream_active={state.get('stream_active')}")
            lines.append(f"stream_start_time={state.get('stream_start_time')}")
            lines.append(f"mmr={state.get('mmr')}")
        else:
            lines.append("")
            lines.append("state: NONE")
    else:
        lines.append("")
        lines.append("redis: not configured")

    return "\n".join(lines)


# -------------------------
# Core: /mmr
# -------------------------
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

    state_key = f"{STATE_KEY_PREFIX}{ACCOUNT_ID}"
    state = await redis_get_json(state_key)

    if not state:
        baseline = await fetch_latest_ranked_start_time(ACCOUNT_ID)
        state = default_state(baseline)
        await redis_set_json(state_key, state)

    state = ensure_state_fields(state)

    # 1) Берём историю матчей (Steam)
    matches = await steam_get_match_history(ACCOUNT_ID, matches_requested=30)

    # 2) Фильтруем ranked-only (lobby_type == 7)
    ranked = []
    for m in matches:
        try:
            if int(m.get("lobby_type", -1)) != 7:
                continue
            if not m.get("match_id") or not m.get("start_time"):
                continue
            ranked.append(m)
        except Exception:
            continue

    processed = set(int(x) for x in (state.get("processed_ids") or []))
    last_time = int(state.get("last_start_time", 0))

    new_matches = []
    for m in ranked:
        mid = int(m["match_id"])
        st = int(m["start_time"])
        if mid in processed:
            continue
        # важно: строго > last_time
        if st > last_time:
            new_matches.append((st, mid))

    new_matches.sort(key=lambda x: x[0])  # по времени

    # 3) По каждому новому матчу тянем details и считаем win/lose
    for st, mid in new_matches:
        details = await steam_get_match_details(mid)

        # В match details lobby_type тоже есть — но доверяем history фильтру
        radiant_win = details.get("radiant_win")
        if radiant_win is None:
            continue

        ps = find_player_slot(details, int(ACCOUNT_ID))
        if ps is None:
            continue

        won = is_win_for_player(bool(radiant_win), int(ps))
        delta = MMR_STEP if won else -MMR_STEP

        # общий MMR (всегда)
        state["mmr"] = int(state.get("mmr", START_MMR)) + delta

        # стримовый счётчик — только если стрим активен,
        # и матч начался после stream_start_time
        if state.get("stream_active") and int(st) >= int(state.get("stream_start_time", 0)):
            if won:
                state["stream_win"] = int(state.get("stream_win", 0)) + 1
            else:
                state["stream_lose"] = int(state.get("stream_lose", 0)) + 1
            state["stream_delta"] = int(state.get("stream_delta", 0)) + delta

        processed.add(mid)
        state["last_start_time"] = max(int(state.get("last_start_time", 0)), int(st))

    # обрезаем processed_ids чтобы не раздувалось
    state["processed_ids"] = list(processed)[-400:]
    await redis_set_json(state_key, state)

    cur = int(state.get("mmr", START_MMR))
    sw = int(state.get("stream_win", 0))
    sl = int(state.get("stream_lose", 0))
    sd = int(state.get("stream_delta", 0))

    text = f"MMR: {cur} • Today -> Win: {sw} Lose: {sl} • Total: {fmt_signed(sd)}"
    _cache_text, _cache_ts = text, now
    return text


# -------------------------
# Admin: force stream start/stop + backfill
# -------------------------
@app.get("/force_stream_start", response_class=PlainTextResponse)
async def force_stream_start(token: str = Query(default=""), minutes: int = Query(default=360)):
    err = require_admin(token)
    if err:
        return err

    if not ACCOUNT_ID:
        return "DOTA_ACCOUNT_ID не установлен"
    if not UPSTASH_URL or not UPSTASH_TOKEN:
        return "Redis не настроен"

    state_key = f"{STATE_KEY_PREFIX}{ACCOUNT_ID}"
    state = await redis_get_json(state_key)
    if not state:
        state = default_state(0)

    state = ensure_state_fields(state)

    now_ts = int(time.time())
    baseline = now_ts - int(minutes) * 60

    state["stream_active"] = True
    state["stream_start_time"] = baseline
    state["stream_win"] = 0
    state["stream_lose"] = 0
    state["stream_delta"] = 0

    await redis_set_json(state_key, state)
    clear_cache()
    return f"OK stream forced. start_time={baseline}"


@app.get("/force_stream_stop", response_class=PlainTextResponse)
async def force_stream_stop(token: str = Query(default="")):
    err = require_admin(token)
    if err:
        return err

    state_key = f"{STATE_KEY_PREFIX}{ACCOUNT_ID}"
    state = await redis_get_json(state_key)
    if not state:
        return "State not initialized"

    state = ensure_state_fields(state)
    state["stream_active"] = False
    await redis_set_json(state_key, state)
    clear_cache()
    return "OK stream stopped"


@app.get("/backfill", response_class=PlainTextResponse)
async def backfill(token: str = Query(default=""), minutes: int = Query(default=720)):
    err = require_admin(token)
    if err:
        return err

    state_key = f"{STATE_KEY_PREFIX}{ACCOUNT_ID}"
    state = await redis_get_json(state_key)
    if not state:
        return "State not initialized"

    state = ensure_state_fields(state)

    now_ts = int(time.time())
    baseline = now_ts - int(minutes) * 60

    # Сдвигаем last_start_time назад и чистим processed_ids, чтобы точно перечитать окно
    state["last_start_time"] = min(int(state.get("last_start_time", baseline)), baseline)
    state["processed_ids"] = []

    await redis_set_json(state_key, state)
    clear_cache()
    return f"OK backfill armed from {baseline}. Call /mmr now."


@app.get("/streamstatus", response_class=PlainTextResponse)
async def streamstatus(token: str = Query(default="")):
    err = require_admin(token)
    if err:
        return err

    state_key = f"{STATE_KEY_PREFIX}{ACCOUNT_ID}"
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


# -------------------------
# Twitch EventSub webhook (минимум — чтобы ставить stream_active)
# -------------------------
def _verify_eventsub_signature(req: Request, body: bytes) -> bool:
    # Twitch EventSub signature:
    # message = id + timestamp + body
    # signature = "sha256=" + HMAC_SHA256(secret, message)
    if not EVENTSUB_SECRET:
        return False

    msg_id = req.headers.get("Twitch-Eventsub-Message-Id", "")
    msg_ts = req.headers.get("Twitch-Eventsub-Message-Timestamp", "")
    sig = req.headers.get("Twitch-Eventsub-Message-Signature", "")

    if not msg_id or not msg_ts or not sig:
        return False

    message = (msg_id + msg_ts).encode("utf-8") + body
    digest = hmac.new(EVENTSUB_SECRET.encode("utf-8"), message, hashlib.sha256).hexdigest()
    expected = "sha256=" + digest
    return hmac.compare_digest(expected, sig)


@app.post("/eventsub", response_class=PlainTextResponse)
async def eventsub(request: Request):
    body = await request.body()

    # 1) verify signature (обязательно)
    if not _verify_eventsub_signature(request, body):
        return "invalid signature"

    data = json.loads(body.decode("utf-8"))

    msg_type = request.headers.get("Twitch-Eventsub-Message-Type", "")

    # 2) challenge
    if msg_type == "webhook_callback_verification":
        challenge = data.get("challenge", "")
        return challenge

    # 3) notification
    if msg_type == "notification":
        sub = data.get("subscription", {}) or {}
        event = data.get("event", {}) or {}
        sub_type = sub.get("type", "")

        if not UPSTASH_URL or not UPSTASH_TOKEN:
            return "redis not configured"

        state_key = f"{STATE_KEY_PREFIX}{ACCOUNT_ID}"
        state = await redis_get_json(state_key) or default_state(0)
        state = ensure_state_fields(state)

        now_ts = int(time.time())

        if sub_type == "stream.online":
            # начинаем новую стрим-сессию
            state["stream_active"] = True
            state["stream_start_time"] = now_ts
            state["stream_win"] = 0
            state["stream_lose"] = 0
            state["stream_delta"] = 0
            await redis_set_json(state_key, state)
            clear_cache()
            return "ok online"

        if sub_type == "stream.offline":
            state["stream_active"] = False
            await redis_set_json(state_key, state)
            clear_cache()
            return "ok offline"

        return "ok other"

    return "ok"


# -------------------------
# NOTE:
# eventsub_setup у тебя уже был реализован отдельно.
# Если он нужен — оставляем как есть у тебя в репо.
# -------------------------
