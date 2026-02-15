import os
import json
import time
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, Any

import httpx
from fastapi import FastAPI, Query
from fastapi.responses import PlainTextResponse

# -------------------- ENV --------------------
ACCOUNT_ID = int(os.environ.get("DOTA_ACCOUNT_ID", "0"))  # steam32
START_MMR = int(os.environ.get("START_MMR", "13772"))
MMR_STEP = int(os.environ.get("MMR_STEP", "25"))
TZ_OFFSET_HOURS = int(os.environ.get("TZ_OFFSET_HOURS", "3"))

UPSTASH_URL = os.environ.get("UPSTASH_REDIS_REST_URL")
UPSTASH_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN")

STEAM_API_KEY = os.environ.get("STEAM_API_KEY")
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN")

# На сколько часов назад "догонять" матчи при первом запуске и при reset,
# чтобы не было ситуации "сыграл пару игр, а бот их пропустил".
BASELINE_LOOKBACK_HOURS = int(os.environ.get("BASELINE_LOOKBACK_HOURS", "24"))

CACHE_TTL = 15  # секунд кэша ответа /mmr (чтобы не долбить API)
STEAM_BASE = "https://api.steampowered.com"

app = FastAPI()

_cache_text: Optional[str] = None
_cache_ts: float = 0.0


# -------------------- time helpers --------------------
def tz_msk():
    return timezone(timedelta(hours=TZ_OFFSET_HOURS))


def today_key() -> str:
    return datetime.now(tz_msk()).strftime("%Y-%m-%d")


def day_key_from_unix(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=tz_msk()).strftime("%Y-%m-%d")


def fmt_signed(n: int) -> str:
    return f"+{n}" if n >= 0 else str(n)


def baseline_start_time_now_minus_lookback() -> int:
    return int(time.time()) - BASELINE_LOOKBACK_HOURS * 3600


# -------------------- auth helpers --------------------
def require_admin(token: Optional[str]) -> Optional[str]:
    if not ADMIN_TOKEN:
        return "Admin token not set"
    if token != ADMIN_TOKEN:
        return "Forbidden"
    return None


def clear_cache():
    global _cache_text, _cache_ts
    _cache_text = None
    _cache_ts = 0.0


# -------------------- Upstash helpers --------------------
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
        return json.loads(val)


async def redis_set_json(key: str, obj: dict) -> None:
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(
            f"{UPSTASH_URL}/set/{key}",
            headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
            data=json.dumps(obj, ensure_ascii=False),
        )
        r.raise_for_status()


# -------------------- Steam API helpers --------------------
async def steam_get_match_history(account_id: int, matches_requested: int = 20) -> dict:
    url = f"{STEAM_BASE}/IDOTA2Match_570/GetMatchHistory/V001/"
    params = {
        "key": STEAM_API_KEY,
        "account_id": account_id,
        "matches_requested": matches_requested,
    }
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(url, params=params)
        r.raise_for_status()
        return r.json()


async def steam_get_match_details(match_id: int) -> dict:
    url = f"{STEAM_BASE}/IDOTA2Match_570/GetMatchDetails/V001/"
    params = {"key": STEAM_API_KEY, "match_id": match_id}
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(url, params=params)
        r.raise_for_status()
        return r.json()


def extract_win_and_start_time_from_details(details: dict, account_id: int) -> Tuple[Optional[bool], int, Optional[int]]:
    """
    Returns: (won, start_time, lobby_type)
    won=None if player not found.
    """
    res = details.get("result") or {}
    start_time = int(res.get("start_time", 0) or 0)
    radiant_win = res.get("radiant_win")
    lobby_type = res.get("lobby_type")
    players = res.get("players") or []

    me = None
    for p in players:
        if int(p.get("account_id", -1)) == int(account_id):
            me = p
            break
    if me is None:
        return None, start_time, int(lobby_type) if lobby_type is not None else None

    player_slot = int(me.get("player_slot", 0))
    is_radiant = player_slot < 128
    won = bool(radiant_win) if is_radiant else (not bool(radiant_win))
    return won, start_time, int(lobby_type) if lobby_type is not None else None


# -------------------- state --------------------
def default_state(baseline_start_time: int) -> dict:
    return {
        "start_mmr": START_MMR,
        "mmr": START_MMR,
        "last_start_time": baseline_start_time,
        "processed_ids": [],  # последние обработанные match_id
        "today_date": today_key(),
        "today_win": 0,
        "today_lose": 0,
        "today_delta": 0,
    }


def soft_reset_state_keep_mmr(old_state: dict, baseline_start_time: int) -> dict:
    current_mmr = int(old_state.get("mmr", START_MMR))
    return {
        "start_mmr": current_mmr,     # фиксируем текущий ммр как новую “базу”
        "mmr": current_mmr,
        "last_start_time": baseline_start_time,
        "processed_ids": [],
        "today_date": today_key(),
        "today_win": 0,
        "today_lose": 0,
        "today_delta": 0,
    }


# -------------------- endpoints --------------------
@app.get("/health", response_class=PlainTextResponse)
async def health():
    return "ok"


@app.get("/mmr", response_class=PlainTextResponse)
async def mmr():
    global _cache_text, _cache_ts

    now = time.time()
    if _cache_text and (now - _cache_ts) < CACHE_TTL:
        return _cache_text

    # env checks
    if not ACCOUNT_ID:
        return "DOTA_ACCOUNT_ID не установлен"
    if not UPSTASH_URL or not UPSTASH_TOKEN:
        return "Redis не настроен"
    if not STEAM_API_KEY:
        return "STEAM_API_KEY не установлен (steamcommunity.com/dev/apikey)"

    state_key = f"mmr:{ACCOUNT_ID}"
    state = await redis_get_json(state_key)

    # init state: baseline = now - lookback (чтобы не пропускать недавние игры)
    if not state:
        baseline = baseline_start_time_now_minus_lookback()
        state = default_state(baseline)
        await redis_set_json(state_key, state)

    # daily reset by MSK day
    if state.get("today_date") != today_key():
        state["today_date"] = today_key()
        state["today_win"] = 0
        state["today_lose"] = 0
        state["today_delta"] = 0

    processed = set(int(x) for x in (state.get("processed_ids") or []))
    last_time = int(state.get("last_start_time", 0))

    # get history
    hist = await steam_get_match_history(ACCOUNT_ID, matches_requested=20)
    result = hist.get("result") or {}
    matches = result.get("matches") or []

    if not matches:
        # чаще всего это приватность Game details / match history
        text = "Матчи не получены. Проверь: Steam -> Privacy -> Game details (Доступ к игровой информации) = Public."
        _cache_text, _cache_ts = text, now
        return text

    # choose new by start_time
    candidates = []
    for m in matches:
        mid = int(m.get("match_id", 0) or 0)
        st = int(m.get("start_time", 0) or 0)
        if not mid or not st:
            continue
        if st > last_time and mid not in processed:
            candidates.append((st, mid))

    candidates.sort()  # oldest -> newest

    # process
    for st, mid in candidates:
        det = await steam_get_match_details(mid)
        won, start_time, lobby_type = extract_win_and_start_time_from_details(det, ACCOUNT_ID)

        # only ranked (lobby_type = 7)
        if lobby_type != 7:
            processed.add(mid)
            state["last_start_time"] = max(int(state.get("last_start_time", 0)), st)
            continue

        if won is None:
            processed.add(mid)
            state["last_start_time"] = max(int(state.get("last_start_time", 0)), st)
            continue

        delta = MMR_STEP if won else -MMR_STEP
        state["mmr"] = int(state.get("mmr", START_MMR)) + delta

        if day_key_from_unix(start_time) == today_key():
            if won:
                state["today_win"] = int(state.get("today_win", 0)) + 1
            else:
                state["today_lose"] = int(state.get("today_lose", 0)) + 1
            state["today_delta"] = int(state.get("today_delta", 0)) + delta

        processed.add(mid)
        state["last_start_time"] = max(int(state.get("last_start_time", 0)), st)

    state["processed_ids"] = list(processed)[-300:]
    await redis_set_json(state_key, state)

    cur = int(state.get("mmr", START_MMR))
    tw = int(state.get("today_win", 0))
    tl = int(state.get("today_lose", 0))
    td = int(state.get("today_delta", 0))

    text = f"MMR: {cur} • Today -> Win: {tw} Lose: {tl} • Total: {fmt_signed(td)}"
    _cache_text, _cache_ts = text, now
    return text


# -------------------- admin endpoints --------------------
@app.get("/cleartoday", response_class=PlainTextResponse)
async def cleartoday(token: Optional[str] = Query(default=None)):
    err = require_admin(token)
    if err:
        return err

    state_key = f"mmr:{ACCOUNT_ID}"
    state = await redis_get_json(state_key)
    if not state:
        return "State not initialized"

    state["today_date"] = today_key()
    state["today_win"] = 0
    state["today_lose"] = 0
    state["today_delta"] = 0

    await redis_set_json(state_key, state)
    clear_cache()
    return "Today cleared"


# soft reset: сохраняет текущий ммр (например 14000), но обнуляет today и начинает считать “с нуля” по новым матчам
@app.get("/reset", response_class=PlainTextResponse)
async def reset(token: Optional[str] = Query(default=None)):
    err = require_admin(token)
    if err:
        return err

    state_key = f"mmr:{ACCOUNT_ID}"
    state = await redis_get_json(state_key)
    if not state:
        return "State not initialized"

    baseline = baseline_start_time_now_minus_lookback()
    new_state = soft_reset_state_keep_mmr(state, baseline)

    await redis_set_json(state_key, new_state)
    clear_cache()
    return f"Soft reset done. MMR locked at {int(new_state['mmr'])}"


# hard reset: откат на START_MMR (13772)
@app.get("/hardreset", response_class=PlainTextResponse)
async def hardreset(token: Optional[str] = Query(default=None)):
    err = require_admin(token)
    if err:
        return err

    state_key = f"mmr:{ACCOUNT_ID}"
    baseline = baseline_start_time_now_minus_lookback()
    state = default_state(baseline)

    await redis_set_json(state_key, state)
    clear_cache()
    return f"Hard reset done. MMR set to {START_MMR}"


@app.get("/testwin", response_class=PlainTextResponse)
async def testwin(token: Optional[str] = Query(default=None)):
    err = require_admin(token)
    if err:
        return err

    state_key = f"mmr:{ACCOUNT_ID}"
    state = await redis_get_json(state_key)
    if not state:
        baseline = baseline_start_time_now_minus_lookback()
        state = default_state(baseline)

    if state.get("today_date") != today_key():
        state["today_date"] = today_key()
        state["today_win"] = 0
        state["today_lose"] = 0
        state["today_delta"] = 0

    state["mmr"] = int(state.get("mmr", START_MMR)) + MMR_STEP
    state["today_win"] = int(state.get("today_win", 0)) + 1
    state["today_delta"] = int(state.get("today_delta", 0)) + MMR_STEP

    await redis_set_json(state_key, state)
    clear_cache()
    return "WIN added"


@app.get("/testlose", response_class=PlainTextResponse)
async def testlose(token: Optional[str] = Query(default=None)):
    err = require_admin(token)
    if err:
        return err

    state_key = f"mmr:{ACCOUNT_ID}"
    state = await redis_get_json(state_key)
    if not state:
        baseline = baseline_start_time_now_minus_lookback()
        state = default_state(baseline)

    if state.get("today_date") != today_key():
        state["today_date"] = today_key()
        state["today_win"] = 0
        state["today_lose"] = 0
        state["today_delta"] = 0

    state["mmr"] = int(state.get("mmr", START_MMR)) - MMR_STEP
    state["today_lose"] = int(state.get("today_lose", 0)) + 1
    state["today_delta"] = int(state.get("today_delta", 0)) - MMR_STEP

    await redis_set_json(state_key, state)
    clear_cache()
    return "LOSE added"
