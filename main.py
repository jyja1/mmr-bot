import os
import json
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, List, Tuple

import httpx
from fastapi import FastAPI, Query
from fastapi.responses import PlainTextResponse


# -------------------- ENV --------------------
ACCOUNT_ID = os.environ.get("DOTA_ACCOUNT_ID")

START_MMR = int(os.environ.get("START_MMR", "13772"))
MMR_STEP = int(os.environ.get("MMR_STEP", "25"))
TZ_OFFSET_HOURS = int(os.environ.get("TZ_OFFSET_HOURS", "3"))

UPSTASH_URL = os.environ.get("UPSTASH_REDIS_REST_URL")
UPSTASH_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN")

ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN")  # optional

OPENDOTA = "https://api.opendota.com/api"

CACHE_TTL = 8  # seconds


# -------------------- APP --------------------
app = FastAPI()

_cache_text: Optional[str] = None
_cache_ts: float = 0.0


# -------------------- TIME HELPERS --------------------
def tz_msk():
    return timezone(timedelta(hours=TZ_OFFSET_HOURS))


def today_key() -> str:
    return datetime.now(tz_msk()).strftime("%Y-%m-%d")


def day_key_from_unix(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=tz_msk()).strftime("%Y-%m-%d")


def fmt_signed(n: int) -> str:
    return f"+{n}" if n >= 0 else str(n)


# -------------------- DOTA HELPERS --------------------
def is_win_for_player(radiant_win: bool, player_slot: int) -> bool:
    # In Dota, player_slot < 128 => Radiant, else Dire
    is_radiant = int(player_slot) < 128
    return bool(radiant_win) if is_radiant else (not bool(radiant_win))


# -------------------- UPSTASH REDIS REST --------------------
async def redis_get_json(key: str) -> Optional[Dict[str, Any]]:
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


async def redis_set_json(key: str, obj: Dict[str, Any]) -> None:
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(
            f"{UPSTASH_URL}/set/{key}",
            headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
            data=json.dumps(obj, ensure_ascii=False),
        )
        r.raise_for_status()


# -------------------- OPENDOTA FETCH --------------------
async def fetch_ranked_matches(limit: int = 30) -> List[Dict[str, Any]]:
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(
            f"{OPENDOTA}/players/{ACCOUNT_ID}/matches",
            params={"lobby_type": 7, "limit": limit},  # 7 = Ranked
        )
        r.raise_for_status()
        return r.json() or []


def normalize_matches_for_scoring(matches: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Берем только те матчи, где есть всё нужное для win/lose:
    match_id, start_time, radiant_win, player_slot
    """
    good = []
    for m in matches:
        mid = m.get("match_id")
        st = m.get("start_time")
        rw = m.get("radiant_win")
        ps = m.get("player_slot")
        if mid is None or st is None or rw is None or ps is None:
            continue
        good.append(m)
    # OpenDota обычно дает по убыванию (свежее сверху), но на всякий:
    good.sort(key=lambda x: int(x["start_time"]), reverse=True)
    return good


# -------------------- STATE --------------------
def default_state_from_last5(last5: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Инициализация: СЧИТАЕМ последние 5 ranked матчей сразу.
    """
    mmr = START_MMR
    tw = tl = td = 0

    processed_ids: List[int] = []
    last_start_time = 0

    # last5 идут от свежего к старому (мы их так отнормализовали),
    # но для последовательности посчитаем от старого к свежему:
    last5_sorted = sorted(last5, key=lambda x: int(x["start_time"]))

    for m in last5_sorted:
        st = int(m["start_time"])
        mid = int(m["match_id"])
        won = is_win_for_player(bool(m["radiant_win"]), int(m["player_slot"]))
        delta = MMR_STEP if won else -MMR_STEP

        mmr += delta

        # Today stats (по МСК)
        if day_key_from_unix(st) == today_key():
            if won:
                tw += 1
            else:
                tl += 1
            td += delta

        processed_ids.append(mid)
        last_start_time = max(last_start_time, st)

    return {
        "start_mmr": START_MMR,
        "mmr": mmr,
        "last_start_time": last_start_time,
        "processed_ids": processed_ids[-200:],
        "today_date": today_key(),
        "today_win": tw,
        "today_lose": tl,
        "today_delta": td,
        # чтобы видеть, что реально инициализировали last5
        "init_last5_count": len(last5_sorted),
        "init_last5_last_start_time": last_start_time,
    }


async def load_or_init_state(state_key: str) -> Dict[str, Any]:
    state = await redis_get_json(state_key)
    if state:
        return state

    matches = await fetch_ranked_matches(limit=50)
    good = normalize_matches_for_scoring(matches)
    last5 = good[:5]

    # если OpenDota не отдал нужных полей — просто создадим “пустую” базу
    if len(last5) == 0:
        return {
            "start_mmr": START_MMR,
            "mmr": START_MMR,
            "last_start_time": 0,
            "processed_ids": [],
            "today_date": today_key(),
            "today_win": 0,
            "today_lose": 0,
            "today_delta": 0,
            "init_last5_count": 0,
            "init_last5_last_start_time": 0,
            "warning": "No scorable matches from OpenDota (missing radiant_win/player_slot?)",
        }

    state = default_state_from_last5(last5)
    await redis_set_json(state_key, state)
    return state


def reset_today_if_needed(state: Dict[str, Any]) -> None:
    if state.get("today_date") != today_key():
        state["today_date"] = today_key()
        state["today_win"] = 0
        state["today_lose"] = 0
        state["today_delta"] = 0


# -------------------- ROUTES --------------------
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

    state_key = f"mmr:{ACCOUNT_ID}"
    state = await load_or_init_state(state_key)

    reset_today_if_needed(state)

    # догоняем новые матчи
    matches = await fetch_ranked_matches(limit=50)
    good = normalize_matches_for_scoring(matches)

    processed = set(int(x) for x in state.get("processed_ids", []))
    last_time = int(state.get("last_start_time", 0))

    # новые матчи после last_start_time и не в processed
    new_matches = []
    for m in good:
        st = int(m["start_time"])
        mid = int(m["match_id"])
        if st > last_time and mid not in processed:
            new_matches.append(m)

    # считаем по времени от старого к новому
    new_matches.sort(key=lambda x: int(x["start_time"]))

    for m in new_matches:
        st = int(m["start_time"])
        mid = int(m["match_id"])

        won = is_win_for_player(bool(m["radiant_win"]), int(m["player_slot"]))
        delta = MMR_STEP if won else -MMR_STEP

        state["mmr"] = int(state.get("mmr", START_MMR)) + delta

        if day_key_from_unix(st) == today_key():
            if won:
                state["today_win"] = int(state.get("today_win", 0)) + 1
            else:
                state["today_lose"] = int(state.get("today_lose", 0)) + 1
            state["today_delta"] = int(state.get("today_delta", 0)) + delta

        processed.add(mid)
        state["last_start_time"] = max(int(state.get("last_start_time", 0)), st)

    state["processed_ids"] = list(processed)[-200:]
    await redis_set_json(state_key, state)

    cur = int(state.get("mmr", START_MMR))
    tw = int(state.get("today_win", 0))
    tl = int(state.get("today_lose", 0))
    td = int(state.get("today_delta", 0))

    text = f"MMR: {cur} • Today -> Win: {tw} Lose: {tl} • Total: {fmt_signed(td)}"
    _cache_text, _cache_ts = text, now
    return text


@app.get("/reset", response_class=PlainTextResponse)
async def reset(token: str = Query("")):
    """
    Сбрасывает state и пересчитывает последние 5 ranked матчей заново.
    """
    if ADMIN_TOKEN:
        if token != ADMIN_TOKEN:
            return "Forbidden"

    if not ACCOUNT_ID:
        return "DOTA_ACCOUNT_ID не установлен"
    if not UPSTASH_URL or not UPSTASH_TOKEN:
        return "Redis не настроен"

    state_key = f"mmr:{ACCOUNT_ID}"

    matches = await fetch_ranked_matches(limit=50)
    good = normalize_matches_for_scoring(matches)
    last5 = good[:5]

    if len(last5) == 0:
        state = {
            "start_mmr": START_MMR,
            "mmr": START_MMR,
            "last_start_time": 0,
            "processed_ids": [],
            "today_date": today_key(),
            "today_win": 0,
            "today_lose": 0,
            "today_delta": 0,
            "init_last5_count": 0,
            "init_last5_last_start_time": 0,
            "warning": "No scorable matches from OpenDota (missing radiant_win/player_slot?)",
        }
    else:
        state = default_state_from_last5(last5)

    await redis_set_json(state_key, state)

    # сброс кеша, чтобы сразу обновилось в чате
    global _cache_text, _cache_ts
    _cache_text, _cache_ts = None, 0.0

    return f"OK reset. init_last5_count={state.get('init_last5_count')} last_start_time={state.get('last_start_time')}"


@app.get("/debug_last_matches", response_class=PlainTextResponse)
async def debug_last_matches(token: str = Query("")):
    """
    Просто покажет последние 10 ranked из OpenDota и что в них есть.
    """
    if ADMIN_TOKEN and token != ADMIN_TOKEN:
        return "Forbidden"

    if not ACCOUNT_ID:
        return "DOTA_ACCOUNT_ID не установлен"

    matches = await fetch_ranked_matches(limit=15)
    good = normalize_matches_for_scoring(matches)

    lines = []
    lines.append(f"account_id={ACCOUNT_ID}")
    lines.append(f"got_matches={len(matches)} scorable={len(good)}")
    lines.append("last_10 (scorable):")
    for m in good[:10]:
        lines.append(
            f"- match_id={m.get('match_id')} start_time={m.get('start_time')} "
            f"radiant_win={m.get('radiant_win')} player_slot={m.get('player_slot')} lobby_type={m.get('lobby_type')}"
        )
    return "\n".join(lines)
