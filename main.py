import os
import json
import time
from datetime import datetime, timedelta, timezone

import httpx
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

ACCOUNT_ID = os.environ.get("DOTA_ACCOUNT_ID")

START_MMR = int(os.environ.get("START_MMR", "13772"))
MMR_STEP = int(os.environ.get("MMR_STEP", "25"))
TZ_OFFSET_HOURS = int(os.environ.get("TZ_OFFSET_HOURS", "3"))

UPSTASH_URL = os.environ.get("UPSTASH_REDIS_REST_URL")
UPSTASH_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN")

OPENDOTA = "https://api.opendota.com/api"

CACHE_TTL = 15

app = FastAPI()
_cache_text = None
_cache_ts = 0

from fastapi import Query

ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "")

def tz_msk():
    return timezone(timedelta(hours=TZ_OFFSET_HOURS))


def today_key():
    return datetime.now(tz_msk()).strftime("%Y-%m-%d")


def day_key_from_unix(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=tz_msk()).strftime("%Y-%m-%d")


def fmt_signed(n: int) -> str:
    return f"+{n}" if n >= 0 else str(n)


def is_win_for_player(radiant_win: bool, player_slot: int) -> bool:
    is_radiant = int(player_slot) < 128
    return bool(radiant_win) if is_radiant else (not bool(radiant_win))


async def redis_get_json(key: str):
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


async def redis_set_json(key: str, obj: dict):
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(
            f"{UPSTASH_URL}/set/{key}",
            headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
            data=json.dumps(obj, ensure_ascii=False),
        )
        r.raise_for_status()


async def fetch_latest_ranked_start_time() -> int:
    """Нужен, чтобы при первом запуске НЕ считать старые матчи."""
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(
            f"{OPENDOTA}/players/{ACCOUNT_ID}/matches",
            params={"lobby_type": 7, "limit": 1},
        )
        r.raise_for_status()
        matches = r.json() or []
    if not matches:
        return 0
    st = matches[0].get("start_time")
    return int(st) if st else 0


def default_state(baseline_start_time: int):
    return {
        "start_mmr": START_MMR,
        "mmr": START_MMR,
        "last_start_time": baseline_start_time,   # стартуем отсюда (старые матчи не считаем)
        "processed_ids": [],                      # чтобы не пересчитывать одно и то же
        "today_date": today_key(),
        "today_win": 0,
        "today_lose": 0,
        "today_delta": 0,                         # (win-lose)*25 за сегодня
    }


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
    state = await redis_get_json(state_key)

    # Первый запуск: создаём состояние и НЕ считаем старые матчи
    if not state:
        baseline = await fetch_latest_ranked_start_time()
        state = default_state(baseline)
        await redis_set_json(state_key, state)

    # Сброс Today по МСК (раз в сутки)
    if state.get("today_date") != today_key():
        state["today_date"] = today_key()
        state["today_win"] = 0
        state["today_lose"] = 0
        state["today_delta"] = 0

    # Берём ranked матчи и догоняем всё НОВОЕ
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(
            f"{OPENDOTA}/players/{ACCOUNT_ID}/matches",
            params={"lobby_type": 7, "limit": 30},
        )
        r.raise_for_status()
        matches = r.json() or []

    processed = set(state.get("processed_ids", []))
    last_time = int(state.get("last_start_time", 0))

    new_matches = []
    for m in matches:
        st = m.get("start_time")
        mid = m.get("match_id")
        if not st or not mid:
            continue
        st = int(st)
        mid = int(mid)
        if st > last_time and mid not in processed:
            new_matches.append(m)

    new_matches.sort(key=lambda x: int(x.get("start_time", 0)))

    for m in new_matches:
        st = int(m["start_time"])
        mid = int(m["match_id"])

        radiant_win = m.get("radiant_win")
        player_slot = m.get("player_slot")
        if radiant_win is None or player_slot is None:
            continue

        won = is_win_for_player(bool(radiant_win), int(player_slot))
        delta = MMR_STEP if won else -MMR_STEP

        # общий MMR
        state["mmr"] = int(state.get("mmr", START_MMR)) + delta

        # статистика за СЕГОДНЯ (по МСК)
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

    # ВЫВОД ровно в твоём формате
    cur = int(state.get("mmr", START_MMR))
    tw = int(state.get("today_win", 0))
    tl = int(state.get("today_lose", 0))
    td = int(state.get("today_delta", 0))

    text = f"MMR: {cur} • Today -> Win: {tw} Lose: {tl} • Total: {fmt_signed(td)}"
    _cache_text, _cache_ts = text, now
    return text

@app.get("/testwin", response_class=PlainTextResponse)
async def test_win(token: str = Query("")):
    if token != ADMIN_TOKEN:
        return "Forbidden"
    # дальше твой код без изменений

@app.get("/testwin", response_class=PlainTextResponse)
async def test_win():
    state_key = f"mmr:{ACCOUNT_ID}"
    state = await redis_get_json(state_key)
    if not state:
        return "State not initialized"

    state["mmr"] += MMR_STEP
    state["today_win"] += 1
    state["today_delta"] += MMR_STEP

    await redis_set_json(state_key, state)

    return "WIN added"

@app.get("/testlose", response_class=PlainTextResponse)
async def test_lose(token: str = Query("")):
    if token != ADMIN_TOKEN:
        return "Forbidden"
    # дальше твой код без изменений

@app.get("/testlose", response_class=PlainTextResponse)
async def test_lose():
    state_key = f"mmr:{ACCOUNT_ID}"
    state = await redis_get_json(state_key)
    if not state:
        return "State not initialized"

    state["mmr"] -= MMR_STEP
    state["today_lose"] += 1
    state["today_delta"] -= MMR_STEP

    await redis_set_json(state_key, state)

    return "LOSE added"

@app.get("/reset", response_class=PlainTextResponse)
async def reset(token: str = Query("")):
    if token != ADMIN_TOKEN:
        return "Forbidden"

    state_key = f"mmr:{ACCOUNT_ID}"
    baseline = await fetch_latest_ranked_start_time()
    state = default_state(baseline)

    await redis_set_json(state_key, state)

    # сброс кэша, чтобы /mmr сразу обновился
    global _cache_text, _cache_ts
    _cache_text = None
    _cache_ts = 0

    return "State reset"
