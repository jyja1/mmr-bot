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

CACHE_TTL = 15

UPSTASH_URL = os.environ.get("UPSTASH_REDIS_REST_URL")
UPSTASH_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN")

OPENDOTA = "https://api.opendota.com/api"

app = FastAPI()
_cache_text = None
_cache_ts = 0


def msk_tz():
    return timezone(timedelta(hours=TZ_OFFSET_HOURS))


def today_key():
    return datetime.now(msk_tz()).strftime("%Y-%m-%d")


def day_key_from_unix(ts):
    return datetime.fromtimestamp(ts, tz=msk_tz()).strftime("%Y-%m-%d")


def fmt(n):
    return f"+{n}" if n >= 0 else str(n)


def is_win(radiant_win, player_slot):
    is_radiant = player_slot < 128
    return radiant_win if is_radiant else (not radiant_win)


async def redis_get(key):
    async with httpx.AsyncClient() as client:
        r = await client.get(
            f"{UPSTASH_URL}/get/{key}",
            headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"}
        )
        data = r.json()
        if data.get("result") is None:
            return None
        return json.loads(data["result"])


async def redis_set(key, value):
    async with httpx.AsyncClient() as client:
        await client.post(
            f"{UPSTASH_URL}/set/{key}",
            headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
            data=json.dumps(value)
        )


def default_state():
    return {
        "start_mmr": START_MMR,
        "mmr": START_MMR,
        "last_start_time": 0,
        "processed_ids": [],
        "today_date": today_key(),
        "today_delta": 0
    }


@app.get("/mmr", response_class=PlainTextResponse)
async def mmr():
    global _cache_text, _cache_ts

    now = time.time()
    if _cache_text and now - _cache_ts < CACHE_TTL:
        return _cache_text

    if not UPSTASH_URL or not UPSTASH_TOKEN:
        return "Redis не настроен"

    state_key = f"mmr:{ACCOUNT_ID}"
    state = await redis_get(state_key)
    if not state:
        state = default_state()

    # Сброс Today по МСК
    if state["today_date"] != today_key():
        state["today_date"] = today_key()
        state["today_delta"] = 0

    # Получаем ranked матчи
    async with httpx.AsyncClient() as client:
        r = await client.get(
            f"{OPENDOTA}/players/{ACCOUNT_ID}/matches",
            params={"lobby_type": 7, "limit": 30}
        )
        matches = r.json()

    processed = set(state["processed_ids"])
    last_time = state["last_start_time"]

    new_matches = []
    for m in matches:
        if m.get("start_time") and m.get("match_id"):
            if m["start_time"] > last_time and m["match_id"] not in processed:
                new_matches.append(m)

    new_matches.sort(key=lambda x: x["start_time"])

    for m in new_matches:
        won = is_win(m["radiant_win"], m["player_slot"])
        delta = MMR_STEP if won else -MMR_STEP

        state["mmr"] += delta

        if day_key_from_unix(m["start_time"]) == today_key():
            state["today_delta"] += delta

        processed.add(m["match_id"])
        state["last_start_time"] = max(state["last_start_time"], m["start_time"])

    state["processed_ids"] = list(processed)[-200:]
    await redis_set(state_key, state)

    cur = state["mmr"]
    total = cur - state["start_mmr"]

    text = f"MMR: {cur} • Today: {fmt(state['today_delta'])} • Total: {fmt(total)}"

    _cache_text = text
    _cache_ts = now

    return text
