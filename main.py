import os
import time
import httpx
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

ACCOUNT_ID = os.environ.get("DOTA_ACCOUNT_ID")
CACHE_TTL = int(os.environ.get("CACHE_TTL", 60))

app = FastAPI()

cache_text = None
cache_time = 0

RANKS = {
    1: "Herald",
    2: "Guardian",
    3: "Crusader",
    4: "Archon",
    5: "Legend",
    6: "Ancient",
    7: "Divine",
    8: "Immortal"
}

@app.get("/mmr", response_class=PlainTextResponse)
async def get_rank():
    global cache_text, cache_time

    if not ACCOUNT_ID:
        return "DOTA_ACCOUNT_ID не установлен"

    now = time.time()
    if cache_text and (now - cache_time) < CACHE_TTL:
        return cache_text

    url = f"https://api.opendota.com/api/players/{ACCOUNT_ID}"

    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        data = response.json()

    rank_tier = data.get("rank_tier")

    if not rank_tier:
        result = "Ранг не найден"
    else:
        tier = int(str(rank_tier)[0])
        star = int(str(rank_tier)[1])
        rank_name = RANKS.get(tier, "Unknown")
        result = f"Мой ранг: {rank_name} {star}"

    cache_text = result
    cache_time = now

    return result
