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

@app.get("/mmr", response_class=PlainTextResponse)
async def get_mmr():
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

    mmr = data.get("mmr_estimate", {}).get("estimate")

    if not mmr:
        result = "MMR не найден (возможно профиль приватный)"
    else:
        result = f"Мой текущий MMR: {mmr}"

    cache_text = result
    cache_time = now

    return result
