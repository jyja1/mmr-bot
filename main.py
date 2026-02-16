import os
import time
from datetime import datetime, timezone, timedelta

import httpx
from fastapi import FastAPI, Query
from fastapi.responses import PlainTextResponse

app = FastAPI()

# =========================
# ENV
# =========================

ACCOUNT_ID = os.environ.get("DOTA_ACCOUNT_ID")
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "")

START_MMR = int(os.environ.get("START_MMR", "13772"))
MMR_STEP = int(os.environ.get("MMR_STEP", "25"))
TZ_OFFSET_HOURS = int(os.environ.get("TZ_OFFSET_HOURS", "3"))

OPENDOTA = "https://api.opendota.com/api"

if not ACCOUNT_ID:
    raise Exception("DOTA_ACCOUNT_ID not set")


# =========================
# STATE (in memory)
# =========================

state = {
    "mmr": START_MMR,
    "today_date": None,
    "win": 0,
    "lose": 0,
    "delta": 0,
    "processed_ids": set(),
}

cache = {
    "value": "",
    "ts": 0,
}

CACHE_TTL = 10


# =========================
# HELPERS
# =========================

def now_local_date():
    tz = timezone(timedelta(hours=TZ_OFFSET_HOURS))
    return datetime.now(tz).date().isoformat()


async def fetch_matches():
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(
            f"{OPENDOTA}/players/{ACCOUNT_ID}/matches",
            params={"limit": 20}
        )
        r.raise_for_status()
        return r.json()


def match_is_win(match):
    player_slot = match.get("player_slot", 0)
    radiant_win = match.get("radiant_win", False)

    is_radiant = player_slot < 128
    return (is_radiant and radiant_win) or (not is_radiant and not radiant_win)


async def process_new_matches():
    matches = await fetch_matches()
    today = now_local_date()

    if state["today_date"] != today:
        state["today_date"] = today
        state["win"] = 0
        state["lose"] = 0
        state["delta"] = 0

    for match in matches:
        if match["lobby_type"] != 7:
            continue

        mid = match["match_id"]
        if mid in state["processed_ids"]:
            continue

        state["processed_ids"].add(mid)

        if match_is_win(match):
            state["win"] += 1
            state["delta"] += MMR_STEP
            state["mmr"] += MMR_STEP
        else:
            state["lose"] += 1
            state["delta"] -= MMR_STEP
            state["mmr"] -= MMR_STEP


# =========================
# ROUTES
# =========================

@app.get("/health")
async def health():
    return "ok"


@app.get("/mmr", response_class=PlainTextResponse)
async def mmr():
    now = time.time()
    if now - cache["ts"] < CACHE_TTL:
        return cache["value"]

    await process_new_matches()

    text = (
        f"MMR: {state['mmr']} • "
        f"Today → Win: {state['win']} Lose: {state['lose']} • "
        f"Total: {state['delta']:+}"
    )

    cache["value"] = text
    cache["ts"] = now
    return text


@app.get("/reset", response_class=PlainTextResponse)
async def reset(token: str = Query(...), init_last: int = Query(0)):
    if token != ADMIN_TOKEN:
        return "Forbidden"

    state["mmr"] = START_MMR
    state["win"] = 0
    state["lose"] = 0
    state["delta"] = 0
    state["processed_ids"].clear()
    state["today_date"] = now_local_date()

    if init_last > 0:
        matches = await fetch_matches()
        ranked = [m for m in matches if m["lobby_type"] == 7][:init_last]

        for match in ranked:
            mid = match["match_id"]
            state["processed_ids"].add(mid)

            if match_is_win(match):
                state["win"] += 1
                state["delta"] += MMR_STEP
                state["mmr"] += MMR_STEP
            else:
                state["lose"] += 1
                state["delta"] -= MMR_STEP
                state["mmr"] -= MMR_STEP

    return f"OK reset. init_last={init_last}"


@app.get("/debug_last_matches", response_class=PlainTextResponse)
async def debug(token: str = Query(...)):
    if token != ADMIN_TOKEN:
        return "Forbidden"

    matches = await fetch_matches()
    ranked = [m for m in matches if m["lobby_type"] == 7]

    lines = []
    lines.append(f"account_id={ACCOUNT_ID}")
    lines.append(f"ranked_found={len(ranked)}")

    for m in ranked[:10]:
        lines.append(
            f"- match_id={m['match_id']} "
            f"radiant_win={m.get('radiant_win')} "
            f"player_slot={m.get('player_slot')} "
            f"start_time={m.get('start_time')}"
        )

    lines.append("")
    lines.append(f"mmr={state['mmr']}")
    lines.append(f"win={state['win']}")
    lines.append(f"lose={state['lose']}")
    lines.append(f"delta={state['delta']}")
    lines.append(f"processed={len(state['processed_ids'])}")

    return "\n".join(lines)
