import os
import json
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import FastAPI, Query
from fastapi.responses import PlainTextResponse

# ===== ENV =====
ACCOUNT_ID = os.environ.get("DOTA_ACCOUNT_ID")  # 32-bit dota account_id (не steam64)
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "")

START_MMR = int(os.environ.get("START_MMR", "13772"))ff
MMR_STEP = int(os.environ.get("MMR_STEP", "25"))
TZ_OFFSET_HOURS = int(os.environ.get("TZ_OFFSET_HOURS", "3"))

UPSTASH_URL = os.environ.get("UPSTASH_REDIS_REST_URL")
UPSTASH_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN")

OPENDOTA = "https://api.opendota.com/api"

CACHE_TTL = 10  # сек кэш ответа /mmr

app = FastAPI()
_cache_text: Optional[str] = None
_cache_ts: float = 0.0


# ===== TIME HELPERS =====
def tz_msk():
    return timezone(timedelta(hours=TZ_OFFSET_HOURS))


def today_key():
    return datetime.now(tz_msk()).strftime("%Y-%m-%d")


def day_key_from_unix(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=tz_msk()).strftime("%Y-%m-%d")


def fmt_signed(n: int) -> str:
    return f"+{n}" if n >= 0 else str(n)


def is_win_for_player(radiant_win: bool, player_slot: int) -> bool:
    # player_slot < 128 => radiant, иначе dire
    is_radiant = int(player_slot) < 128
    return bool(radiant_win) if is_radiant else (not bool(radiant_win))


# ===== REDIS (UPSTASH REST) =====
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


async def redis_set_json(key: str, obj: Dict[str, Any]):
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(
            f"{UPSTASH_URL}/set/{key}",
            headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
            data=json.dumps(obj, ensure_ascii=False),
        )
        r.raise_for_status()


def state_key() -> str:
    return f"mmr:{ACCOUNT_ID}"


def default_state() -> Dict[str, Any]:
    return {
        "start_mmr": START_MMR,
        "mmr": START_MMR,
        "last_start_time": 0,       # будем считать матчи с start_time > last_start_time
        "processed_ids": [],        # match_id уже учтённые
        "today_date": today_key(),
        "today_win": 0,
        "today_lose": 0,
        "today_delta": 0,
    }


# ===== OPENDOTA =====
async def fetch_ranked_matches(limit: int = 30) -> List[Dict[str, Any]]:
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(
            f"{OPENDOTA}/players/{ACCOUNT_ID}/matches",
            params={"lobby_type": 7, "limit": limit},
        )
        r.raise_for_status()
        return r.json() or []


def compute_delta_from_match(m: Dict[str, Any]) -> Optional[int]:
    st = m.get("start_time")
    mid = m.get("match_id")
    radiant_win = m.get("radiant_win")
    player_slot = m.get("player_slot")
    if not st or not mid:
        return None
    if radiant_win is None or player_slot is None:
        # если профиль/история скрыты — этих полей может не быть
        return None
    won = is_win_for_player(bool(radiant_win), int(player_slot))
    return MMR_STEP if won else -MMR_STEP


def extract_match_brief(m: Dict[str, Any]) -> Tuple[int, int, Optional[bool], Optional[int], Optional[int]]:
    # (match_id, start_time, radiant_win, player_slot, lobby_type)
    return (
        int(m.get("match_id") or 0),
        int(m.get("start_time") or 0),
        m.get("radiant_win"),
        (int(m.get("player_slot")) if m.get("player_slot") is not None else None),
        (int(m.get("lobby_type")) if m.get("lobby_type") is not None else None),
    )


# ===== ROUTES =====
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

    st_key = state_key()
    state = await redis_get_json(st_key)
    if not state:
        state = default_state()
        await redis_set_json(st_key, state)

    # daily reset по МСК
    if state.get("today_date") != today_key():
        state["today_date"] = today_key()
        state["today_win"] = 0
        state["today_lose"] = 0
        state["today_delta"] = 0

    matches = await fetch_ranked_matches(limit=30)

    processed = set(state.get("processed_ids", []))
    last_time = int(state.get("last_start_time", 0))

    # берём только новые матчи
    new_matches = []
    for m in matches:
        mid, st, _, _, _ = extract_match_brief(m)
        if mid and st and st > last_time and mid not in processed:
            new_matches.append(m)

    # считаем по времени от старых к новым
    new_matches.sort(key=lambda x: int(x.get("start_time", 0)))

    for m in new_matches:
        mid, st, _, _, _ = extract_match_brief(m)
        delta = compute_delta_from_match(m)
        if delta is None:
            # просто пропускаем матч если не хватает данных
            continue

        state["mmr"] = int(state.get("mmr", START_MMR)) + delta

        if day_key_from_unix(st) == today_key():
            if delta > 0:
                state["today_win"] = int(state.get("today_win", 0)) + 1
            else:
                state["today_lose"] = int(state.get("today_lose", 0)) + 1
            state["today_delta"] = int(state.get("today_delta", 0)) + delta

        processed.add(mid)
        state["last_start_time"] = max(int(state.get("last_start_time", 0)), st)

    state["processed_ids"] = list(processed)[-300:]
    await redis_set_json(st_key, state)

    cur = int(state.get("mmr", START_MMR))
    tw = int(state.get("today_win", 0))
    tl = int(state.get("today_lose", 0))
    td = int(state.get("today_delta", 0))

    text = f"MMR: {cur} • Today -> Win: {tw} Lose: {tl} • Total: {fmt_signed(td)}"
    _cache_text, _cache_ts = text, now
    return text


# ===== ADMIN/DEBUG =====
def check_admin(token: str) -> bool:
    return bool(ADMIN_TOKEN) and token == ADMIN_TOKEN


@app.get("/reset", response_class=PlainTextResponse)
async def reset(
    token: str = Query(""),
    init_last: int = Query(0, ge=0, le=30),
):
    """
    reset?token=XXX&init_last=5
    - сбрасывает состояние на START_MMR
    - если init_last>0: сразу засчитывает последние init_last ranked матчей
      и помечает их processed_ids, last_start_time = start_time самого свежего из них
    """
    if not check_admin(token):
        return "Forbidden"

    if not ACCOUNT_ID:
        return "DOTA_ACCOUNT_ID не установлен"
    if not UPSTASH_URL or not UPSTASH_TOKEN:
        return "Redis не настроен"

    st_key = state_key()
    state = default_state()

    matches = await fetch_ranked_matches(limit=max(30, init_last))
    # последние init_last матчей (самые свежие в начале списка)
    used = matches[:init_last] if init_last > 0 else []

    # считать от старого к новому
    used_sorted = sorted(used, key=lambda x: int(x.get("start_time", 0)))

        mmr_val = START_MMR
    # В reset считаем "Today" как "за init_last матчей", чтобы сразу показать результат
    today_w = 0
    today_l = 0
    today_d = 0

    processed_ids: List[int] = []
    last_st = 0

    for m in used_sorted:
        mid, st, _, _, _ = extract_match_brief(m)
        delta = compute_delta_from_match(m)
        if not mid or not st or delta is None:
            continue

        mmr_val += delta
        processed_ids.append(mid)
        last_st = max(last_st, st)

               # В reset считаем "Today" как статистику за init_last матчей
        if delta > 0:
            today_w += 1
        else:
            today_l += 1
        today_d += delta



    state["mmr"] = mmr_val
    state["processed_ids"] = processed_ids
    state["last_start_time"] = last_st
    state["today_date"] = today_key()
    state["today_win"] = today_w
    state["today_lose"] = today_l
    state["today_delta"] = today_d

    await redis_set_json(st_key, state)

    # сбросим кэш, чтобы /mmr сразу показал новое
    global _cache_text, _cache_ts
    _cache_text, _cache_ts = None, 0

    return f"OK reset. init_last={init_last} counted={len(processed_ids)} last_start_time={last_st}"


@app.get("/debug_last_matches", response_class=PlainTextResponse)
async def debug_last_matches(token: str = Query(""), limit: int = Query(15, ge=1, le=30)):
    if not check_admin(token):
        return "Forbidden"

    matches = await fetch_ranked_matches(limit=limit)
    lines = []
    lines.append(f"account_id={ACCOUNT_ID}")
    lines.append(f"got_matches={len(matches)}")
    lines.append(f"last_{min(10, len(matches))}:")
    for m in matches[:10]:
        mid, st, rw, ps, lt = extract_match_brief(m)
        lines.append(f"- match_id={mid} start_time={st} radiant_win={rw} player_slot={ps} lobby_type={lt}")

    st = await redis_get_json(state_key())
    if st:
        lines.append("")
        lines.append("state:")
        lines.append(f"mmr={st.get('mmr')}")
        lines.append(f"last_start_time={st.get('last_start_time')}")
        lines.append(f"processed_ids_count={len(st.get('processed_ids', []))}")
        lines.append(f"today_date={st.get('today_date')} win={st.get('today_win')} lose={st.get('today_lose')} delta={st.get('today_delta')}")
    else:
        lines.append("")
        lines.append("state: NONE")

    return "\n".join(lines)
