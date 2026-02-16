import os
import json
import time
import hmac
import hashlib
from typing import Any, Dict, Optional, List, Tuple

import httpx
from fastapi import FastAPI, Request, Query, HTTPException
from fastapi.responses import PlainTextResponse


# -----------------------
# ENV
# -----------------------
ACCOUNT_ID = os.environ.get("DOTA_ACCOUNT_ID")  # 32-bit dota account id (например 1258333388)
START_MMR = int(os.environ.get("START_MMR", "13772"))
MMR_STEP = int(os.environ.get("MMR_STEP", "25"))

UPSTASH_URL = os.environ.get("UPSTASH_REDIS_REST_URL")
UPSTASH_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN")

ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "")

STEAM_API_KEY = os.environ.get("STEAM_API_KEY", "")

TWITCH_CLIENT_ID = os.environ.get("TWITCH_CLIENT_ID", "")
TWITCH_CLIENT_SECRET = os.environ.get("TWITCH_CLIENT_SECRET", "")
TWITCH_BROADCASTER_LOGIN = os.environ.get("TWITCH_BROADCASTER_LOGIN", "")  # debustie
EVENTSUB_SECRET = os.environ.get("EVENTSUB_SECRET", "")

# Render public base URL, если не задан — используем домен сервиса
PUBLIC_BASE_URL = os.environ.get("PUBLIC_BASE_URL", "https://mmr-bot.onrender.com").rstrip("/")

# -----------------------
# CONST
# -----------------------
VALVE_HISTORY_URL = "https://api.steampowered.com/IDOTA2Match_570/GetMatchHistory/v1/"
VALVE_DETAILS_URL = "https://api.steampowered.com/IDOTA2Match_570/GetMatchDetails/v1/"

CACHE_TTL = 10  # сек, чтобы фоссабот не долбил Valve слишком часто

# Retry policy for match details
DETAILS_MAX_ATTEMPTS = 12
DETAILS_BACKOFF_BASE = 20  # сек
DETAILS_BACKOFF_MAX = 10 * 60  # 10 минут

# Limit memory
MAX_PROCESSED_STREAM = 500
MAX_PENDING = 200
MAX_ERRORS = 30


# -----------------------
# APP
# -----------------------
app = FastAPI()
_cache_text: Optional[str] = None
_cache_ts: float = 0.0


# -----------------------
# HELPERS
# -----------------------
def fmt_signed(n: int) -> str:
    return f"+{n}" if n >= 0 else str(n)


def now_ts() -> int:
    return int(time.time())


def admin_guard(token: str) -> None:
    if not ADMIN_TOKEN:
        raise HTTPException(status_code=500, detail="ADMIN_TOKEN not set on server")
    if token != ADMIN_TOKEN:
        raise HTTPException(status_code=403, detail="Forbidden")


def state_key() -> str:
    # один аккаунт
    return f"mmr:{ACCOUNT_ID}"


def default_state() -> Dict[str, Any]:
    return {
        "version": 3,

        # MMR global
        "mmr": START_MMR,

        # Stream mode (today = current stream)
        "stream_active": False,
        "stream_start_time": 0,     # unix seconds
        "stream_win": 0,
        "stream_lose": 0,
        "stream_delta": 0,

        # Track processed matches for THIS stream only
        "processed_ids_stream": [],  # list[int]

        # Pending matches awaiting details
        # pending[match_id] = { "start_time": int, "attempts": int, "next_try": int }
        "pending": {},

        # Debug last errors
        "last_errors": [],  # list[str]
    }


def push_error(st: Dict[str, Any], msg: str) -> None:
    errs = st.get("last_errors", [])
    errs.append(msg)
    st["last_errors"] = errs[-MAX_ERRORS:]


async def redis_get_json(key: str) -> Optional[Dict[str, Any]]:
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
        try:
            return json.loads(val)
        except Exception:
            return None


async def redis_set_json(key: str, obj: Dict[str, Any]) -> None:
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(
            f"{UPSTASH_URL}/set/{key}",
            headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
            data=json.dumps(obj, ensure_ascii=False),
        )
        r.raise_for_status()


def is_win_for_player(radiant_win: bool, player_slot: int) -> bool:
    # player_slot < 128 => radiant
    is_radiant = int(player_slot) < 128
    return bool(radiant_win) if is_radiant else (not bool(radiant_win))


def clamp_pending(st: Dict[str, Any]) -> None:
    pending = st.get("pending", {})
    if not isinstance(pending, dict):
        st["pending"] = {}
        return
    # keep only newest by start_time
    if len(pending) <= MAX_PENDING:
        return
    items = []
    for mid_str, meta in pending.items():
        try:
            stt = int(meta.get("start_time", 0))
        except Exception:
            stt = 0
        items.append((mid_str, stt))
    items.sort(key=lambda x: x[1], reverse=True)
    keep = dict(items[:MAX_PENDING])
    st["pending"] = {k: pending[k] for k in keep.keys()}


def clamp_processed(st: Dict[str, Any]) -> None:
    arr = st.get("processed_ids_stream", [])
    if not isinstance(arr, list):
        st["processed_ids_stream"] = []
        return
    st["processed_ids_stream"] = arr[-MAX_PROCESSED_STREAM:]


# -----------------------
# VALVE API
# -----------------------
async def valve_get_match_history(limit: int = 20) -> List[Dict[str, Any]]:
    if not STEAM_API_KEY:
        raise RuntimeError("STEAM_API_KEY not set")
    if not ACCOUNT_ID:
        raise RuntimeError("DOTA_ACCOUNT_ID not set")

    params = {
        "key": STEAM_API_KEY,
        "account_id": int(ACCOUNT_ID),
        "matches_requested": int(limit),
        "lobby_type": 7,  # ranked
    }
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(VALVE_HISTORY_URL, params=params)
        r.raise_for_status()
        data = r.json() or {}
        res = data.get("result") or {}
        matches = res.get("matches") or []
        return matches


async def valve_get_match_details(match_id: int) -> Dict[str, Any]:
    if not STEAM_API_KEY:
        raise RuntimeError("STEAM_API_KEY not set")

    params = {
        "key": STEAM_API_KEY,
        "match_id": int(match_id),
    }
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(VALVE_DETAILS_URL, params=params)

        # Часто бывает 500 — это и есть наша проблема
        if r.status_code >= 500:
            raise httpx.HTTPStatusError("Valve details 5xx", request=r.request, response=r)

        r.raise_for_status()
        data = r.json() or {}
        res = data.get("result") or {}
        return res


# -----------------------
# CORE: PROCESS MATCHES
# -----------------------
async def process_stream_matches(st: Dict[str, Any]) -> Tuple[int, int]:
    """
    Returns: (processed_ok_count, pending_added_or_retried_count)
    """
    if not st.get("stream_active"):
        return (0, 0)

    stream_start = int(st.get("stream_start_time", 0))
    if stream_start <= 0:
        return (0, 0)

    processed = set(int(x) for x in (st.get("processed_ids_stream") or []))
    pending: Dict[str, Any] = st.get("pending", {}) if isinstance(st.get("pending"), dict) else {}

    now = now_ts()
    ok = 0
    pend_work = 0

    # 1) First retry pending that are due
    due_ids = []
    for mid_str, meta in pending.items():
        try:
            mid = int(mid_str)
            next_try = int(meta.get("next_try", 0))
        except Exception:
            continue
        if next_try <= now:
            due_ids.append(mid)

    for mid in due_ids:
        meta = pending.get(str(mid), {})
        try:
            details = await valve_get_match_details(mid)
            radiant_win = bool(details.get("radiant_win"))
            # find player_slot for our account
            player_slot = None
            for p in (details.get("players") or []):
                if int(p.get("account_id", -1)) == int(ACCOUNT_ID):
                    player_slot = int(p.get("player_slot"))
                    break
            if player_slot is None:
                push_error(st, f"{now}: details ok but player not found match_id={mid}")
                # mark as processed to avoid infinite loop
                processed.add(mid)
                pending.pop(str(mid), None)
                ok += 1
                continue

            won = is_win_for_player(radiant_win, player_slot)
            delta = MMR_STEP if won else -MMR_STEP

            st["mmr"] = int(st.get("mmr", START_MMR)) + delta
            if won:
                st["stream_win"] = int(st.get("stream_win", 0)) + 1
            else:
                st["stream_lose"] = int(st.get("stream_lose", 0)) + 1
            st["stream_delta"] = int(st.get("stream_delta", 0)) + delta

            processed.add(mid)
            pending.pop(str(mid), None)
            ok += 1
        except Exception as e:
            # keep pending with backoff
            attempts = int(meta.get("attempts", 0)) + 1
            backoff = min(DETAILS_BACKOFF_MAX, DETAILS_BACKOFF_BASE * (2 ** min(attempts, 8)))
            pending[str(mid)] = {
                **meta,
                "attempts": attempts,
                "next_try": now + backoff,
            }
            push_error(st, f"{now}: details retry failed match_id={mid} attempts={attempts} err={type(e).__name__}")
            pend_work += 1

            if attempts >= DETAILS_MAX_ATTEMPTS:
                # give up but do not break whole bot; mark processed to stop spam
                processed.add(mid)
                pending.pop(str(mid), None)
                push_error(st, f"{now}: details GIVE UP match_id={mid} after {attempts} attempts")
                ok += 1

    # 2) Fetch recent match history and enqueue new matches after stream start
    try:
        matches = await valve_get_match_history(limit=25)
    except Exception as e:
        push_error(st, f"{now}: history error err={type(e).__name__}")
        # still return (we might have processed some pending)
        st["processed_ids_stream"] = list(processed)
        st["pending"] = pending
        clamp_processed(st)
        clamp_pending(st)
        return (ok, pend_work)

    # choose candidates: start_time >= stream_start, ranked, not processed, not pending
    candidates = []
    for m in matches:
        mid = m.get("match_id")
        stt = m.get("start_time")
        lobby_type = m.get("lobby_type")

        if not mid or not stt:
            continue
        mid = int(mid)
        stt = int(stt)
        if int(lobby_type or 0) != 7:
            continue
        if stt < stream_start:
            continue
        if mid in processed:
            continue
        if str(mid) in pending:
            continue
        candidates.append((stt, mid))

    # sort old->new so stats apply in order
    candidates.sort(key=lambda x: x[0])

    for stt, mid in candidates:
        try:
            details = await valve_get_match_details(mid)
            radiant_win = bool(details.get("radiant_win"))
            player_slot = None
            for p in (details.get("players") or []):
                if int(p.get("account_id", -1)) == int(ACCOUNT_ID):
                    player_slot = int(p.get("player_slot"))
                    break
            if player_slot is None:
                push_error(st, f"{now}: details ok but player not found match_id={mid}")
                processed.add(mid)
                ok += 1
                continue

            won = is_win_for_player(radiant_win, player_slot)
            delta = MMR_STEP if won else -MMR_STEP

            st["mmr"] = int(st.get("mmr", START_MMR)) + delta
            if won:
                st["stream_win"] = int(st.get("stream_win", 0)) + 1
            else:
                st["stream_lose"] = int(st.get("stream_lose", 0)) + 1
            st["stream_delta"] = int(st.get("stream_delta", 0)) + delta

            processed.add(mid)
            ok += 1

        except Exception as e:
            # enqueue pending
            pending[str(mid)] = {
                "start_time": stt,
                "attempts": 1,
                "next_try": now + DETAILS_BACKOFF_BASE,
            }
            push_error(st, f"{now}: details enqueue pending match_id={mid} err={type(e).__name__}")
            pend_work += 1

    st["processed_ids_stream"] = list(processed)
    st["pending"] = pending
    clamp_processed(st)
    clamp_pending(st)
    return (ok, pend_work)


# -----------------------
# TWITCH EVENTSUB
# -----------------------
async def twitch_app_token() -> str:
    if not TWITCH_CLIENT_ID or not TWITCH_CLIENT_SECRET:
        raise RuntimeError("TWITCH_CLIENT_ID/SECRET not set")
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(
            "https://id.twitch.tv/oauth2/token",
            params={
                "client_id": TWITCH_CLIENT_ID,
                "client_secret": TWITCH_CLIENT_SECRET,
                "grant_type": "client_credentials",
            },
        )
        r.raise_for_status()
        data = r.json() or {}
        return data.get("access_token", "")


async def twitch_get_user_id(login: str, access_token: str) -> str:
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(
            "https://api.twitch.tv/helix/users",
            params={"login": login},
            headers={
                "Authorization": f"Bearer {access_token}",
                "Client-Id": TWITCH_CLIENT_ID,
            },
        )
        r.raise_for_status()
        data = r.json() or {}
        arr = data.get("data") or []
        if not arr:
            raise RuntimeError("Broadcaster not found")
        return arr[0].get("id", "")


async def twitch_list_subs(access_token: str) -> List[Dict[str, Any]]:
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(
            "https://api.twitch.tv/helix/eventsub/subscriptions",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Client-Id": TWITCH_CLIENT_ID,
            },
        )
        r.raise_for_status()
        data = r.json() or {}
        return data.get("data") or []


async def twitch_create_sub(access_token: str, broadcaster_id: str, typ: str) -> None:
    # typ: stream.online / stream.offline
    body = {
        "type": typ,
        "version": "1",
        "condition": {"broadcaster_user_id": broadcaster_id},
        "transport": {
            "method": "webhook",
            "callback": f"{PUBLIC_BASE_URL}/eventsub",
            "secret": EVENTSUB_SECRET,
        },
    }
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(
            "https://api.twitch.tv/helix/eventsub/subscriptions",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Client-Id": TWITCH_CLIENT_ID,
                "Content-Type": "application/json",
            },
            json=body,
        )
        r.raise_for_status()


def verify_eventsub_signature(req: Request, body: bytes) -> bool:
    msg_id = req.headers.get("Twitch-Eventsub-Message-Id", "")
    msg_ts = req.headers.get("Twitch-Eventsub-Message-Timestamp", "")
    sig = req.headers.get("Twitch-Eventsub-Message-Signature", "")
    if not msg_id or not msg_ts or not sig:
        return False
    if not EVENTSUB_SECRET:
        return False

    message = (msg_id + msg_ts).encode("utf-8") + body
    digest = hmac.new(EVENTSUB_SECRET.encode("utf-8"), message, hashlib.sha256).hexdigest()
    expected = f"sha256={digest}"
    return hmac.compare_digest(expected, sig)


async def set_stream_active(active: bool, start_time: int = 0) -> None:
    st = await redis_get_json(state_key()) or default_state()
    st["stream_active"] = bool(active)
    if active:
        # start new stream session
        st["stream_start_time"] = int(start_time) if start_time else now_ts()
        st["stream_win"] = 0
        st["stream_lose"] = 0
        st["stream_delta"] = 0
        st["processed_ids_stream"] = []
        st["pending"] = {}
        st["last_errors"] = []
    await redis_set_json(state_key(), st)


# -----------------------
# ROUTES
# -----------------------
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
    if not STEAM_API_KEY:
        return "STEAM_API_KEY не установлен"

    st = await redis_get_json(state_key())
    if not st:
        st = default_state()
        await redis_set_json(state_key(), st)

    # process matches if stream active
    try:
        await process_stream_matches(st)
    except Exception as e:
        push_error(st, f"{now_ts()}: process_stream_matches crash err={type(e).__name__}")

    await redis_set_json(state_key(), st)

    cur = int(st.get("mmr", START_MMR))
    tw = int(st.get("stream_win", 0))
    tl = int(st.get("stream_lose", 0))
    td = int(st.get("stream_delta", 0))

    text = f"MMR: {cur} • Today -> Win: {tw} Lose: {tl} • Total: {fmt_signed(td)}"
    _cache_text, _cache_ts = text, now
    return text


@app.get("/streamstatus", response_class=PlainTextResponse)
async def streamstatus(token: str = Query("")):
    admin_guard(token)

    st = await redis_get_json(state_key()) or default_state()
    lines = [
        f"account_id={ACCOUNT_ID}",
        f"stream_active={st.get('stream_active')}",
        f"stream_start_time={st.get('stream_start_time')}",
        f"stream_win={st.get('stream_win')}",
        f"stream_lose={st.get('stream_lose')}",
        f"stream_delta={st.get('stream_delta')}",
        f"mmr={st.get('mmr')}",
        f"processed_ids_stream_count={len(st.get('processed_ids_stream') or [])}",
        f"pending_count={len((st.get('pending') or {}))}",
        "",
        "last_errors:",
    ]
    for e in (st.get("last_errors") or []):
        lines.append(f"- {e}")
    return "\n".join(lines)


@app.get("/debug_last_matches", response_class=PlainTextResponse)
async def debug_last_matches(token: str = Query("")):
    admin_guard(token)

    st = await redis_get_json(state_key()) or default_state()

    matches = await valve_get_match_history(limit=15)
    lines = [
        f"account_id={ACCOUNT_ID}",
        f"got_matches={len(matches)}",
        "last_15:",
    ]
    for m in matches:
        lines.append(f"- match_id={m.get('match_id')} start_time={m.get('start_time')} lobby_type={m.get('lobby_type')}")
    lines += [
        "",
        "state:",
        f"stream_active={st.get('stream_active')}",
        f"stream_start_time={st.get('stream_start_time')}",
        f"processed_ids_stream_count={len(st.get('processed_ids_stream') or [])}",
        f"pending_count={len(st.get('pending') or {})}",
        f"mmr={st.get('mmr')}",
    ]
    return "\n".join(lines)


@app.get("/debug_match", response_class=PlainTextResponse)
async def debug_match(token: str = Query(""), match_id: int = Query(...)):
    admin_guard(token)
    try:
        details = await valve_get_match_details(match_id)
        # show minimal
        radiant_win = details.get("radiant_win")
        players = details.get("players") or []
        slot = None
        for p in players:
            if int(p.get("account_id", -1)) == int(ACCOUNT_ID):
                slot = p.get("player_slot")
                break
        return f"ok\nradiant_win={radiant_win}\nmy_player_slot={slot}\nplayers={len(players)}"
    except httpx.HTTPStatusError as e:
        return f"details_error: status={e.response.status_code}\n{e.response.text[:500]}"
    except Exception as e:
        return f"details_error: {type(e).__name__}: {str(e)[:300]}"


@app.get("/stream_on", response_class=PlainTextResponse)
async def stream_on(token: str = Query("")):
    admin_guard(token)
    await set_stream_active(True, now_ts())
    return "ok: stream_active=True"


@app.get("/stream_off", response_class=PlainTextResponse)
async def stream_off(token: str = Query("")):
    admin_guard(token)
    st = await redis_get_json(state_key()) or default_state()
    st["stream_active"] = False
    await redis_set_json(state_key(), st)
    return "ok: stream_active=False"


@app.get("/reset_stream", response_class=PlainTextResponse)
async def reset_stream(token: str = Query("")):
    """
    Reset ONLY stream counters (win/lose/delta) and rebase stream_start_time=now, keep mmr.
    """
    admin_guard(token)
    st = await redis_get_json(state_key()) or default_state()
    st["stream_start_time"] = now_ts()
    st["stream_win"] = 0
    st["stream_lose"] = 0
    st["stream_delta"] = 0
    st["processed_ids_stream"] = []
    st["pending"] = {}
    st["last_errors"] = []
    st["stream_active"] = True
    await redis_set_json(state_key(), st)
    return "ok: reset_stream"


@app.get("/reset_all", response_class=PlainTextResponse)
async def reset_all(token: str = Query("")):
    """
    Full reset to START_MMR and clears everything.
    """
    admin_guard(token)
    st = default_state()
    await redis_set_json(state_key(), st)
    return "ok: reset_all"


# --- TEST endpoints (admin protected)
@app.get("/testwin", response_class=PlainTextResponse)
async def test_win(token: str = Query("")):
    admin_guard(token)
    st = await redis_get_json(state_key()) or default_state()
    st["mmr"] = int(st.get("mmr", START_MMR)) + MMR_STEP
    st["stream_win"] = int(st.get("stream_win", 0)) + 1
    st["stream_delta"] = int(st.get("stream_delta", 0)) + MMR_STEP
    await redis_set_json(state_key(), st)
    return "WIN added"


@app.get("/testlose", response_class=PlainTextResponse)
async def test_lose(token: str = Query("")):
    admin_guard(token)
    st = await redis_get_json(state_key()) or default_state()
    st["mmr"] = int(st.get("mmr", START_MMR)) - MMR_STEP
    st["stream_lose"] = int(st.get("stream_lose", 0)) + 1
    st["stream_delta"] = int(st.get("stream_delta", 0)) - MMR_STEP
    await redis_set_json(state_key(), st)
    return "LOSE added"


# -----------------------
# EVENTSUB SETUP + CALLBACK
# -----------------------
@app.get("/eventsub_setup", response_class=PlainTextResponse)
async def eventsub_setup(token: str = Query("")):
    admin_guard(token)

    if not EVENTSUB_SECRET:
        return "EVENTSUB_SECRET not set"
    if not TWITCH_BROADCASTER_LOGIN:
        return "TWITCH_BROADCASTER_LOGIN not set"
    if not TWITCH_CLIENT_ID or not TWITCH_CLIENT_SECRET:
        return "TWITCH_CLIENT_ID/SECRET not set"

    access = await twitch_app_token()
    broadcaster_id = await twitch_get_user_id(TWITCH_BROADCASTER_LOGIN, access)

    subs = await twitch_list_subs(access)
    have_online = any(s.get("type") == "stream.online" and (s.get("condition") or {}).get("broadcaster_user_id") == broadcaster_id for s in subs)
    have_offline = any(s.get("type") == "stream.offline" and (s.get("condition") or {}).get("broadcaster_user_id") == broadcaster_id for s in subs)

    if not have_online:
        await twitch_create_sub(access, broadcaster_id, "stream.online")
    if not have_offline:
        await twitch_create_sub(access, broadcaster_id, "stream.offline")

    return "\n".join([
        "ok",
        f"callback: {PUBLIC_BASE_URL}/eventsub",
        f"online: {'exists' if have_online else 'created'}",
        f"offline: {'exists' if have_offline else 'created'}",
    ])


@app.post("/eventsub", response_class=PlainTextResponse)
async def eventsub(request: Request):
    body = await request.body()

    # signature verify
    if not verify_eventsub_signature(request, body):
        raise HTTPException(status_code=403, detail="Invalid signature")

    msg_type = request.headers.get("Twitch-Eventsub-Message-Type", "")
    data = json.loads(body.decode("utf-8") or "{}")

    # verification
    if msg_type == "webhook_callback_verification":
        challenge = data.get("challenge", "")
        return challenge

    # notifications
    if msg_type == "notification":
        sub = data.get("subscription") or {}
        typ = sub.get("type", "")
        event = data.get("event") or {}

        if typ == "stream.online":
            # start stream now (use current server time)
            await set_stream_active(True, now_ts())
            return "ok"

        if typ == "stream.offline":
            st = await redis_get_json(state_key()) or default_state()
            st["stream_active"] = False
            await redis_set_json(state_key(), st)
            return "ok"

        return "ok"

    # revocation or others
    return "ok"

@app.get("/set_stream_start", response_class=PlainTextResponse)
async def set_stream_start(token: str = Query(""), ts: int = Query(0)):
    if token != ADMIN_TOKEN:
        return "Forbidden"

    state = await get_state()
    state["stream_active"] = True
    state["stream_start_time"] = ts
    state["processed_ids_stream"] = []
    await save_state(state)

    return f"stream_start_time set to {ts}"
