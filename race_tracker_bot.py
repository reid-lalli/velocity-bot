#!/usr/bin/env python3
"""
Vy Clan War Result Tracker — Discord Bot
=========================================
Automatically detects Lorenzi/Quaxly-format war results in Discord,
parses them, and logs everything to Google Sheets.

Sheets created:
  • War Log      — one row per war (totals + per-race nets)
  • Track Stats  — running net/avg/best/worst per track code
  • Race Details — one row per individual race

Commands (prefix !):
    !addwar [paste]     — manually log a war result (or reply to a Quaxly message)
        !change ...         — edit a war (supports warID)
    !update              — rebuild derived sheets from all logged wars
        !undo                — undo the last mutating command
        !redo                — redo the last undone command
    !trackstats [code|min_played]  — all tracks, one track (e.g. !trackstats rAF), or tracks played at least N times (e.g. !trackstats 3)
    !reactionmatch      — reconcile slot reactions to current Pacific hour (manual catch-up)
  !warstats [n]       — last n wars (default 5)
  !setupsheets        — (re)init sheet headers  [admin only]
"""

import os
import re
import asyncio
import time
import json
import copy
import threading
from collections import deque
from datetime import datetime
from typing import Optional, Dict, Any, AsyncIterator, cast
from zoneinfo import ZoneInfo

import discord
from discord.ext import commands, tasks
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

load_dotenv()

# ── Configuration ──────────────────────────────────────────────────────────────
DISCORD_TOKEN    = os.getenv("DISCORD_TOKEN", "")
SPREADSHEET_ID   = os.getenv("SPREADSHEET_ID", "")
CREDENTIALS_FILE = os.getenv("CREDENTIALS_FILE", "credentials.json")
CLAN_NAME        = os.getenv("CLAN_NAME", "Vy")
AUTO_LOG_RESULTS = os.getenv("AUTO_LOG_RESULTS", "true").strip().lower() in ("1", "true", "yes", "on")

SHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

UNDO_META_SHEET = "_UNDO_META"
UNDO_PREFIX = "_UNDO_"
REDO_META_SHEET = "_REDO_META"
REDO_PREFIX = "_REDO_"

# Active in-memory war sessions keyed by channel ID.
ACTIVE_WARS: Dict[int, Dict[str, Any]] = {}

# 6v6 points table used by Quaxly-style net calculations.
PLACE_POINTS = {
    1: 15, 2: 12, 3: 10, 4: 9, 5: 8, 6: 7,
    7: 6, 8: 5, 9: 4, 10: 3, 11: 2, 12: 1,
}

# ── War Result Parser ──────────────────────────────────────────────────────────
# Matches individual race lines, e.g.:
#   " 1:  +6 | 1,3,4,7,10,12   (rAF)"
RACE_LINE_RE = re.compile(
    r"^\s*(\d+):\s*([-+]?\d+)\s*\|\s*([\d,\s]+)\((\w+)\)",
    re.MULTILINE,
)
HEADER_RE = re.compile(r"Total Score after Race\s+(\d+)", re.IGNORECASE)
DIFF_RE   = re.compile(r"Difference\s*\n\s*([-+]?\d+)", re.IGNORECASE)
TIME_SLOT_RE = re.compile(r"\b(1[0-2]|0?[1-9]):00\s*([AP]M)\b", re.IGNORECASE)
DISCORD_MESSAGE_LINK_RE = re.compile(
    r"https?://(?:ptb\.|canary\.)?discord(?:app)?\.com/channels/\d+/(\d+)/(\d+)",
    re.IGNORECASE,
)

PT_TZ = ZoneInfo("America/Los_Angeles")
REACTION_EMOJIS = ["✅", "❓", "❌"]

VALID_TRACKS = {
    "rDDJ", "SSS", "rAF", "rSHS", "rDH", "rKTB", "MBC", "BC",
    "rSGB", "GBR", "rWS", "rPB", "rWSh", "SP", "rTF", "CC",
    "CCF", "WS", "AH", "rMMM", "DD", "rCM", "RR", "PS", "rMC",
    "FO", "DKS", "BCi", "rDKP", "DBB",
}
VALID_TRACK_MAP = {track.lower(): track for track in VALID_TRACKS}

REACTION_SLOTS_FILE = "reaction_slots.json"
REACTION_CONFIG_FILE = "reaction_schedule_config.json"
DEFAULT_REACT_START_HOUR = 9
DEFAULT_REACT_END_HOUR = 22


def _canonical_track_code(track: str) -> Optional[str]:
    """Return canonical track code for case-insensitive input, else None."""
    normalized = (track or "").strip().lower()
    if not normalized:
        return None
    return VALID_TRACK_MAP.get(normalized)


def _reaction_slots_path() -> str:
    return os.path.join(os.path.dirname(__file__), REACTION_SLOTS_FILE)


def _reaction_config_path() -> str:
    return os.path.join(os.path.dirname(__file__), REACTION_CONFIG_FILE)


def load_reaction_slots() -> list[dict[str, int]]:
    """Load tracked time-slot messages for reaction automation."""
    path = _reaction_slots_path()
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return []

    slots: list[dict[str, int]] = []

    def _coerce_int(value) -> Optional[int]:
        try:
            return int(str(value).strip())
        except (TypeError, ValueError):
            return None

    for row in data if isinstance(data, list) else []:
        if not isinstance(row, dict):
            continue
        channel_id = _coerce_int(row.get("channel_id"))
        message_id = _coerce_int(row.get("message_id"))
        hour = _coerce_int(row.get("hour"))
        if channel_id is None or message_id is None or hour is None:
            continue
        if hour < 0 or hour > 23:
            continue
        slots.append({"channel_id": channel_id, "message_id": message_id, "hour": hour})
    return slots


def save_reaction_slots(slots: list[dict[str, int]]):
    """Persist tracked time-slot messages for reaction automation."""
    path = _reaction_slots_path()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(slots, f, indent=2)


def load_reaction_config() -> dict[str, Any]:
    """Load scheduler config for bot-posted daily time-slot messages."""
    defaults = {
        "channel_id": None,
        "start_hour": DEFAULT_REACT_START_HOUR,
        "end_hour": DEFAULT_REACT_END_HOUR,
        "last_posted_date": "",
    }

    path = _reaction_config_path()
    if not os.path.exists(path):
        return defaults

    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except (OSError, json.JSONDecodeError):
        return defaults

    if not isinstance(raw, dict):
        return defaults

    channel_id = raw.get("channel_id")
    try:
        channel_id = int(str(channel_id).strip()) if channel_id not in (None, "") else None
    except (TypeError, ValueError):
        channel_id = None

    try:
        start_hour = int(str(raw.get("start_hour", DEFAULT_REACT_START_HOUR)).strip())
    except (TypeError, ValueError):
        start_hour = DEFAULT_REACT_START_HOUR

    try:
        end_hour = int(str(raw.get("end_hour", DEFAULT_REACT_END_HOUR)).strip())
    except (TypeError, ValueError):
        end_hour = DEFAULT_REACT_END_HOUR

    if start_hour < 0 or start_hour > 23 or end_hour < 0 or end_hour > 23 or start_hour > end_hour:
        start_hour = DEFAULT_REACT_START_HOUR
        end_hour = DEFAULT_REACT_END_HOUR

    last_posted_date = raw.get("last_posted_date", "")
    if not isinstance(last_posted_date, str):
        last_posted_date = ""

    return {
        "channel_id": channel_id,
        "start_hour": start_hour,
        "end_hour": end_hour,
        "last_posted_date": last_posted_date,
    }


def save_reaction_config(config: dict[str, Any]):
    """Persist scheduler config for bot-posted daily time-slot messages."""
    path = _reaction_config_path()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2)


REACTION_SLOTS: list[dict[str, int]] = load_reaction_slots()
REACTION_CONFIG: dict[str, Any] = load_reaction_config()
_REACTION_SCHEDULER_LOCK = asyncio.Lock()
_LAST_MIDNIGHT_REACT_DATE: Optional[str] = None
_LAST_CLEANUP_HOUR_KEY: Optional[str] = None
_RUNTIME_UNDO_SNAPSHOT: Optional[dict[str, Any]] = None
_RUNTIME_REDO_SNAPSHOT: Optional[dict[str, Any]] = None
_LAST_MUTATION_SCOPE: Optional[str] = None
_LAST_MUTATION_ACTION: Optional[str] = None
_LAST_UNDONE_SCOPE: Optional[str] = None
_LAST_UNDONE_ACTION: Optional[str] = None


def _capture_runtime_state() -> dict[str, Any]:
    """Capture bot runtime state for one-step undo/redo of non-sheets commands."""
    return {
        "active_wars": copy.deepcopy(ACTIVE_WARS),
        "reaction_slots": copy.deepcopy(REACTION_SLOTS),
        "reaction_config": copy.deepcopy(REACTION_CONFIG),
    }


def _restore_runtime_state(snapshot: dict[str, Any]):
    """Restore bot runtime state and persist reaction config/slots."""
    global ACTIVE_WARS
    global REACTION_SLOTS
    global REACTION_CONFIG

    ACTIVE_WARS = copy.deepcopy(snapshot.get("active_wars", {}))
    REACTION_SLOTS = copy.deepcopy(snapshot.get("reaction_slots", []))
    REACTION_CONFIG = copy.deepcopy(snapshot.get("reaction_config", load_reaction_config()))
    save_reaction_slots(REACTION_SLOTS)
    save_reaction_config(REACTION_CONFIG)


def _mark_runtime_mutation(action: str):
    """Record a runtime mutation so !undo can reverse it."""
    global _RUNTIME_UNDO_SNAPSHOT
    global _RUNTIME_REDO_SNAPSHOT
    global _LAST_MUTATION_SCOPE
    global _LAST_MUTATION_ACTION
    global _LAST_UNDONE_SCOPE
    global _LAST_UNDONE_ACTION

    _RUNTIME_UNDO_SNAPSHOT = _capture_runtime_state()
    _RUNTIME_REDO_SNAPSHOT = None
    _LAST_MUTATION_SCOPE = "runtime"
    _LAST_MUTATION_ACTION = action
    _LAST_UNDONE_SCOPE = None
    _LAST_UNDONE_ACTION = None


def _mark_sheets_mutation(action: str):
    """Record a sheets mutation so !undo can reverse it."""
    global _LAST_MUTATION_SCOPE
    global _LAST_MUTATION_ACTION
    global _LAST_UNDONE_SCOPE
    global _LAST_UNDONE_ACTION

    _LAST_MUTATION_SCOPE = "sheets"
    _LAST_MUTATION_ACTION = action
    _LAST_UNDONE_SCOPE = None
    _LAST_UNDONE_ACTION = None


def _parse_score_block(text: str) -> tuple[Optional[str], Optional[int], Optional[int]]:
    """Extract opponent name and both team scores from the summary block."""
    header_match = HEADER_RE.search(text)
    diff_match = re.search(r"\n\s*Difference\b", text, re.IGNORECASE)
    if not header_match or not diff_match:
        return None, None, None

    summary_block = text[header_match.end():diff_match.start()]
    summary_lines = [line.strip() for line in summary_block.splitlines() if line.strip()]

    team_tokens = []
    score_tokens = []
    for line in summary_lines:
        if re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]*", line):
            team_tokens.append(line)
        elif re.fullmatch(r"\d+", line):
            score_tokens.append(int(line))
        else:
            for token in re.findall(r"[A-Za-z][A-Za-z0-9_-]*|\d+", line):
                if token.isdigit():
                    score_tokens.append(int(token))
                else:
                    team_tokens.append(token)

    if len(team_tokens) < 2 or len(score_tokens) < 2:
        return None, None, None

    team_a, team_b = team_tokens[0], team_tokens[1]
    score_a, score_b = score_tokens[0], score_tokens[1]

    if team_a.lower() == CLAN_NAME.lower():
        return team_b.upper(), score_a, score_b
    if team_b.lower() == CLAN_NAME.lower():
        return team_a.upper(), score_b, score_a

    # If clan name is not present, honor the user's convention: first team/score is Vy, second is opponent.
    return team_b.upper(), score_a, score_b


def parse_war(text: str) -> Optional[dict]:
    """
    Parse a Lorenzi/Quaxly-style war result block.
    Returns a dict with war data, or None if the text is not a war result.
    """
    if "Total Score after Race" not in text:
        return None

    races = []
    for m in RACE_LINE_RE.finditer(text):
        positions = [int(p.strip()) for p in m.group(3).split(",") if p.strip().isdigit()]
        raw_track = m.group(4)
        canonical_track = _canonical_track_code(raw_track)
        races.append({
            "race":      int(m.group(1)),
            "net":       int(m.group(2)),
            "positions": positions,
            "track":     canonical_track or raw_track,
        })

    if not races:
        return None

    hm = HEADER_RE.search(text)
    dm = DIFF_RE.search(text)

    opponent, vy_score, opp_score = _parse_score_block(text)

    return {
        "date":       datetime.now().strftime("%Y-%m-%d %H:%M"),
        "opponent":   opponent or "Unknown",
        "vy_score":   vy_score,
        "opp_score":  opp_score,
        "difference": int(dm.group(1)) if dm else None,
        "num_races":  int(hm.group(1)) if hm else len(races),
        "races":      sorted(races, key=lambda r: r["race"]),
    }


def _sanitize_timeslot_text(text: str) -> str:
    """Normalize message text for resilient time-slot matching."""
    content = (text or "").strip()
    # Remove common Discord markdown wrappers from slot-only messages.
    if content.startswith("`") and content.endswith("`"):
        content = content.strip("`").strip()
    content = content.replace("**", "").replace("__", "").replace("*", "")
    content = content.replace("\u200b", "").replace("\xa0", " ").strip()
    return content


def _parse_timeslot_hour(text: str) -> Optional[int]:
    """Parse texts containing slot labels like '9:00 AM' into 24-hour hour."""
    content = _sanitize_timeslot_text(text)
    match = TIME_SLOT_RE.search(content)
    if not match:
        return None

    hour_12 = int(match.group(1))
    meridiem = match.group(2).upper()

    if meridiem == "AM":
        return 0 if hour_12 == 12 else hour_12
    return 12 if hour_12 == 12 else hour_12 + 12


def _parse_timeslot_hour_from_message(message: discord.Message) -> Optional[int]:
    """Extract slot hour from message body and embed text."""
    candidates: list[str] = []

    if message.content:
        candidates.extend([line for line in message.content.splitlines() if line.strip()])
        candidates.append(message.content)

    for embed in message.embeds:
        if embed.title:
            candidates.append(embed.title)
        if embed.description:
            candidates.extend([line for line in embed.description.splitlines() if line.strip()])
            candidates.append(embed.description)
        for field in embed.fields:
            if field.name:
                candidates.append(field.name)
            if field.value:
                candidates.extend([line for line in field.value.splitlines() if line.strip()])
                candidates.append(field.value)

    for candidate in candidates:
        hour = _parse_timeslot_hour(candidate)
        if hour is not None:
            return hour
    return None


async def _discover_channel_reaction_slots(channel: discord.abc.Messageable, limit: int = 500) -> list[dict[str, int]]:
    """Discover hour-slot messages in a channel and return sorted unique slot mapping."""
    discovered: dict[int, dict[str, int]] = {}
    async for message in channel.history(limit=limit):
        hour = _parse_timeslot_hour_from_message(message)
        if hour is None:
            continue
        # Prefer most recent message for each hour slot.
        discovered[hour] = {
            "channel_id": message.channel.id,
            "message_id": message.id,
            "hour": hour,
        }
    return [discovered[h] for h in sorted(discovered.keys())]


def _extract_message_links(text: str) -> list[tuple[int, int]]:
    """Extract (channel_id, message_id) pairs from Discord message links."""
    pairs: list[tuple[int, int]] = []
    for channel_id_str, message_id_str in DISCORD_MESSAGE_LINK_RE.findall(text or ""):
        try:
            pairs.append((int(channel_id_str), int(message_id_str)))
        except ValueError:
            continue
    return pairs


async def _fetch_message_by_ids(channel_id: int, message_id: int) -> Optional[discord.Message]:
    """Fetch a message by explicit channel/message IDs."""
    channel = bot.get_channel(channel_id)
    if channel is None:
        try:
            channel = await bot.fetch_channel(channel_id)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            return None

    fetch_message = getattr(channel, "fetch_message", None)
    if not callable(fetch_message):
        return None

    try:
        result = fetch_message(message_id)
        if asyncio.iscoroutine(result):
            return await result
        return None
    except (discord.NotFound, discord.Forbidden, discord.HTTPException):
        return None


def _hour_to_label(hour: int) -> str:
    """Convert 24-hour int to labels like '9:00 AM'."""
    suffix = "AM" if hour < 12 else "PM"
    hour12 = hour % 12
    if hour12 == 0:
        hour12 = 12
    return f"{hour12}:00 {suffix}"


async def _get_channel_by_id(channel_id: int):
    """Resolve a Discord channel by ID from cache or API."""
    channel = bot.get_channel(channel_id)
    if channel is not None:
        return channel
    try:
        return await bot.fetch_channel(channel_id)
    except (discord.NotFound, discord.Forbidden, discord.HTTPException):
        return None


async def _post_daily_reaction_slots(now_pt: datetime) -> int:
    """Post today's Unix hour-slot messages in configured channel and seed reactions."""
    global REACTION_SLOTS
    global REACTION_CONFIG

    channel_id = REACTION_CONFIG.get("channel_id")
    if not isinstance(channel_id, int):
        return 0

    channel = await _get_channel_by_id(channel_id)
    if channel is None:
        return 0

    send_fn = getattr(channel, "send", None)
    if not callable(send_fn):
        return 0

    start_hour = int(REACTION_CONFIG.get("start_hour", DEFAULT_REACT_START_HOUR))
    end_hour = int(REACTION_CONFIG.get("end_hour", DEFAULT_REACT_END_HOUR))

    slots: list[dict[str, int]] = []
    posted = 0
    for hour in range(start_hour, end_hour + 1):
        slot_dt = datetime(now_pt.year, now_pt.month, now_pt.day, hour, 0, tzinfo=PT_TZ)
        unix_ts = int(slot_dt.timestamp())
        text = f"<t:{unix_ts}:t>"

        try:
            send_result = send_fn(text)
            if not asyncio.iscoroutine(send_result):
                continue
            message = await send_result
        except (discord.Forbidden, discord.HTTPException):
            continue

        await _add_default_reactions(message)
        slots.append({
            "channel_id": message.channel.id,
            "message_id": message.id,
            "hour": hour,
        })
        posted += 1

    REACTION_SLOTS = slots
    save_reaction_slots(REACTION_SLOTS)

    if posted > 0:
        REACTION_CONFIG["last_posted_date"] = now_pt.strftime("%Y-%m-%d")
    save_reaction_config(REACTION_CONFIG)
    return posted


async def _delete_tracked_slot_messages(channel_id: Optional[int] = None) -> tuple[int, int, int]:
    """Delete tracked slot messages (optionally scoped to one channel) and prune slot state."""
    global REACTION_SLOTS

    if channel_id is None:
        target_slots = list(REACTION_SLOTS)
        remaining_slots: list[dict[str, int]] = []
    else:
        target_slots = [slot for slot in REACTION_SLOTS if slot.get("channel_id") == channel_id]
        remaining_slots = [slot for slot in REACTION_SLOTS if slot.get("channel_id") != channel_id]

    deleted = 0
    failed = 0
    for slot in target_slots:
        msg = await _fetch_message_from_slot(slot)
        if msg is None:
            failed += 1
            continue

        delete_fn = getattr(msg, "delete", None)
        if not callable(delete_fn):
            failed += 1
            continue

        try:
            result = delete_fn()
            if asyncio.iscoroutine(result):
                await result
            deleted += 1
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            failed += 1

    REACTION_SLOTS = remaining_slots
    save_reaction_slots(REACTION_SLOTS)
    return deleted, failed, len(target_slots)


async def _delete_previous_day_slot_messages(channel_id: int, now_pt: datetime, limit: int = 500) -> tuple[int, int]:
    """Best-effort cleanup for old bot-posted slot messages from prior Pacific dates."""
    channel = await _get_channel_by_id(channel_id)
    if channel is None or bot.user is None:
        return 0, 0

    history_fn = getattr(channel, "history", None)
    if not callable(history_fn):
        return 0, 0

    deleted = 0
    failed = 0
    today_pt = now_pt.date()

    history_iter = history_fn(limit=limit)
    if not hasattr(history_iter, "__aiter__"):
        return 0, 0

    for_delete_iter = cast(AsyncIterator[discord.Message], history_iter)
    async for message in for_delete_iter:
        # Only touch messages posted by this bot.
        if message.author.id != bot.user.id:
            continue

        # Only touch messages that look like scheduler slot posts.
        if _parse_timeslot_hour_from_message(message) is None:
            continue

        message_date_pt = message.created_at.astimezone(PT_TZ).date()
        if message_date_pt >= today_pt:
            continue

        try:
            result = message.delete()
            if asyncio.iscoroutine(result):
                await result
            deleted += 1
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            failed += 1

    return deleted, failed


async def _delete_bot_messages_in_channel(channel_id: int, limit: int = 2000) -> tuple[int, int]:
    """Delete recent messages authored by this bot in one channel."""
    channel = await _get_channel_by_id(channel_id)
    if channel is None or bot.user is None:
        return 0, 0

    history_fn = getattr(channel, "history", None)
    if not callable(history_fn):
        return 0, 0

    history_iter = history_fn(limit=limit)
    if not hasattr(history_iter, "__aiter__"):
        return 0, 0

    deleted = 0
    failed = 0
    for_delete_iter = cast(AsyncIterator[discord.Message], history_iter)
    async for message in for_delete_iter:
        if message.author.id != bot.user.id:
            continue
        try:
            result = message.delete()
            if asyncio.iscoroutine(result):
                await result
            deleted += 1
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            failed += 1

    return deleted, failed


async def _ensure_daily_reaction_slots(now_pt: datetime) -> int:
    """Ensure today's slot messages exist; post a new daily set when needed."""
    # Serialize scheduler and command-triggered posting to prevent duplicate daily posts.
    async with _REACTION_SCHEDULER_LOCK:
        date_key = now_pt.strftime("%Y-%m-%d")
        channel_id = REACTION_CONFIG.get("channel_id")
        if not isinstance(channel_id, int):
            return 0

        if REACTION_CONFIG.get("last_posted_date") == date_key and REACTION_SLOTS:
            return 0

        # New day detected: clear prior tracked slot messages before posting today's set.
        if REACTION_SLOTS:
            await _delete_tracked_slot_messages(channel_id)

        # Fallback sweep: remove any older bot slot messages still in channel history.
        await _delete_previous_day_slot_messages(channel_id, now_pt)

        return await _post_daily_reaction_slots(now_pt)


async def _fetch_message_from_slot(slot: dict[str, int], strict: bool = False) -> Optional[Any]:
    """Resolve a tracked slot message. strict=True verifies existence via fetch."""
    channel = bot.get_channel(slot["channel_id"])
    if channel is None:
        try:
            channel = await bot.fetch_channel(slot["channel_id"])
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            return None

    if strict:
        fetch_message = getattr(channel, "fetch_message", None)
        if not callable(fetch_message):
            return None
        try:
            result = fetch_message(slot["message_id"])
            if asyncio.iscoroutine(result):
                return await result
            return None
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            return None

    # Prefer partial messages to avoid an extra GET /messages/{id} request.
    get_partial = getattr(channel, "get_partial_message", None)
    if callable(get_partial):
        try:
            return get_partial(slot["message_id"])
        except Exception:
            pass

    fetch_message = getattr(channel, "fetch_message", None)
    if not callable(fetch_message):
        return None

    try:
        result = fetch_message(slot["message_id"])
        if asyncio.iscoroutine(result):
            return await result
        return None
    except (discord.NotFound, discord.Forbidden, discord.HTTPException):
        return None


async def _add_default_reactions(message) -> int:
    """Add scheduler reactions to one message. Returns successful adds count."""
    added = 0
    for emoji in REACTION_EMOJIS:
        try:
            await message.add_reaction(emoji)
            added += 1
        except (discord.Forbidden, discord.HTTPException):
            continue
    return added


async def _clear_default_reactions(message) -> int:
    """Clear scheduler reactions from one message. Returns successful clears count."""
    cleared = 0
    for emoji in REACTION_EMOJIS:
        try:
            await message.clear_reaction(emoji)
            cleared += 1
            continue
        except (discord.Forbidden, discord.HTTPException):
            pass

        # Fallback when lacking manage messages: remove only bot's own reaction.
        if bot.user is None:
            continue
        try:
            await message.remove_reaction(emoji, bot.user)
            cleared += 1
        except (discord.Forbidden, discord.HTTPException, TypeError):
            continue
    return cleared


async def _run_daily_reaction_seed():
    """Ensure today's bot-posted Pacific time-slot messages exist."""
    await _ensure_daily_reaction_slots(datetime.now(PT_TZ))


async def _run_hourly_reaction_cleanup(now_pt: datetime):
    """After each hour, clear reactions from slots whose hour already passed."""
    current_hour = now_pt.hour
    for slot in REACTION_SLOTS:
        slot_hour = slot.get("hour")
        if not isinstance(slot_hour, int):
            continue
        if slot_hour >= current_hour:
            continue
        msg = await _fetch_message_from_slot(slot)
        if msg is None:
            continue
        _ = await _clear_default_reactions(msg)


@tasks.loop(minutes=1)
async def reaction_schedule_loop():
    """Drive daily reaction creation and hourly cleanup using Pacific time."""
    global _LAST_MIDNIGHT_REACT_DATE
    global _LAST_CLEANUP_HOUR_KEY

    if not isinstance(REACTION_CONFIG.get("channel_id"), int):
        return

    now_pt = datetime.now(PT_TZ)
    await _ensure_daily_reaction_slots(now_pt)

    if now_pt.hour == 0 and now_pt.minute == 0:
        date_key = now_pt.strftime("%Y-%m-%d")
        if _LAST_MIDNIGHT_REACT_DATE != date_key:
            await _run_daily_reaction_seed()
            _LAST_MIDNIGHT_REACT_DATE = date_key

    if now_pt.minute == 0:
        hour_key = now_pt.strftime("%Y-%m-%d-%H")
        if _LAST_CLEANUP_HOUR_KEY != hour_key:
            await _run_hourly_reaction_cleanup(now_pt)
            _LAST_CLEANUP_HOUR_KEY = hour_key


@reaction_schedule_loop.before_loop
async def before_reaction_schedule_loop():
    await bot.wait_until_ready()


# ── Google Sheets ──────────────────────────────────────────────────────────────

# Rate limiter to stay under Google Sheets API quota (60 read requests/min/user).
class _SheetsRateLimiter:
    """Sliding-window rate limiter for Google Sheets API calls."""

    def __init__(self, max_calls: int = 50, window_secs: float = 60.0):
        self._max_calls = max_calls
        self._window = window_secs
        self._timestamps: deque[float] = deque()
        self._lock = threading.Lock()

    def wait(self):
        """Block until an API call can be made within the rate limit."""
        with self._lock:
            now = time.time()
            while self._timestamps and self._timestamps[0] <= now - self._window:
                self._timestamps.popleft()
            if len(self._timestamps) >= self._max_calls:
                sleep_time = self._timestamps[0] + self._window - now + 0.1
                if sleep_time > 0:
                    time.sleep(sleep_time)
                now = time.time()
                while self._timestamps and self._timestamps[0] <= now - self._window:
                    self._timestamps.popleft()
            self._timestamps.append(time.time())


_sheets_limiter = _SheetsRateLimiter()

_THROTTLED_WS_METHODS = frozenset({
    'row_values', 'col_values', 'get_all_values', 'get_all_records',
    'append_row', 'append_rows', 'update', 'update_acell', 'update_cell',
    'batch_clear', 'batch_update', 'format', 'batch_format',
    'add_rows', 'add_cols', 'resize',
    'delete_rows', 'delete_columns',
})


class _ThrottledWorksheet:
    """Proxy that rate-limits gspread Worksheet API calls."""

    def __init__(self, ws):
        object.__setattr__(self, '_ws', ws)

    def __getattr__(self, name) -> Any:
        attr = getattr(self._ws, name)
        if callable(attr) and name in _THROTTLED_WS_METHODS:
            def wrapper(*args, **kwargs):
                _sheets_limiter.wait()
                return attr(*args, **kwargs)
            return wrapper
        return attr

    def __setattr__(self, name, value):
        setattr(self._ws, name, value)


class _ThrottledSpreadsheet:
    """Proxy that rate-limits gspread Spreadsheet worksheet lookups."""

    def __init__(self, ss):
        object.__setattr__(self, '_ss', ss)

    def __getattr__(self, name) -> Any:
        attr = getattr(self._ss, name)
        if name in ('worksheet', 'add_worksheet'):
            def wrapper(*args, **kwargs):
                _sheets_limiter.wait()
                return _ThrottledWorksheet(attr(*args, **kwargs))
            return wrapper
        return attr


def get_spreadsheet():
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SHEETS_SCOPES)
    gc = gspread.authorize(creds)
    return _ThrottledSpreadsheet(gc.open_by_key(SPREADSHEET_ID))


def _is_retryable_sheets_error(exc: Exception) -> bool:
    """Return True if the Sheets API error should be retried."""
    if not isinstance(exc, gspread.exceptions.APIError):
        return False

    status_code = getattr(getattr(exc, "response", None), "status_code", None)
    if status_code in (429, 500, 503):
        return True

    msg = str(exc).lower()
    return "quota exceeded" in msg or "rate limit" in msg or "[429]" in msg


def run_sheets_with_retry(func, *args, max_attempts: int = 6, base_delay: float = 1.2, **kwargs):
    """Execute a Sheets operation with exponential backoff on API rate limits."""
    for attempt in range(1, max_attempts + 1):
        try:
            return func(*args, **kwargs)
        except Exception as exc:
            if not _is_retryable_sheets_error(exc) or attempt >= max_attempts:
                raise
            delay = base_delay * (2 ** (attempt - 1))
            time.sleep(delay)


def _get_or_create_ws(spreadsheet, name: str, rows=1000, cols=30) -> gspread.Worksheet:
    try:
        return spreadsheet.worksheet(name)
    except gspread.WorksheetNotFound:
        return spreadsheet.add_worksheet(title=name, rows=rows, cols=cols)


def _get_net_tracker_ws(spreadsheet) -> gspread.Worksheet:
    """Return the active net tracker worksheet (Sheet1 preferred, fallback Net Tracker)."""
    for sheet_name in ("Sheet1", "Net Tracker"):
        try:
            return spreadsheet.worksheet(sheet_name)
        except gspread.WorksheetNotFound:
            continue
    ws = spreadsheet.add_worksheet(title="Net Tracker", rows=1000, cols=4)
    ws.append_row(["Date", "Opponent", "Net", "War ID"])
    ws.format("A1:D1", {"textFormat": {"bold": True}})
    return ws


def _column_letter(column_number: int) -> str:
    """Convert a 1-based column number to A1 notation letters."""
    letters = ""
    while column_number > 0:
        column_number, remainder = divmod(column_number - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters


def _clear_data_rows(worksheet: gspread.Worksheet):
    """Clear all worksheet rows below the header row."""
    if worksheet.row_count <= 1:
        return
    worksheet.batch_clear([f"A2:{_column_letter(worksheet.col_count)}{worksheet.row_count}"])


def _ensure_size(worksheet: gspread.Worksheet, rows: int, cols: int):
    """Grow worksheet grid to at least rows x cols."""
    if rows > worksheet.row_count:
        worksheet.add_rows(rows - worksheet.row_count)
    if cols > worksheet.col_count:
        worksheet.add_cols(cols - worksheet.col_count)


def _copy_sheet_all_values(src: gspread.Worksheet, dst: gspread.Worksheet):
    """Copy all cell values from src worksheet into dst worksheet."""
    values = src.get_all_values()
    if not values:
        _clear_data_rows(dst)
        return

    rows = len(values)
    cols = max(len(r) for r in values)
    normalized = [r + [""] * (cols - len(r)) for r in values]
    _ensure_size(dst, rows, cols)
    dst.batch_clear([f"A1:{_column_letter(dst.col_count)}{dst.row_count}"])
    dst.update(range_name=f"A1:{_column_letter(cols)}{rows}", values=normalized)


def _get_or_create_undo_ws(spreadsheet, source_title: str, rows=1000, cols=30) -> gspread.Worksheet:
    """Get/create a backup worksheet for a source worksheet title."""
    safe_name = f"{UNDO_PREFIX}{source_title.replace(' ', '_')}"
    return _get_or_create_ws(spreadsheet, safe_name, rows=rows, cols=cols)


def _get_or_create_redo_ws(spreadsheet, source_title: str, rows=1000, cols=30) -> gspread.Worksheet:
    """Get/create a redo worksheet for a source worksheet title."""
    safe_name = f"{REDO_PREFIX}{source_title.replace(' ', '_')}"
    return _get_or_create_ws(spreadsheet, safe_name, rows=rows, cols=cols)


def backup_state(spreadsheet, action: str):
    """Create a one-step backup snapshot used by !undo."""
    (
        war_log,
        track_stats,
        race_details,
        war_track_summary,
        net_tracker,
    ) = setup_headers(spreadsheet)
    sources = [war_log, track_stats, race_details, war_track_summary, net_tracker]

    for src in sources:
        dst = _get_or_create_undo_ws(spreadsheet, src.title, rows=max(1000, src.row_count), cols=max(30, src.col_count))
        _copy_sheet_all_values(src, dst)

    meta = _get_or_create_ws(spreadsheet, UNDO_META_SHEET, rows=20, cols=6)
    meta.batch_clear([f"A1:{_column_letter(meta.col_count)}{meta.row_count}"])
    meta.update(
        range_name="A1:D2",
        values=[
            ["timestamp", "action", "net_tracker_title", "sources_json"],
            [datetime.now().strftime("%Y-%m-%d %H:%M:%S"), action, net_tracker.title, json.dumps([s.title for s in sources])],
        ],
    )

    # New mutating actions invalidate prior redo state.
    redo_meta = _get_or_create_ws(spreadsheet, REDO_META_SHEET, rows=20, cols=6)
    redo_meta.batch_clear([f"A1:{_column_letter(redo_meta.col_count)}{redo_meta.row_count}"])


def backup_redo_state(spreadsheet, action: str):
    """Create a one-step backup snapshot used by !redo after !undo."""
    (
        war_log,
        track_stats,
        race_details,
        war_track_summary,
        net_tracker,
    ) = setup_headers(spreadsheet)
    sources = [war_log, track_stats, race_details, war_track_summary, net_tracker]

    for src in sources:
        dst = _get_or_create_redo_ws(spreadsheet, src.title, rows=max(1000, src.row_count), cols=max(30, src.col_count))
        _copy_sheet_all_values(src, dst)

    meta = _get_or_create_ws(spreadsheet, REDO_META_SHEET, rows=20, cols=6)
    meta.batch_clear([f"A1:{_column_letter(meta.col_count)}{meta.row_count}"])
    meta.update(
        range_name="A1:D2",
        values=[
            ["timestamp", "action", "net_tracker_title", "sources_json"],
            [datetime.now().strftime("%Y-%m-%d %H:%M:%S"), action, net_tracker.title, json.dumps([s.title for s in sources])],
        ],
    )


def restore_state_from_backup(spreadsheet) -> tuple[bool, str]:
    """Restore sheets from one-step backup. Returns (ok, message)."""
    try:
        meta = spreadsheet.worksheet(UNDO_META_SHEET)
    except gspread.WorksheetNotFound:
        return False, "No undo snapshot found yet."

    meta_rows = meta.get_all_values()
    if len(meta_rows) < 2 or len(meta_rows[1]) < 4:
        return False, "Undo snapshot is empty."

    action = meta_rows[1][1]
    source_titles = json.loads(meta_rows[1][3]) if meta_rows[1][3] else []
    if not source_titles:
        return False, "Undo snapshot has no source sheet list."

    for source_title in source_titles:
        backup_ws = _get_or_create_undo_ws(spreadsheet, source_title)
        target_ws = _get_or_create_ws(spreadsheet, source_title, rows=max(1000, backup_ws.row_count), cols=max(30, backup_ws.col_count))
        _copy_sheet_all_values(backup_ws, target_ws)

    return True, action


def restore_state_from_redo(spreadsheet) -> tuple[bool, str]:
    """Restore sheets from one-step redo snapshot. Returns (ok, message)."""
    try:
        meta = spreadsheet.worksheet(REDO_META_SHEET)
    except gspread.WorksheetNotFound:
        return False, "No redo snapshot found yet."

    meta_rows = meta.get_all_values()
    if len(meta_rows) < 2 or len(meta_rows[1]) < 4:
        return False, "Redo snapshot is empty."

    action = meta_rows[1][1]
    source_titles = json.loads(meta_rows[1][3]) if meta_rows[1][3] else []
    if not source_titles:
        return False, "Redo snapshot has no source sheet list."

    for source_title in source_titles:
        backup_ws = _get_or_create_redo_ws(spreadsheet, source_title)
        target_ws = _get_or_create_ws(spreadsheet, source_title, rows=max(1000, backup_ws.row_count), cols=max(30, backup_ws.col_count))
        _copy_sheet_all_values(backup_ws, target_ws)

    return True, action


def _ensure_header_column(worksheet: gspread.Worksheet, header_name: str) -> int:
    """Ensure a header exists and return its 1-based column index."""
    headers = worksheet.row_values(1)
    if header_name in headers:
        return headers.index(header_name) + 1
    new_column = len(headers) + 1
    if new_column > worksheet.col_count:
        worksheet.add_cols(new_column - worksheet.col_count)
    worksheet.update_acell(f"{_column_letter(new_column)}1", header_name)
    return new_column


def _to_int(value) -> Optional[int]:
    """Best-effort integer conversion for sheet values."""
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _is_datetime_like(value) -> bool:
    """Return True for values like 'YYYY-MM-DD HH:MM'."""
    if value in (None, ""):
        return False
    return bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}\s+\d{1,2}:\d{2}", str(value).strip()))


def _war_from_log_row(record: dict) -> dict:
    """Reconstruct war data from a War Log record."""
    races = []
    for race_number in range(1, 13):
        net = _to_int(record.get(f"R{race_number} Net"))
        raw_track = str(record.get(f"R{race_number} Track", "")).strip()
        track = _canonical_track_code(raw_track) or raw_track
        if net is None or not track:
            continue
        races.append({
            "race": race_number,
            "net": net,
            "positions": [],
            "track": track,
        })

    return {
        "war_id": _to_int(record.get("War ID")),
        "date": str(record.get("Date", "")),
        "opponent": str(record.get("Opponent", "Unknown")),
        "vy_score": _to_int(record.get(f"{CLAN_NAME} Score")),
        "opp_score": _to_int(record.get("Opp Score")),
        "difference": _to_int(record.get("Net")),
        "num_races": len(races),
        "races": races,
    }


def _war_duplicate_signature(record: dict) -> tuple[Any, ...]:
    """Build a stable signature for de-duping same war content across rows."""
    signature: list[Any] = [
        str(record.get("Date", "")).strip(),
        str(record.get("Opponent", "")).strip().upper(),
        _to_int(record.get(f"{CLAN_NAME} Score")),
        _to_int(record.get("Opp Score")),
        _to_int(record.get("Net")),
    ]
    for race_number in range(1, 13):
        signature.append(_to_int(record.get(f"R{race_number} Net")))
    for race_number in range(1, 13):
        signature.append(str(record.get(f"R{race_number} Track", "")).strip().upper())
    return tuple(signature)


def _repair_war_log_rows(war_log: gspread.Worksheet, race_details: gspread.Worksheet) -> int:
    """Repair known malformed War Log rows (missing date + shifted columns)."""
    raw_rows = war_log.get_all_values()
    if len(raw_rows) <= 1:
        return 0

    rd_records = race_details.get_all_records()
    rd_date_by_war_id: dict[int, str] = {}
    for record in rd_records:
        war_id = _to_int(record.get("War ID"))
        date_value = str(record.get("Date", "")).strip()
        if war_id is not None and date_value and _is_datetime_like(date_value):
            rd_date_by_war_id[war_id] = date_value

    repaired = 0
    # Header is row 1, data starts at row 2.
    for sheet_row_number, row in enumerate(raw_rows[1:], start=2):
        # Normalize row width to the expected 30 columns (A..AD).
        if len(row) < 30:
            row = row + [""] * (30 - len(row))
        elif len(row) > 30:
            row = row[:30]

        war_id = _to_int(row[0])
        if war_id is None:
            continue

        # Signature of malformed row observed:
        # - Date column not date-like
        # - Opponent column is numeric (actually Vy score)
        # - Last column duplicates War ID due old shifted layout
        malformed = (
            not _is_datetime_like(row[1])
            and _to_int(row[2]) is not None
            and _to_int(row[3]) is not None
            and str(row[29]).strip() == str(war_id)
        )
        if not malformed:
            continue

        fixed = [""] * 30
        fixed[0] = str(war_id)
        fixed[1] = rd_date_by_war_id.get(war_id, "")
        fixed[2] = str(row[1]).strip().upper()
        fixed[3] = row[2]
        fixed[4] = row[3]
        fixed[5] = row[4]
        # Shift race nets + tracks one column right into canonical positions.
        fixed[6:30] = row[5:29]

        war_log.update(range_name=f"A{sheet_row_number}:AD{sheet_row_number}", values=[fixed])
        repaired += 1

    return repaired


def ensure_war_ids(war_log: gspread.Worksheet):
    """Guarantee every War Log row has a numeric War ID."""
    war_id_column = _ensure_header_column(war_log, "War ID")
    war_records = war_log.get_all_records()

    max_existing_id = 0
    for record in war_records:
        war_id = _to_int(record.get("War ID"))
        if war_id is not None:
            max_existing_id = max(max_existing_id, war_id)

    next_id = max_existing_id + 1
    column_letter = _column_letter(war_id_column)
    for row_number, record in enumerate(war_records, start=2):
        if _to_int(record.get("War ID")) is None:
            war_log.update_acell(f"{column_letter}{row_number}", str(next_id))
            next_id += 1


def _normalize_war_log_layout(war_log: gspread.Worksheet):
    """Ensure War Log columns follow the canonical layout with War ID in column A."""
    desired_headers = ["War ID", "Date", "Opponent", f"{CLAN_NAME} Score", "Opp Score", "Net"]
    desired_headers += [f"R{i} Net" for i in range(1, 13)]
    desired_headers += [f"R{i} Track" for i in range(1, 13)]

    current_headers = war_log.row_values(1)
    if current_headers == desired_headers:
        return

    records = war_log.get_all_records()
    _clear_data_rows(war_log)

    war_log.update(range_name=f"A1:{_column_letter(len(desired_headers))}1", values=[desired_headers])
    war_log.format(f"A1:{_column_letter(len(desired_headers))}1", {"textFormat": {"bold": True}})

    if not records:
        return

    migrated_rows = []
    for record in records:
        migrated_rows.append([record.get(header, "") for header in desired_headers])
    war_log.append_rows(migrated_rows)


def refresh_summary_sheets(spreadsheet):
    """Rebuild derived sheets from War Log and sort Track Stats by Avg per Race descending."""
    (
        war_log,
        track_stats,
        race_details,
        war_track_summary,
        net_tracker,
    ) = setup_headers(spreadsheet)
    _repair_war_log_rows(war_log, race_details)
    ensure_war_ids(war_log)

    _ensure_header_column(war_track_summary, "War ID")
    _ensure_header_column(net_tracker, "War ID")

    war_records = war_log.get_all_records()

    _clear_data_rows(track_stats)
    _clear_data_rows(war_track_summary)
    _clear_data_rows(net_tracker)

    track_totals: dict[str, dict] = {}
    summary_rows = []
    net_rows = []

    for record in war_records:
        war = _war_from_log_row(record)
        net_rows.append([
            war["date"],
            war["opponent"],
            war["difference"] if war["difference"] is not None else "",
            war["war_id"] if war["war_id"] is not None else "",
        ])

        ranked_races = sorted(war["races"], key=lambda race: race["net"], reverse=True)
        for rank, race in enumerate(ranked_races, start=1):
            summary_rows.append([
                war["date"],
                war["opponent"],
                race["track"],
                race["net"],
                rank,
                war["war_id"] if war["war_id"] is not None else "",
            ])

            track_entry = track_totals.setdefault(race["track"], {
                "played": 0,
                "net": 0,
                "scores": [],
            })
            track_entry["played"] += 1
            track_entry["net"] += race["net"]
            track_entry["scores"].append(race["net"])

    track_rows = []
    for track, data in track_totals.items():
        avg = round(data["net"] / data["played"], 2) if data["played"] else 0
        track_rows.append([
            track,
            data["played"],
            data["net"],
            avg,
            max(data["scores"]),
            min(data["scores"]),
        ])

    track_rows.sort(key=lambda row: (row[3], row[2], row[0]), reverse=True)

    if track_rows:
        track_stats.append_rows(track_rows)
    if summary_rows:
        war_track_summary.append_rows(summary_rows)
    if net_rows:
        net_tracker.append_rows(net_rows)


def setup_headers(spreadsheet) -> tuple:
    """Ensure all primary/analytics sheets exist with correct headers."""
    # War Log ─────────────────────────────────────────────────────────────────
    wl = _get_or_create_ws(spreadsheet, "War Log")
    if not wl.row_values(1):
        headers = ["War ID", "Date", "Opponent", f"{CLAN_NAME} Score", "Opp Score", "Net"]
        headers += [f"R{i} Net" for i in range(1, 13)]
        headers += [f"R{i} Track" for i in range(1, 13)]
        wl.append_row(headers)
        wl.format(f"A1:{_column_letter(len(headers))}1", {"textFormat": {"bold": True}})
    else:
        _normalize_war_log_layout(wl)

    # Track Stats ─────────────────────────────────────────────────────────────
    ts = _get_or_create_ws(spreadsheet, "Track Stats", rows=300, cols=6)
    if not ts.row_values(1):
        ts.append_row(["Track", "Times Played", "Net Total", "Avg per Race", "Best Score", "Worst Score"])
        ts.format("A1:F1", {"textFormat": {"bold": True}})

    # Race Details ───────────────────────────────────────────────────────────
    rd = _get_or_create_ws(spreadsheet, "Race Details")
    if not rd.row_values(1):
        rd.append_row(["Date", "Opponent", "Race #", "Net Score", "Vy Positions", "Track", "War ID"])
        rd.format("A1:G1", {"textFormat": {"bold": True}})
    else:
        _ensure_header_column(rd, "War ID")

    # War Track Summary ──────────────────────────────────────────────────────
    wts = _get_or_create_ws(spreadsheet, "War Track Summary", rows=1000, cols=5)
    if not wts.row_values(1):
        wts.append_row(["War Date", "Opponent", "Track", "Net Score", "Rank", "War ID"])
        wts.format("A1:F1", {"textFormat": {"bold": True}})
    else:
        _ensure_header_column(wts, "War ID")

    # Net Tracker ─────────────────────────────────────────────────────────────
    net_tracker = _get_net_tracker_ws(spreadsheet)

    if not net_tracker.row_values(1):
        net_tracker.append_row(["Date", "Opponent", "Net", "War ID"])
        net_tracker.format("A1:D1", {"textFormat": {"bold": True}})
    else:
        _ensure_header_column(net_tracker, "War ID")

    return wl, ts, rd, wts, net_tracker


def write_war(war: dict):
    """Write a parsed war dict to all five Google Sheets."""
    spreadsheet = get_spreadsheet()
    (
        war_log,
        _track_stats,
        race_details,
        _war_track_summary,
        _net_tracker,
    ) = setup_headers(spreadsheet)
    ensure_war_ids(war_log)
    existing_wars = war_log.get_all_records()
    next_war_id = max((_to_int(record.get("War ID")) or 0 for record in existing_wars), default=0) + 1
    war["war_id"] = next_war_id

    # ── War Log row ───────────────────────────────────────────────────────────
    race_nets   = {r["race"]: r["net"]   for r in war["races"]}
    race_tracks = {r["race"]: r["track"] for r in war["races"]}

    wl_row = [
        war["war_id"],
        war["date"],
        war["opponent"],
        war["vy_score"]   if war["vy_score"]   is not None else "",
        war["opp_score"]  if war["opp_score"]  is not None else "",
        war["difference"] if war["difference"] is not None else "",
    ]
    for i in range(1, 13):
        wl_row.append(race_nets.get(i, ""))
    for i in range(1, 13):
        wl_row.append(race_tracks.get(i, ""))
    war_log.append_row(wl_row)

    # ── Race Details rows ─────────────────────────────────────────────────────
    detail_rows = [
        [
            war["date"],
            war["opponent"],
            r["race"],
            r["net"],
            ", ".join(str(p) for p in r["positions"]),
            r.get("track", ""),
            war["war_id"],
        ]
        for r in war["races"]
    ]
    if detail_rows:
        race_details.append_rows(detail_rows)

    refresh_summary_sheets(spreadsheet)


# ── Discord Bot ────────────────────────────────────────────────────────────────
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)


@bot.event
async def on_ready():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Logged in as {bot.user}  |  Tracking wars for {CLAN_NAME}")
    if not reaction_schedule_loop.is_running():
        reaction_schedule_loop.start()


@bot.event
async def on_message(message: discord.Message):
    if message.author == bot.user:
        return

    ctx = await bot.get_context(message)
    # Treat any prefixed message as command-like to avoid accidental auto-log duplicates
    # when a command is malformed or unrecognized.
    is_prefixed_message = bool(ctx.prefix)

    # In active manual war sessions, support Quaxly-like commandless race input.
    handled_shorthand = await _handle_war_shorthand_message(message)

    # Avoid double-logging when users run !addwar with pasted war text.
    if not handled_shorthand and not is_prefixed_message:
        text = _extract_war_text_from_message(message)
        if AUTO_LOG_RESULTS and "Total Score after Race" in text:
            await _log_war(message.channel, text)

    await bot.process_commands(message)


@bot.command(name="addwar")
async def cmd_addwar(ctx: commands.Context, *, text: Optional[str] = None):
    """
    Manually log a war result.
    Usage: !addwar <paste the full Lorenzi/Quaxly output here>
    """
    source_text = text

    # If no inline text was given, allow replying to a Quaxly message and running !addwar.
    if not source_text and ctx.message.reference:
        ref_msg = ctx.message.reference.resolved
        if not isinstance(ref_msg, discord.Message) and ctx.message.reference.message_id:
            try:
                ref_msg = await ctx.channel.fetch_message(ctx.message.reference.message_id)
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                ref_msg = None

        if isinstance(ref_msg, discord.Message):
            source_text = _extract_war_text_from_message(ref_msg)

    if not source_text:
        await ctx.send(
            "**Usage:** `!addwar <paste full war result>`\n"
            "Or reply to the Quaxly result message and send just `!addwar`."
        )
        return
    await _log_war(ctx.channel, source_text)


@bot.command(name="reactsetup")
async def cmd_reactsetup(ctx: commands.Context, start_hour: Optional[int] = None, end_hour: Optional[int] = None):
    """
    Configure daily reaction schedule channel and hour range (Pacific, 24h).
    Usage:
      !reactsetup             -> defaults to 9..22 (9 AM to 10 PM)
      !reactsetup 9 22
    """
    global REACTION_CONFIG

    if start_hour is None:
        start_hour = DEFAULT_REACT_START_HOUR
    if end_hour is None:
        end_hour = DEFAULT_REACT_END_HOUR

    if start_hour < 0 or start_hour > 23 or end_hour < 0 or end_hour > 23 or start_hour > end_hour:
        await ctx.send("Usage: `!reactsetup [start_hour] [end_hour]` where hours are 0-23 and start <= end.")
        return

    _mark_runtime_mutation("reactsetup")
    REACTION_CONFIG["channel_id"] = ctx.channel.id
    REACTION_CONFIG["start_hour"] = int(start_hour)
    REACTION_CONFIG["end_hour"] = int(end_hour)
    REACTION_CONFIG["last_posted_date"] = ""
    save_reaction_config(REACTION_CONFIG)

    await ctx.send(
        f"✅ React schedule configured for this channel (Pacific time): "
        f"`{_hour_to_label(start_hour)}` to `{_hour_to_label(end_hour)}`.\n"
        "Run `!react` to post/sync today's Unix slots now."
    )


@bot.command(name="react")
async def cmd_react(ctx: commands.Context):
    """
    Manual override for daily scheduler:
    - ensures today's Unix time-slot messages exist
    - adds default reactions to all slots
    - clears reactions for already-passed hours (Pacific)
    """
    global REACTION_SLOTS
    global REACTION_CONFIG

    # If scheduler channel is not configured, default to current channel.
    if not isinstance(REACTION_CONFIG.get("channel_id"), int):
        REACTION_CONFIG["channel_id"] = ctx.channel.id
        REACTION_CONFIG["start_hour"] = int(REACTION_CONFIG.get("start_hour", DEFAULT_REACT_START_HOUR))
        REACTION_CONFIG["end_hour"] = int(REACTION_CONFIG.get("end_hour", DEFAULT_REACT_END_HOUR))
        REACTION_CONFIG["last_posted_date"] = ""
        save_reaction_config(REACTION_CONFIG)

    now_pt = datetime.now(PT_TZ)
    posted = await _ensure_daily_reaction_slots(now_pt)

    if not REACTION_SLOTS:
        await ctx.send(
            "No active slot messages found. Configure with `!reactsetup` and run `!react` again."
        )
        return

    # Strict-check existence so deleted slot messages are detected and pruned.
    valid_slots: list[dict[str, int]] = []
    missing = 0
    for slot in REACTION_SLOTS:
        msg = await _fetch_message_from_slot(slot, strict=True)
        if msg is None:
            missing += 1
            continue
        valid_slots.append(slot)

    # If today's slots were deleted, republish today's set and continue.
    reposted = 0
    if valid_slots == []:
        async with _REACTION_SCHEDULER_LOCK:
            reposted = await _post_daily_reaction_slots(now_pt)
        valid_slots = list(REACTION_SLOTS)

    if missing > 0:
        REACTION_SLOTS = valid_slots
        save_reaction_slots(REACTION_SLOTS)

    added = 0
    touched = 0
    for slot in REACTION_SLOTS:
        msg = await _fetch_message_from_slot(slot)
        if msg is None:
            continue
        add_count = await _add_default_reactions(msg)
        if add_count > 0:
            touched += 1
            added += add_count

    await _run_hourly_reaction_cleanup(now_pt)

    current_hour = now_pt.hour
    active_or_upcoming = sum(1 for slot in REACTION_SLOTS if slot.get("hour", -1) >= current_hour)
    expired = max(0, len(REACTION_SLOTS) - active_or_upcoming)

    await ctx.send(
        f"✅ React sync complete (Pacific). Posted new slot messages: `{posted}`"
        f"{f' (reposted `{reposted}` due to deleted slots)' if reposted > 0 else ''}. "
        f"Added `{added}` reactions across `{touched}` messages and cleared expired slots (`{expired}`)."
        f"{f' Pruned `{missing}` deleted/missing tracked slots.' if missing > 0 else ''}"
    )


@bot.command(name="reactionmatch")
async def cmd_reactionmatch(ctx: commands.Context):
    """
    Manually reconcile tracked slot reactions to the current Pacific hour.
    Useful if the hourly scheduler missed a cleanup window.
    """
    global REACTION_SLOTS
    global REACTION_CONFIG

    # If scheduler channel is not configured, default to current channel.
    if not isinstance(REACTION_CONFIG.get("channel_id"), int):
        REACTION_CONFIG["channel_id"] = ctx.channel.id
        REACTION_CONFIG["start_hour"] = int(REACTION_CONFIG.get("start_hour", DEFAULT_REACT_START_HOUR))
        REACTION_CONFIG["end_hour"] = int(REACTION_CONFIG.get("end_hour", DEFAULT_REACT_END_HOUR))
        REACTION_CONFIG["last_posted_date"] = ""
        save_reaction_config(REACTION_CONFIG)

    _mark_runtime_mutation("reactionmatch")

    now_pt = datetime.now(PT_TZ)
    posted = await _ensure_daily_reaction_slots(now_pt)

    if not REACTION_SLOTS:
        await ctx.send(
            "No active slot messages found. Configure with `!reactsetup` and run `!react` or `!reactionmatch` again."
        )
        return

    valid_slots: list[dict[str, int]] = []
    missing = 0
    for slot in REACTION_SLOTS:
        msg = await _fetch_message_from_slot(slot, strict=True)
        if msg is None:
            missing += 1
            continue
        valid_slots.append(slot)

    if missing > 0:
        REACTION_SLOTS = valid_slots
        save_reaction_slots(REACTION_SLOTS)

    current_hour = now_pt.hour
    added = 0
    cleared = 0
    for slot in REACTION_SLOTS:
        msg = await _fetch_message_from_slot(slot)
        if msg is None:
            continue

        slot_hour = slot.get("hour")
        if isinstance(slot_hour, int) and slot_hour < current_hour:
            cleared += await _clear_default_reactions(msg)
        else:
            added += await _add_default_reactions(msg)

    expired = 0
    for slot in REACTION_SLOTS:
        slot_hour = slot.get("hour")
        if isinstance(slot_hour, int) and slot_hour < current_hour:
            expired += 1
    active_or_upcoming = max(0, len(REACTION_SLOTS) - expired)

    await ctx.send(
        f"✅ Reaction match complete (Pacific `{now_pt.strftime('%I:%M %p')}`). "
        f"Posted new slot messages: `{posted}`. "
        f"Cleared `{cleared}` reactions for `{expired}` expired slots, and ensured `{added}` reactions "
        f"across `{active_or_upcoming}` active/upcoming slots."
        f"{f' Pruned `{missing}` deleted/missing tracked slots.' if missing > 0 else ''}"
    )


@bot.command(name="reactstatus")
async def cmd_reactstatus(ctx: commands.Context):
    """Show scheduler config and active daily slot messages."""
    channel_id = REACTION_CONFIG.get("channel_id")
    if not isinstance(channel_id, int):
        await ctx.send("React scheduler is not configured. Run `!reactsetup` first.")
        return

    now_pt = datetime.now(PT_TZ)
    date_key = now_pt.strftime("%Y-%m-%d")
    channel_slots = [slot for slot in REACTION_SLOTS if slot.get("channel_id") == channel_id]

    lines = [
        f"React status for this channel ({now_pt.strftime('%Y-%m-%d %I:%M %p')} PT):",
        "",
        f"configured_channel_id={channel_id}",
        f"hour_range={_hour_to_label(int(REACTION_CONFIG.get('start_hour', DEFAULT_REACT_START_HOUR)))} - "
        f"{_hour_to_label(int(REACTION_CONFIG.get('end_hour', DEFAULT_REACT_END_HOUR)))}",
        f"last_posted_date={REACTION_CONFIG.get('last_posted_date', '')}",
        f"today={date_key}",
        "",
    ]

    if not channel_slots:
        lines.append("No active slot messages are tracked yet for the configured channel.")

    for slot in sorted(channel_slots, key=lambda s: s["hour"]):
        hour = int(slot["hour"])
        if hour < now_pt.hour:
            state = "expired (reactions should be cleared)"
        elif hour == now_pt.hour:
            state = "current hour"
        else:
            state = "upcoming"

        lines.append(f"{_hour_to_label(hour)} | {state} | message_id={slot['message_id']}")

    message = "\n".join(lines)
    if len(message) <= 1900:
        await ctx.send(f"```\n{message}\n```")
        return

    chunk = ""
    for line in lines:
        candidate = f"{chunk}\n{line}" if chunk else line
        if len(candidate) > 1900:
            await ctx.send(f"```\n{chunk}\n```")
            chunk = line
        else:
            chunk = candidate
    if chunk:
        await ctx.send(f"```\n{chunk}\n```")


@bot.command(name="reactclear")
async def cmd_reactclear(ctx: commands.Context):
    """Clear reactions and disable scheduler for this channel."""
    global REACTION_SLOTS
    global REACTION_CONFIG

    channel_slots = [slot for slot in REACTION_SLOTS if slot.get("channel_id") == ctx.channel.id]
    should_disable = REACTION_CONFIG.get("channel_id") == ctx.channel.id
    if not channel_slots and not should_disable:
        await ctx.send("No scheduler state found to clear for this channel.")
        return

    _mark_runtime_mutation("reactclear")

    cleared = 0
    for slot in channel_slots:
        msg = await _fetch_message_from_slot(slot)
        if msg is None:
            continue
        await _clear_default_reactions(msg)
        cleared += 1

    REACTION_SLOTS = [slot for slot in REACTION_SLOTS if slot.get("channel_id") != ctx.channel.id]
    save_reaction_slots(REACTION_SLOTS)

    if REACTION_CONFIG.get("channel_id") == ctx.channel.id:
        REACTION_CONFIG["channel_id"] = None
        REACTION_CONFIG["last_posted_date"] = ""
        save_reaction_config(REACTION_CONFIG)

    await ctx.send(
        f"🧹 Cleared scheduler reactions from `{cleared}` tracked messages and removed "
        f"`{len(channel_slots)}` tracked slots for this channel. Scheduler is now disabled here."
    )


@bot.command(name="reactdelete")
async def cmd_reactdelete(ctx: commands.Context):
    """Delete all tracked slot messages for this channel and reset today's post state."""
    global REACTION_SLOTS
    global REACTION_CONFIG

    channel_slots = [slot for slot in REACTION_SLOTS if slot.get("channel_id") == ctx.channel.id]
    if not channel_slots:
        await ctx.send("No tracked slot messages found for this channel.")
        return

    _mark_runtime_mutation("reactdelete")

    deleted = 0
    failed = 0
    for slot in channel_slots:
        msg = await _fetch_message_from_slot(slot)
        if msg is None:
            failed += 1
            continue

        delete_fn = getattr(msg, "delete", None)
        if not callable(delete_fn):
            failed += 1
            continue

        try:
            result = delete_fn()
            if asyncio.iscoroutine(result):
                await result
            deleted += 1
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            failed += 1

    REACTION_SLOTS = [slot for slot in REACTION_SLOTS if slot.get("channel_id") != ctx.channel.id]
    save_reaction_slots(REACTION_SLOTS)

    # Allow immediate repost with !react for same-day testing.
    if REACTION_CONFIG.get("channel_id") == ctx.channel.id:
        REACTION_CONFIG["last_posted_date"] = ""
        save_reaction_config(REACTION_CONFIG)

    await ctx.send(
        f"🗑️ React test cleanup complete. Deleted `{deleted}` slot messages "
        f"(`{failed}` missing/failed). Run `!react` to repost today's test slots."
    )


@bot.command(name="reactreset")
async def cmd_reactreset(ctx: commands.Context):
    """Delete this channel's bot messages and repost today's gather slot messages."""
    global REACTION_SLOTS
    global REACTION_CONFIG

    _mark_runtime_mutation("reactreset")

    REACTION_CONFIG["channel_id"] = ctx.channel.id
    REACTION_CONFIG["start_hour"] = int(REACTION_CONFIG.get("start_hour", DEFAULT_REACT_START_HOUR))
    REACTION_CONFIG["end_hour"] = int(REACTION_CONFIG.get("end_hour", DEFAULT_REACT_END_HOUR))
    REACTION_CONFIG["last_posted_date"] = ""
    save_reaction_config(REACTION_CONFIG)

    deleted, failed = await _delete_bot_messages_in_channel(ctx.channel.id)

    REACTION_SLOTS = [slot for slot in REACTION_SLOTS if slot.get("channel_id") != ctx.channel.id]
    save_reaction_slots(REACTION_SLOTS)

    async with _REACTION_SCHEDULER_LOCK:
        posted = await _post_daily_reaction_slots(datetime.now(PT_TZ))

    await ctx.send(
        f"✅ React reset complete. Deleted `{deleted}` bot messages in this channel"
        f"{f' (`{failed}` failed)' if failed > 0 else ''} and posted `{posted}` new gather slots for today."
    )


@bot.command(name="warstart")
async def cmd_warstart(ctx: commands.Context, opponent: str):
    """
    Start a manual war session in this channel.
    Usage: !warstart <opponent>
    """
    channel_id = ctx.channel.id
    _mark_runtime_mutation("warstart")
    ACTIVE_WARS[channel_id] = {
        "opponent": opponent.strip().upper(),
        "races": {},
        "created_by": ctx.author.id,
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
    }
    await ctx.send(
        f"✅ Started war vs `{opponent.strip().upper()}`.\n"
        "No race limit is enforced; keep entering races until `!warend`.\n"
        "Add race results with: `!warset <race> <net> <positions_csv>`\n"
        "Example: `!warset 1 +24 1,2,4,6,7,9`"
    )


@bot.command(name="warset")
async def cmd_warset(ctx: commands.Context, race: int, net: int, positions_csv: str):
    """
    Set/update one race result in active session.
    Usage: !warset <race> <net> <positions_csv>
    """
    channel_id = ctx.channel.id
    session = ACTIVE_WARS.get(channel_id)
    if not session:
        await ctx.send("No active war in this channel. Start one with `!warstart <opponent>`.")
        return

    if race < 1:
        await ctx.send("Race number must be 1 or greater.")
        return

    positions = []
    for token in positions_csv.split(","):
        token = token.strip()
        if not token:
            continue
        if not token.isdigit():
            await ctx.send("Positions must be comma-separated integers, e.g. `1,2,4,6,7,9`.")
            return
        positions.append(int(token))

    if not positions:
        await ctx.send("Please provide at least one finishing position.")
        return

    computed_net = _calc_net_from_positions(positions)
    if int(net) != computed_net:
        net_note = f" (auto-net adjusted from {int(net):+d} to {computed_net:+d})"
    else:
        net_note = ""

    _mark_runtime_mutation("warset")
    session["races"][race] = {
        "race": race,
        "net": computed_net,
        "positions": positions,
    }
    war_preview = _session_as_war_dict(session)
    await ctx.send(
        f"✅ Set race `{race}` with `{positions_csv}` -> net `{computed_net:+d}`{net_note}\n"
        f"```\n{_format_war_summary_text(war_preview)}\n```"
    )


@bot.command(name="warshow")
async def cmd_warshow(ctx: commands.Context):
    """Show current active war session in Quaxly-like format."""
    channel_id = ctx.channel.id
    session = ACTIVE_WARS.get(channel_id)
    if not session:
        await ctx.send("No active war in this channel.")
        return

    summary = _session_as_war_dict(session)
    await ctx.send(f"```\n{_format_war_summary_text(summary)}\n```")


@bot.command(name="warcancel")
async def cmd_warcancel(ctx: commands.Context):
    """Cancel the active war session in this channel."""
    channel_id = ctx.channel.id
    if channel_id in ACTIVE_WARS:
        _mark_runtime_mutation("warcancel")
        del ACTIVE_WARS[channel_id]
        await ctx.send("🛑 Active war session cancelled.")
    else:
        await ctx.send("No active war session to cancel.")


@bot.command(name="undorace")
async def cmd_undorace(ctx: commands.Context):
    """
    Undo the last race entered in the active war session.
    Also clears any pending track waiting for shorthand input.
    Usage: !undorace
    """
    channel_id = ctx.channel.id
    session = ACTIVE_WARS.get(channel_id)
    if not session:
        await ctx.send("No active war in this channel.")
        return

    _mark_runtime_mutation("undorace")
    had_pending = "pending_track" in session
    session.pop("pending_track", None)

    if not session["races"]:
        msg = "No races to undo."
        if had_pending:
            msg = "Cleared pending track (no races to undo)."
        await ctx.send(msg)
        return

    last_race_num = max(session["races"].keys())
    removed = session["races"].pop(last_race_num)

    if not session["races"]:
        await ctx.send(
            f"↩️ Removed race `{last_race_num}` (`{removed['track']}`, net `{removed['net']:+d}`). "
            "No races remaining."
        )
        return

    war_preview = _session_as_war_dict(session)
    await ctx.send(
        f"↩️ Removed race `{last_race_num}` (`{removed['track']}`, net `{removed['net']:+d}`)."
        f"{'  Also cleared pending track.' if had_pending else ''}\n"
        f"```\n{_format_war_summary_text(war_preview)}\n```"
    )


@bot.command(name="editspots")
async def cmd_editspots(ctx: commands.Context, race: int, *, spots: str):
    """
    Edit the finishing positions for an existing race in the active session.
    Recalculates net automatically. Accepts CSV or shorthand.
    Usage:
      !editspots 3 1,2,4,6,7,9
      !editspots 3 13468+
    """
    channel_id = ctx.channel.id
    session = ACTIVE_WARS.get(channel_id)
    if not session:
        await ctx.send("No active war in this channel.")
        return

    if race not in session["races"]:
        await ctx.send(f"Race `{race}` not found. Use `!warshow` to see current races.")
        return

    spots = spots.strip()

    # Try CSV first (contains commas or is all digits/spaces).
    positions: Optional[list[int]] = None
    if "," in spots or re.fullmatch(r"[\d\s]+", spots):
        parsed = []
        valid = True
        for token in re.split(r"[,\s]+", spots):
            if not token:
                continue
            if not token.isdigit():
                valid = False
                break
            parsed.append(int(token))
        if valid and parsed:
            positions = parsed

    # Fall back to shorthand (e.g. 13478+).
    if positions is None:
        positions = _parse_positions_shorthand(spots)
        if positions is None:
            await ctx.send(
                "Could not parse positions. Use CSV like `1,2,4,6,7,9` or shorthand like `13478+`."
            )
            return

    net = _calc_net_from_positions(positions)
    old_net = session["races"][race]["net"]
    track = session["races"][race]["track"]

    _mark_runtime_mutation("editspots")
    session["races"][race]["positions"] = positions
    session["races"][race]["net"] = net

    war_preview = _session_as_war_dict(session)
    await ctx.send(
        f"✅ Race `{race}` (`{track}`) positions updated to `{','.join(map(str, positions))}` "
        f"-> net `{net:+d}` (was `{old_net:+d}`)\n"
        f"```\n{_format_war_summary_text(war_preview)}\n```"
    )


@bot.command(name="warend")
async def cmd_warend(ctx: commands.Context, vy_score: Optional[int] = None, opp_score: Optional[int] = None):
    """
        Finalize active war and export it to Google Sheets.
        Usage:
            !warend                     (auto-calculated scores from race positions)
            !warend <vy_score> <opp_score>   (optional manual override)
    """
    channel_id = ctx.channel.id
    session = ACTIVE_WARS.get(channel_id)
    if not session:
        await ctx.send("No active war in this channel. Start one with `!warstart`.")
        return

    if len(session["races"]) == 0:
        await ctx.send("No race entries were set. Add races with `!warset` first.")
        return

    war = _session_as_war_dict(session)
    war["num_races"] = len(session["races"])

    # Optional manual score override.
    if vy_score is not None and opp_score is not None:
        war["vy_score"] = int(vy_score)
        war["opp_score"] = int(opp_score)
        war["difference"] = int(vy_score) - int(opp_score)

    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: (
                run_sheets_with_retry(backup_state, get_spreadsheet(), "warend"),
                run_sheets_with_retry(write_war, war),
            ),
        )
        _mark_runtime_mutation("warend")
        _mark_sheets_mutation("warend")
        del ACTIVE_WARS[channel_id]
        diff = int(war.get("difference", 0) or 0)
        if diff > 0:
            outcome = "WIN"
        elif diff < 0:
            outcome = "LOSS"
        else:
            outcome = "TIE"

        await ctx.send(
            f"✅ Exported war vs `{war['opponent']}` to Google Sheets.\n"
            f"**Result:** {outcome}\n"
            f"**Total Differential:** {diff:+d}\n"
            f"```\n{_format_war_summary_text(war)}\n```"
        )
    except Exception as exc:
        await ctx.send(f"❌ Failed to export war: `{exc}`")


def _clean_command_arg(arg: Optional[str]) -> str:
    """Normalize Discord command args by removing hidden/formatting characters."""
    if arg is None:
        return ""
    cleaned = str(arg)
    cleaned = cleaned.replace("\u200b", "").replace("\ufeff", "").replace("\xa0", " ")
    cleaned = cleaned.strip().strip("`").strip().strip('"').strip("'").strip()
    return cleaned


def _parse_nonnegative_int_arg(arg: Optional[str]) -> Optional[int]:
    """Parse a command arg as a non-negative integer, returning None when not numeric."""
    cleaned = _clean_command_arg(arg).replace(",", "")
    if not cleaned:
        return None
    if not re.fullmatch(r"\d+", cleaned):
        return None
    try:
        return int(cleaned)
    except ValueError:
        return None


def _normalize_track_token(value: Optional[str]) -> str:
    """Normalize track-like tokens for resilient comparisons."""
    cleaned = _clean_command_arg(value)
    canonical = _canonical_track_code(cleaned)
    if canonical:
        return canonical.upper()
    return cleaned.upper()


@bot.command(name="trackstats")
async def cmd_trackstats(ctx: commands.Context, arg: Optional[str] = None):
    """
    Show track stats.
    • !trackstats         — all tracks sorted by average gain per race
    • !trackstats rAF     — stats for a specific track
    • !trackstats 3       — only tracks played at least 3 times
    """
    try:
        spreadsheet = get_spreadsheet()
        ts = spreadsheet.worksheet("Track Stats")
        records = ts.get_all_records()
        arg_clean = _clean_command_arg(arg)
        min_played_arg = _parse_nonnegative_int_arg(arg_clean)

        if not records:
            await ctx.send("No track data logged yet.")
            return

        if arg_clean and min_played_arg is None:
            track = arg_clean
            track_norm = _normalize_track_token(track)
            match = next(
                (
                    r
                    for r in records
                    if _normalize_track_token(str(r.get("Track", ""))) == track_norm
                ),
                None,
            )
            if not match:
                await ctx.send(f"Track `{track}` not found in the stats sheet.")
                return

            color = 0x57F287 if int(match.get("Net Total", 0)) >= 0 else 0xED4245
            embed = discord.Embed(title=f"Track Stats — {match['Track']}", color=color)
            embed.add_field(name="Times Played", value=match.get("Times Played", 0),   inline=True)
            embed.add_field(name="Net Total",    value=match.get("Net Total",    0),   inline=True)
            embed.add_field(name="Avg/Race",     value=match.get("Avg per Race", 0),   inline=True)
            embed.add_field(name="Best Score",   value=match.get("Best Score",  "N/A"), inline=True)
            embed.add_field(name="Worst Score",  value=match.get("Worst Score", "N/A"), inline=True)
            await ctx.send(embed=embed)
        else:
            min_played = min_played_arg or 0
            if min_played > 0:
                records = [r for r in records if int(r.get("Times Played", 0)) >= min_played]

            if not records:
                if min_played > 0:
                    await ctx.send(f"No tracks found with at least `{min_played}` plays.")
                else:
                    await ctx.send("No track data logged yet.")
                return

            records_sorted = sorted(records, key=lambda r: float(r.get("Avg per Race", 0)), reverse=True)
            lines = ["```", f"{'Track':<8}  {'Played':>6}  {'Net':>6}  {'Avg':>6}", "─" * 32]
            for r in records_sorted:
                net = int(r.get("Net Total", 0))
                lines.append(
                    f"{str(r.get('Track', '')):<8}  "
                    f"{int(r.get('Times Played', 0)):>6}  "
                    f"{net:>+6}  "
                    f"{float(r.get('Avg per Race', 0)):>6.2f}"
                )
            lines.append("```")
            await ctx.send("\n".join(lines))

    except gspread.WorksheetNotFound:
        await ctx.send("Track Stats sheet not found. Run a war first or use `!setupsheets`.")
    except Exception as exc:
        await ctx.send(f"❌ Error: `{exc}`")


@bot.command(name="warstats")
async def cmd_warstats(ctx: commands.Context, n: int = 5):
    """
    Show recent war results.
    • !warstats      — last 5 wars
    • !warstats 10   — last 10 wars
    """
    try:
        spreadsheet = get_spreadsheet()
        wl = spreadsheet.worksheet("War Log")
        rows = wl.get_all_records()

        if not rows:
            await ctx.send("No wars logged yet.")
            return

        recent = rows[-n:]
        wins   = sum(1 for r in recent if isinstance(r.get("Net"), (int, float)) and float(r["Net"]) >= 0)
        losses = len(recent) - wins

        lines = [
            "```",
            f"{'Date':<17}  {'Opp':<8}  {CLAN_NAME:>5}  {'Opp':>5}  {'Net':>5}",
            "─" * 40,
        ]
        for r in recent:
            net = r.get("Net", "")
            net_str = f"{int(net):+d}" if isinstance(net, (int, float)) else str(net)
            lines.append(
                f"{str(r.get('Date', '')):<17}  "
                f"{str(r.get('Opponent', '')):<8}  "
                f"{str(r.get(f'{CLAN_NAME} Score', '')):>5}  "
                f"{str(r.get('Opp Score', '')):>5}  "
                f"{net_str:>5}"
            )
        lines.append("─" * 40)
        lines.append(f"W: {wins}  L: {losses}  (last {len(recent)} wars)")
        lines.append("```")
        await ctx.send("\n".join(lines))

    except gspread.WorksheetNotFound:
        await ctx.send("War Log sheet not found.")
    except Exception as exc:
        await ctx.send(f"❌ Error: `{exc}`")


@bot.command(name="setupsheets")
@commands.has_permissions(administrator=True)
async def cmd_setupsheets(ctx: commands.Context):
    """Re-initialize Google Sheets headers. Admin only."""
    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: (
                run_sheets_with_retry(backup_state, get_spreadsheet(), "setupsheets"),
                run_sheets_with_retry(setup_headers, get_spreadsheet()),
            ),
        )
        _mark_sheets_mutation("setupsheets")
        await ctx.send("✅ Sheets initialized / headers verified.")
    except Exception as exc:
        await ctx.send(f"❌ Error: `{exc}`")


@bot.command(name="update")
async def cmd_update(ctx: commands.Context):
    """Rebuild derived sheets from the full War Log history."""
    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: (
                run_sheets_with_retry(backup_state, get_spreadsheet(), "update"),
                run_sheets_with_retry(refresh_summary_sheets, get_spreadsheet()),
            ),
        )
        _mark_sheets_mutation("update")
        await ctx.send("✅ Rebuilt all war summaries from `War Log` and sorted `Track Stats` by average gain per race.")
    except Exception as exc:
        await ctx.send(f"❌ Error: `{exc}`")


@bot.command(name="undo")
async def cmd_undo(ctx: commands.Context):
    """Undo the last mutating command (runtime or sheets), single-step."""
    global _RUNTIME_REDO_SNAPSHOT
    global _LAST_UNDONE_SCOPE
    global _LAST_UNDONE_ACTION

    if _LAST_MUTATION_SCOPE == "runtime":
        if _RUNTIME_UNDO_SNAPSHOT is None or not _LAST_MUTATION_ACTION:
            await ctx.send("❌ Cannot undo: no runtime snapshot found.")
            return
        _RUNTIME_REDO_SNAPSHOT = _capture_runtime_state()
        _restore_runtime_state(_RUNTIME_UNDO_SNAPSHOT)
        _LAST_UNDONE_SCOPE = "runtime"
        _LAST_UNDONE_ACTION = _LAST_MUTATION_ACTION
        await ctx.send(
            f"↩️ Undoing `!{_LAST_MUTATION_ACTION}`. "
            "Use `!redo` if this was a mistake."
        )
        return

    if _LAST_MUTATION_SCOPE != "sheets":
        await ctx.send("❌ Nothing to undo yet.")
        return

    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            lambda: (
                run_sheets_with_retry(backup_redo_state, get_spreadsheet(), "undo"),
                run_sheets_with_retry(restore_state_from_backup, get_spreadsheet()),
            )[-1],
        )
        if not result or not isinstance(result, tuple) or len(result) != 2:
            await ctx.send("❌ Cannot undo: invalid snapshot result.")
            return
        ok, action = result
        if not ok:
            await ctx.send(f"❌ Cannot undo: {action}")
            return

        # Pair runtime rollback for mixed commands (e.g. !warend).
        if _RUNTIME_UNDO_SNAPSHOT is not None and _LAST_MUTATION_ACTION == action:
            _RUNTIME_REDO_SNAPSHOT = _capture_runtime_state()
            _restore_runtime_state(_RUNTIME_UNDO_SNAPSHOT)

        _LAST_UNDONE_SCOPE = "sheets"
        _LAST_UNDONE_ACTION = action
        await ctx.send(
            f"↩️ Undoing `!{action}`. "
            "Use `!redo` if this was a mistake."
        )
    except Exception as exc:
        await ctx.send(f"❌ Error: `{exc}`")


@bot.command(name="redo")
async def cmd_redo(ctx: commands.Context):
    """Redo the last undone command (runtime or sheets), single-step."""
    if _LAST_UNDONE_SCOPE == "runtime":
        if _RUNTIME_REDO_SNAPSHOT is None or not _LAST_UNDONE_ACTION:
            await ctx.send("❌ Cannot redo: no runtime redo snapshot found.")
            return
        _restore_runtime_state(_RUNTIME_REDO_SNAPSHOT)
        await ctx.send(f"↪️ Redoing `!{_LAST_UNDONE_ACTION}`.")
        return

    if _LAST_UNDONE_SCOPE != "sheets":
        await ctx.send("❌ Nothing to redo yet.")
        return

    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            lambda: run_sheets_with_retry(restore_state_from_redo, get_spreadsheet()),
        )
        if not result or not isinstance(result, tuple) or len(result) != 2:
            await ctx.send("❌ Cannot redo: invalid snapshot result.")
            return
        ok, action = result
        if not ok:
            await ctx.send(f"❌ Cannot redo: {action}")
            return

        # Pair runtime re-apply for mixed commands (e.g. !warend).
        if _RUNTIME_REDO_SNAPSHOT is not None and _LAST_UNDONE_ACTION == action:
            _restore_runtime_state(_RUNTIME_REDO_SNAPSHOT)

        await ctx.send(f"↪️ Redoing `!{action}`.")
    except Exception as exc:
        await ctx.send(f"❌ Error: `{exc}`")


@bot.command(name="deletewar")
async def cmd_deletewar(ctx: commands.Context, war_index: int = 1):
    """
    Delete a war result from all sheets.
    Usage:
      !deletewar        — delete the most recent war
      !deletewar 2      — delete the 2nd most recent war
    """
    try:
        spreadsheet = get_spreadsheet()
        wl = spreadsheet.worksheet("War Log")
        rd = spreadsheet.worksheet("Race Details")
        ensure_war_ids(wl)
        
        # Try to get optional sheets (may not exist if no wars logged yet)
        try:
            wts = spreadsheet.worksheet("War Track Summary")
        except gspread.WorksheetNotFound:
            wts = None
        
        nt = _get_net_tracker_ws(spreadsheet)

        # Get all war log rows (skip header)
        wl_records = wl.get_all_records()
        if not wl_records:
            await ctx.send("No wars to delete.")
            return

        if war_index < 1 or war_index > len(wl_records):
            await ctx.send(f"Invalid war index. You have {len(wl_records)} wars logged.")
            return

        # Get the war to delete (index from end: 1 = most recent)
        war_to_delete = wl_records[-war_index]
        war_id = _to_int(war_to_delete.get("War ID"))
        war_date = war_to_delete.get("Date", "Unknown")
        war_opponent = war_to_delete.get("Opponent", "Unknown")

        # Show confirmation
        embed = discord.Embed(
            title="❌ Delete War?",
            description=f"**War ID:** {war_id}\n**Date:** {war_date}\n**Opponent:** {war_opponent}",
            color=0xED4245,
        )
        embed.set_footer(text="React with ✅ to confirm or ❌ to cancel (30s timeout)")

        msg = await ctx.send(embed=embed)
        await msg.add_reaction("✅")
        await msg.add_reaction("❌")

        def check(reaction, user):
            return user == ctx.author and str(reaction.emoji) in ["✅", "❌"]

        try:
            reaction, _ = await bot.wait_for("reaction_add", timeout=30.0, check=check)
            if str(reaction.emoji) != "✅":
                await ctx.send("Cancelled.")
                return
        except asyncio.TimeoutError:
            await ctx.send("Deletion cancelled (timeout).")
            return

        run_sheets_with_retry(backup_state, spreadsheet, "deletewar")

        # Delete from War Log (row index = record index + 2, because row 1 is header)
        wl_row_num = len(wl_records) - war_index + 2

        # Delete related Race Details rows
        rd_records = rd.get_all_records()
        rd_rows_to_delete = []
        for i, rec in enumerate(rd_records):
            record_war_id = _to_int(rec.get("War ID"))
            if war_id is not None and record_war_id is not None:
                if record_war_id == war_id:
                    rd_rows_to_delete.append(i + 2)
            elif rec.get("Date") == war_date and rec.get("Opponent") == war_opponent:
                rd_rows_to_delete.append(i + 2)

        # Delete related War Track Summary rows
        wts_rows_to_delete = []
        if wts:
            wts_records = wts.get_all_records()
            for i, rec in enumerate(wts_records):
                record_war_id = _to_int(rec.get("War ID"))
                if war_id is not None and record_war_id is not None:
                    if record_war_id == war_id:
                        wts_rows_to_delete.append(i + 2)
                elif rec.get("War Date") == war_date and rec.get("Opponent") == war_opponent:
                    wts_rows_to_delete.append(i + 2)

        # Delete related Net Tracker rows
        nt_rows_to_delete = []
        if nt:
            nt_records = nt.get_all_records()
            for i, rec in enumerate(nt_records):
                record_war_id = _to_int(rec.get("War ID"))
                if war_id is not None and record_war_id is not None:
                    if record_war_id == war_id:
                        nt_rows_to_delete.append(i + 2)
                elif rec.get("Date") == war_date and rec.get("Opponent") == war_opponent:
                    nt_rows_to_delete.append(i + 2)

        # Delete rows (in reverse order to avoid index shifting)
        for row_num in sorted(rd_rows_to_delete, reverse=True):
            rd.delete_rows(row_num, row_num)
        if wts:
            for row_num in sorted(wts_rows_to_delete, reverse=True):
                wts.delete_rows(row_num, row_num)
        if nt:
            for row_num in sorted(nt_rows_to_delete, reverse=True):
                nt.delete_rows(row_num, row_num)
        wl.delete_rows(wl_row_num, wl_row_num)

        run_sheets_with_retry(refresh_summary_sheets, spreadsheet)
        _mark_sheets_mutation("deletewar")

        await ctx.send(f"✅ Deleted war ID `{war_id}` vs {war_opponent} ({war_date}). All sheets updated.")

    except Exception as exc:
        await ctx.send(f"❌ Error: `{exc}`")


@bot.command(name="dedupewars")
async def cmd_dedupewars(ctx: commands.Context, mode: Optional[str] = None):
    """Remove duplicate wars, or preview what would be removed first."""
    try:
        spreadsheet = get_spreadsheet()
        wl = spreadsheet.worksheet("War Log")
        rd = spreadsheet.worksheet("Race Details")
        ensure_war_ids(wl)

        mode_token = _clean_command_arg(mode).lower()
        preview_mode = mode_token in {"preview", "dry", "dryrun", "dry-run", "check", "p"}

        wl_records = wl.get_all_records()
        if not wl_records:
            await ctx.send("No wars logged yet.")
            return

        signature_rows: dict[tuple[Any, ...], list[tuple[int, int]]] = {}
        for row_num, record in enumerate(wl_records, start=2):
            war_id = _to_int(record.get("War ID"))
            if war_id is None:
                continue
            signature = _war_duplicate_signature(record)
            signature_rows.setdefault(signature, []).append((row_num, war_id))

        duplicate_row_nums: list[int] = []
        duplicate_war_ids: list[int] = []
        for rows in signature_rows.values():
            if len(rows) <= 1:
                continue
            # Keep the oldest (first) row, remove later duplicates.
            for row_num, war_id in rows[1:]:
                duplicate_row_nums.append(row_num)
                duplicate_war_ids.append(war_id)

        if not duplicate_row_nums:
            await ctx.send("No duplicate wars found.")
            return

        if preview_mode:
            duplicate_ids_sorted = sorted(set(duplicate_war_ids))
            id_preview = ", ".join(str(war_id) for war_id in duplicate_ids_sorted[:15])
            suffix = "" if len(duplicate_ids_sorted) <= 15 else ", ..."
            await ctx.send(
                f"🔎 Preview only: would remove `{len(duplicate_row_nums)}` duplicate war row(s) "
                f"across `{len(duplicate_ids_sorted)}` war ID(s): `{id_preview}{suffix}`.\n"
                "Run `!dedupewars` with no args to apply."
            )
            return

        run_sheets_with_retry(backup_state, spreadsheet, "dedupewars")

        rd_records = rd.get_all_records()
        rd_rows_to_delete: list[int] = []
        duplicate_war_id_set = set(duplicate_war_ids)
        for row_num, record in enumerate(rd_records, start=2):
            if _to_int(record.get("War ID")) in duplicate_war_id_set:
                rd_rows_to_delete.append(row_num)

        for row_num in sorted(rd_rows_to_delete, reverse=True):
            rd.delete_rows(row_num, row_num)
        for row_num in sorted(duplicate_row_nums, reverse=True):
            wl.delete_rows(row_num, row_num)

        run_sheets_with_retry(refresh_summary_sheets, spreadsheet)
        _mark_sheets_mutation("dedupewars")

        removed_wars = len(duplicate_row_nums)
        removed_races = len(rd_rows_to_delete)
        await ctx.send(
            f"✅ De-dup complete. Removed `{removed_wars}` duplicate war row(s) and `{removed_races}` related race row(s)."
        )
    except Exception as exc:
        await ctx.send(f"❌ Error: `{exc}`")


@bot.command(name="warids")
async def cmd_warids(ctx: commands.Context, n: int = 10):
    """Show recent war IDs so you can target !change by ID."""
    try:
        spreadsheet = get_spreadsheet()
        wl = spreadsheet.worksheet("War Log")
        ensure_war_ids(wl)
        rows = wl.get_all_records()
        if not rows:
            await ctx.send("No wars logged yet.")
            return

        recent = rows[-max(1, min(n, 25)):]
        lines = ["```", f"{'WarID':>6}  {'Date':<17}  {'Opp':<8}  {'Net':>5}", "─" * 44]
        for row in recent:
            war_id = _to_int(row.get("War ID"))
            net = row.get("Net", "")
            net_str = f"{int(net):+d}" if isinstance(net, (int, float)) else str(net)
            lines.append(
                f"{str(war_id or ''):>6}  {str(row.get('Date', '')):<17}  {str(row.get('Opponent', '')):<8}  {net_str:>5}"
            )
        lines.append("```")
        await ctx.send("\n".join(lines))
    except Exception as exc:
        await ctx.send(f"❌ Error: `{exc}`")


@bot.command(name="change")
async def cmd_change(
    ctx: commands.Context,
    selector: Optional[str] = None,
    field: Optional[str] = None,
    *,
    value: Optional[str] = None,
):
    """
    Edit a logged war.
    Usage:
      !change oppname CK              (most recent war)
      !change teamscore 540           (most recent war)
      !change oppscore 444            (most recent war)
      !change 12 oppname CK           (specific war ID)
      !change 12 teamscore 540        (specific war ID)
      !change 12 oppscore 444         (specific war ID)
    """
    if not selector:
        await ctx.send("Usage: `!change [warID] oppname|teamscore|oppscore <value>`")
        return

    target_war_id: Optional[int] = None
    field_key: Optional[str] = None
    new_value: Optional[str] = None

    if selector.strip().lower() in {"oppname", "teamscore", "oppscore"}:
        field_key = selector.strip().lower()
        new_value = (field or "").strip()
        if value:
            new_value = f"{new_value} {value}".strip() if new_value else value.strip()
    else:
        target_war_id = _to_int(selector)
        field_key = (field or "").strip().lower()
        new_value = (value or "").strip()

    if field_key not in {"oppname", "teamscore", "oppscore"} or not new_value:
        await ctx.send("Usage: `!change [warID] oppname|teamscore|oppscore <value>`")
        return

    valid_fields = {"oppname", "teamscore", "oppscore"}
    if field_key not in valid_fields:
        await ctx.send("Valid fields: `oppname`, `teamscore`, `oppscore`")
        return

    try:
        spreadsheet = get_spreadsheet()
        wl = spreadsheet.worksheet("War Log")
        rd = spreadsheet.worksheet("Race Details")
        ensure_war_ids(wl)
        _ensure_header_column(rd, "War ID")

        try:
            wts = spreadsheet.worksheet("War Track Summary")
        except gspread.WorksheetNotFound:
            wts = None

        nt = _get_net_tracker_ws(spreadsheet)

        wl_records = wl.get_all_records()
        if not wl_records:
            await ctx.send("No wars logged yet.")
            return

        target_row_num = None
        target_war = None
        if target_war_id is None:
            target_war = wl_records[-1]
            target_row_num = len(wl_records) + 1
            target_war_id = _to_int(target_war.get("War ID"))
        else:
            for index, record in enumerate(wl_records, start=2):
                if _to_int(record.get("War ID")) == target_war_id:
                    target_war = record
                    target_row_num = index
                    break
            if target_row_num is None:
                await ctx.send(f"War ID `{target_war_id}` not found. Use `!warids` to list IDs.")
                return

        if target_war is None:
            await ctx.send("Could not resolve the target war row.")
            return

        run_sheets_with_retry(backup_state, spreadsheet, "change")

        war_date = target_war.get("Date", "")
        old_opponent = str(target_war.get("Opponent", ""))

        if field_key == "oppname":
            new_opponent = new_value.strip().upper()
            wl.update_acell(f"C{target_row_num}", new_opponent)

            rd_records = rd.get_all_records()
            for index, record in enumerate(rd_records, start=2):
                record_war_id = _to_int(record.get("War ID"))
                if target_war_id is not None and record_war_id == target_war_id:
                    rd.update_acell(f"B{index}", new_opponent)
                elif target_war_id is None and record.get("Date") == war_date and str(record.get("Opponent", "")) == old_opponent:
                    rd.update_acell(f"B{index}", new_opponent)

            if wts:
                wts_records = wts.get_all_records()
                for index, record in enumerate(wts_records, start=2):
                    record_war_id = _to_int(record.get("War ID"))
                    if target_war_id is not None and record_war_id == target_war_id:
                        wts.update_acell(f"B{index}", new_opponent)
                    elif target_war_id is None and record.get("War Date") == war_date and str(record.get("Opponent", "")) == old_opponent:
                        wts.update_acell(f"B{index}", new_opponent)

            if nt:
                nt_records = nt.get_all_records()
                for index, record in enumerate(nt_records, start=2):
                    record_war_id = _to_int(record.get("War ID"))
                    if target_war_id is not None and record_war_id == target_war_id:
                        nt.update_acell(f"B{index}", new_opponent)
                    elif target_war_id is None and record.get("Date") == war_date and str(record.get("Opponent", "")) == old_opponent:
                        nt.update_acell(f"B{index}", new_opponent)

            run_sheets_with_retry(refresh_summary_sheets, spreadsheet)
            _mark_sheets_mutation("change")
            await ctx.send(f"✅ Updated war ID `{target_war_id}` opponent to `{new_opponent}`.")
            return

        try:
            numeric_value = int(new_value.strip())
        except ValueError:
            await ctx.send("`teamscore` and `oppscore` must be whole numbers.")
            return

        if field_key == "teamscore":
            wl.update_acell(f"D{target_row_num}", str(numeric_value))
            current_opp = _to_int(target_war.get("Opp Score"))
            if current_opp is not None:
                wl.update_acell(f"F{target_row_num}", str(numeric_value - current_opp))
            run_sheets_with_retry(refresh_summary_sheets, spreadsheet)
            _mark_sheets_mutation("change")
            await ctx.send(f"✅ Updated war ID `{target_war_id}` {CLAN_NAME} score to `{numeric_value}`.")
            return

        wl.update_acell(f"E{target_row_num}", str(numeric_value))
        current_team = _to_int(target_war.get(f"{CLAN_NAME} Score"))
        if current_team is not None:
            wl.update_acell(f"F{target_row_num}", str(current_team - numeric_value))
        run_sheets_with_retry(refresh_summary_sheets, spreadsheet)
        _mark_sheets_mutation("change")
        await ctx.send(f"✅ Updated war ID `{target_war_id}` opponent score to `{numeric_value}`.")

    except Exception as exc:
        await ctx.send(f"❌ Error: `{exc}`")


async def _log_war(channel, text: str):
    """Parse text as a war result and write it to Google Sheets."""
    war = parse_war(text)
    if not war:
        return

    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: (
                run_sheets_with_retry(backup_state, get_spreadsheet(), "addwar"),
                run_sheets_with_retry(write_war, war),
            ),
        )
        _mark_sheets_mutation("addwar")

        net   = war["difference"]
        color = 0x57F287 if (net or 0) >= 0 else 0xED4245
        emoji = "✅" if (net or 0) >= 0 else "❌"

        embed = discord.Embed(title=f"{emoji} War Result Logged to Google Sheets!", color=color)
        embed.add_field(name="Opponent",             value=war["opponent"],                                    inline=True)
        embed.add_field(name=f"{CLAN_NAME} Score",   value=war["vy_score"]  if war["vy_score"]  is not None else "?", inline=True)
        embed.add_field(name="Opp Score",            value=war["opp_score"] if war["opp_score"] is not None else "?", inline=True)
        embed.add_field(name="Net",                  value=f"{net:+d}" if net is not None else "?",           inline=True)
        embed.add_field(name="Races Tracked",        value=len(war["races"]),                                 inline=True)
        embed.set_footer(text=f"Logged • {war['date']}")
        await channel.send(embed=embed)

    except Exception as exc:
        if _is_retryable_sheets_error(exc):
            await channel.send("❌ Failed to log war due to Google Sheets rate limit. Please try again in about 1 minute.")
            return
        await channel.send(f"❌ Failed to log war: `{exc}`")


def _extract_war_text_from_message(message: discord.Message) -> str:
    """Build a parseable text blob from message body + embed content."""
    text = message.content or ""
    for embed in message.embeds:
        if embed.title:
            text += "\n" + embed.title
        if embed.description:
            text += "\n" + embed.description
        for field in embed.fields:
            if field.name:
                text += "\n" + field.name
            if field.value:
                text += "\n" + field.value
    return text


def _next_unset_race(session: dict) -> Optional[int]:
    """Return next race number (smallest missing, otherwise max+1) for open-ended sessions."""
    race_numbers = sorted(session["races"].keys())
    if not race_numbers:
        return 1
    for race_number in range(1, race_numbers[-1] + 1):
        if race_number not in session["races"]:
            return race_number
    return race_numbers[-1] + 1


def _parse_positions_shorthand(shorthand: str) -> Optional[list[int]]:
    """
    Parse shorthand like:
      13478+  -> [1,3,4,7,8,11]
      137-    -> [1,3,7,8,9,10]
      138-0   -> [1,3,8,9,10,12]
      126     -> [1,2,6,10,11,12]
    0=10th, +=11th.
    Internal '-' fills the gap between adjacent places (e.g. 8-0 adds 9).
    Trailing '-' fills upward from current max place until 6 spots are present.
    If fewer than 6 spots are provided, remaining spots are auto-filled with
    the worst available positions (12,11,10,...).
    """
    text = shorthand.strip()
    if not re.fullmatch(r"[0-9+\-]{2,12}", text):
        return None

    trailing_fill = text.endswith("-")
    core = text[:-1] if trailing_fill else text

    def _token_to_place(ch: str) -> Optional[int]:
        if ch == "0":
            return 10
        if ch == "+":
            return 11
        if ch.isdigit():
            value = int(ch)
            return value if 1 <= value <= 9 else None
        return None

    positions: list[int] = []
    i = 0
    while i < len(core):
        ch = core[i]
        if ch == "-":
            # Internal range fill between neighboring explicit tokens, e.g. 8-0 -> add 9.
            if i == 0 or i + 1 >= len(core):
                return None
            prev = positions[-1] if positions else None
            nxt = _token_to_place(core[i + 1])
            if prev is None or nxt is None or nxt <= prev:
                return None
            for value in range(prev + 1, nxt):
                if value in positions:
                    return None
                positions.append(value)
            i += 1
            continue

        value = _token_to_place(ch)
        if value is None:
            return None
        if value < 1 or value > 12 or value in positions:
            return None
        positions.append(value)
        i += 1

    # Trailing '-' means fill upward from current max (e.g. 137- -> 8,9,10).
    if trailing_fill:
        start = (max(positions) + 1) if positions else 8
        while len(positions) < 6 and start <= 12:
            if start not in positions:
                positions.append(start)
            start += 1

    # Without '-', compact inputs imply worst remaining places.
    if len(positions) < 6:
        for spot in range(12, 0, -1):
            if len(positions) >= 6:
                break
            if spot not in positions:
                positions.append(spot)

    if len(positions) != 6:
        return None
    if len(set(positions)) != 6:
        return None
    return sorted(positions)


def _calc_net_from_positions(positions: list[int]) -> int:
    """Compute 6v6 net using 15-12-10-...-1 table and Quaxly net scaling."""
    team_points = sum(PLACE_POINTS[p] for p in positions)
    # In 6v6, baseline is 41 points; Quaxly-style net is doubled.
    return (team_points - 41) * 2


def _calc_points_from_positions(positions: list[int]) -> tuple[int, int]:
    """Return (vy_points, opp_points) for a single 6v6 race."""
    vy_points = sum(PLACE_POINTS[p] for p in positions)
    opp_points = 82 - vy_points
    return vy_points, opp_points


def _session_totals(session: dict) -> tuple[int, int, int, int]:
    """Return (vy_total, opp_total, net_total, current_race_num) from session races."""
    races = [session["races"][k] for k in sorted(session["races"].keys())]
    vy_total = 0
    opp_total = 0
    for race in races:
        vy, opp = _calc_points_from_positions(race["positions"])
        vy_total += vy
        opp_total += opp
    net_total = vy_total - opp_total
    current_race_num = max(session["races"].keys()) if session["races"] else 0
    return vy_total, opp_total, net_total, current_race_num


def _session_as_war_dict(session: dict) -> dict:
    """Build a war dict from current session state with auto-calculated totals."""
    race_entries = [session["races"][k] for k in sorted(session["races"].keys())]
    vy_total, opp_total, net_total, current_race_num = _session_totals(session)
    return {
        "date": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "opponent": session["opponent"],
        "vy_score": vy_total,
        "opp_score": opp_total,
        "difference": net_total,
        "num_races": current_race_num,
        "races": race_entries,
    }


async def _handle_war_shorthand_message(message: discord.Message) -> bool:
    """Handle commandless race input (track then shorthand) for active war sessions."""
    session = ACTIVE_WARS.get(message.channel.id)
    if not session:
        return False

    content = (message.content or "").strip()
    if not content or content.startswith("!"):
        return False

    # Combined form: "AH 13478+"
    combo_match = re.fullmatch(r"([A-Za-z0-9_\-]+)\s+([0-9+\-]{2,12})", content)
    if combo_match:
        track = combo_match.group(1)
        canonical_track = _canonical_track_code(track)
        if canonical_track is None:
            return False
        shorthand = combo_match.group(2)
        positions = _parse_positions_shorthand(shorthand)
        if not positions:
            await message.channel.send("Could not parse shorthand. Example: `13478+` or `137-`.")
            return True

        race_number = _next_unset_race(session)
        net = _calc_net_from_positions(positions)
        _mark_runtime_mutation("warset")
        session["races"][race_number] = {
            "race": race_number,
            "net": net,
            "track": canonical_track,
            "positions": positions,
        }
        try:
            await message.add_reaction("✅")
        except (discord.Forbidden, discord.HTTPException):
            pass
        war_preview = _session_as_war_dict(session)
        await message.channel.send(
            f"✅ R{race_number} `{canonical_track}` set from shorthand `{shorthand}` -> positions `{','.join(map(str, positions))}`, net `{net:+d}`\n"
            f"```\n{_format_war_summary_text(war_preview)}\n```"
        )
        return True

    # Shorthand-only form when pending track exists.
    if "pending_track" in session:
        positions = _parse_positions_shorthand(content)
        if not positions:
            return False

        race_number = _next_unset_race(session)
        track = session.pop("pending_track")
        net = _calc_net_from_positions(positions)
        _mark_runtime_mutation("warset")
        session["races"][race_number] = {
            "race": race_number,
            "net": net,
            "track": track,
            "positions": positions,
        }
        try:
            await message.add_reaction("✅")
        except (discord.Forbidden, discord.HTTPException):
            pass
        war_preview = _session_as_war_dict(session)
        await message.channel.send(
            f"✅ R{race_number} `{track}` set from shorthand `{content}` -> positions `{','.join(map(str, positions))}`, net `{net:+d}`\n"
            f"```\n{_format_war_summary_text(war_preview)}\n```"
        )
        return True

    # Single token track code, e.g. "AH" or "gbr"
    if re.fullmatch(r"[A-Za-z0-9_\-]{2,8}", content):
        canonical_track = _canonical_track_code(content)
        if canonical_track is None:
            return False
        session["pending_track"] = canonical_track
        next_race = _next_unset_race(session)
        try:
            await message.add_reaction("🏁")
        except (discord.Forbidden, discord.HTTPException):
            pass
        await message.channel.send(
            f"Track set for race {next_race}: `{canonical_track}`. Now send shorthand like `13478+` or `137-`."
        )
        return True

    # Shorthand positions only (no track): e.g. "13478+" or "137-"
    positions = _parse_positions_shorthand(content)
    if not positions:
        return False

    race_number = _next_unset_race(session)
    net = _calc_net_from_positions(positions)
    _mark_runtime_mutation("warset")
    session["races"][race_number] = {
        "race": race_number,
        "net": net,
        "positions": positions,
    }
    try:
        await message.add_reaction("✅")
    except (discord.Forbidden, discord.HTTPException):
        pass
    war_preview = _session_as_war_dict(session)
    await message.channel.send(
        f"✅ R{race_number} set from shorthand `{content}` -> positions `{','.join(map(str, positions))}`, net `{net:+d}`\n"
        f"```\n{_format_war_summary_text(war_preview)}\n```"
    )
    return True


def _format_war_summary_text(war: dict) -> str:
    """Render a Quaxly-style summary block for display in Discord."""
    lines = [
        f"Total Score after Race {war.get('num_races', len(war.get('races', [])))}",
        f"{CLAN_NAME}",
        f"{war.get('vy_score', '?')}",
        f"{war.get('opponent', 'Unknown')}",
        f"{war.get('opp_score', '?')}",
        "",
        "Difference",
        f"{war.get('difference', 0):+d}" if isinstance(war.get("difference"), int) else str(war.get("difference", "?")),
        "",
        "Races",
    ]
    for race in sorted(war.get("races", []), key=lambda r: r["race"]):
        pos_text = ",".join(str(p) for p in race.get("positions", []))
        track = race.get("track", "")
        track_suffix = f"   ({track})" if track else ""
        lines.append(f"{race['race']}: {race['net']:+d} | {pos_text}{track_suffix}")
    return "\n".join(lines)


@bot.command(name="vyhelp")
async def cmd_help(ctx: commands.Context):
    """Show all bot commands."""
    msg = (
        "**Vy Bot Commands**\n"
        "\n"
        "**War Logging**\n"
        "`!addwar <paste>` — Manually log a war (paste Quaxly output, or reply to Quaxly message and run `!addwar`)\n"
        "\n"
        "**Live War Session**\n"
        "`!warstart <opponent>` — Start a manual war session\n"
        "`!warset <race> <net> <positions>` — Set/update one race result (positions as 1,3,4,7,10,12)\n"
        "`!editspots <race> <positions>` — Edit finishing positions for a race (recalculates net)\n"
        "`!undorace` — Remove the last race from the active session\n"
        "`!warshow` — Preview current session in Quaxly format\n"
        "`!warcancel` — Cancel/discard the active session\n"
        "`!warend [vy_score] [opp_score]` — Finalize and export to Sheets\n"
        "\n"
        "**Stats**\n"
        "`!warstats [n]` — Last n wars (default 5)\n"
        "`!trackstats` — All tracks sorted by avg net\n"
        "`!trackstats <code>` — One specific track, e.g. `!trackstats rAF`\n"
        "`!trackstats <n>` — Tracks played at least n times\n"
        "`!warids [n]` — Recent war IDs\n"
        "\n"
        "**Editing Logged Wars**\n"
        "`!change oppname <name>` — Change opponent name (most recent war)\n"
        "`!change teamscore <score>` — Change Vy score (most recent war)\n"
        "`!change oppscore <score>` — Change opp score (most recent war)\n"
        "`!change <warID> oppname/teamscore/oppscore <value>` — Target specific war by ID\n"
        "`!deletewar [n]` — Delete nth most recent war (confirm via reaction)\n"
        "`!dedupewars [preview]` — Preview or remove duplicate logged wars\n"
        "\n"
        "**Sheets**\n"
        "`!update` — Rebuild all derived sheets from War Log\n"
        "`!setupsheets` — Re-init all sheet headers *(admin only)*\n"
        "`!undo` — Undo last mutating command\n"
        "`!redo` — Redo last undone command\n"
        "\n"
        "**Reaction Schedule**\n"
        "`!reactsetup [start] [end]` — Configure daily slot posts (Pacific, 24h, default 9–22)\n"
        "`!react` — Post/sync today's time-slot messages\n"
        "`!reactionmatch` — Sync reactions to current Pacific hour manually\n"
        "`!reactstatus` — Show scheduler config and active slots\n"
        "`!reactreset` — Delete all bot messages and repost today's slots\n"
        "`!reactdelete` — Delete tracked slot messages\n"
        "`!reactclear` — Clear reactions and disable scheduler for this channel\n"
    )
    await ctx.send(msg)


# ── Entry Point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    missing = []
    if not DISCORD_TOKEN:
        missing.append("DISCORD_TOKEN")
    if not SPREADSHEET_ID:
        missing.append("SPREADSHEET_ID")
    if not os.path.exists(CREDENTIALS_FILE):
        missing.append(f"credentials file '{CREDENTIALS_FILE}'")

    if missing:
        print("ERROR: Missing required config:")
        for m in missing:
            print(f"  • {m}")
        print("\nCheck your .env file and the README.md for setup instructions.")
        raise SystemExit(1)

    bot.run(DISCORD_TOKEN)
