#!/usr/bin/env python3
"""
Garmin Messenger ↔ Matrix (Maubot) bridge — Hybrid C.

This build integrates the agreed upgrades without changing existing behavior
beyond what is required:

- Start Garmin MainActivity only (no back/overlay-dismiss is required for startup).
- Robust echo suppression for origin=0 using a persistent mapping:
    - If origin=0 AND ota_uuid is mapped with source='matrix' => do NOT forward to Maubot (Matrix echo).
    - Else forward origin=0 messages to Maubot (so locally typed Garmin messages reach Matrix).
  Plus a small optional fallback using recent_outbound(text-hash) within a time window.

- Persistent mapping store in state.db (fresh DB OK):
    - /matrix/map_event (Maubot -> bridge) upserts mapping of garmin_ota_uuid -> matrix_event_id, with source=garmin|matrix.
    - /matrix/lookup and /matrix/lookup_reaction lookups to help Maubot implement reactions safely.
- Reaction bridging support:
    - Poll reaction_record join message to emit bridge_inbound_reaction events to Maubot.
    - Separate inbound_reaction_delivery with retry/backoff and ack.
- Retry/backoff remains exponential with jitter and max-attempt dead-letter behavior (as before).
- HTTP log_message bug already fixed.

Everything else is preserved as-is.
"""

from __future__ import annotations

import dataclasses
import hashlib
import hmac
import json
import mimetypes
import os
import random
import re
import signal
import sqlite3
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, parse_qs
import urllib.request
import base64


def env(name: str, default: Any = None, cast: Any = str):
    v = os.environ.get(name, default)
    if cast is bool:
        if v is None:
            return False
        return str(v).strip().lower() in ("1", "true", "yes", "on")
    if cast is int:
        try:
            return int(str(v))
        except Exception:
            return int(default) if default is not None else 0
    if cast is float:
        try:
            return float(str(v))
        except Exception:
            return float(default) if default is not None else 0.0
    return v


# ---- Required ----
DB_PATH = env("DB_PATH")
ROOT_DIR = env("ROOT_DIR")

# ---- Bridge state ----
STATE_DIR = env("STATE_DIR", "/var/lib/garmin-bridge")
STATE_DB = env("STATE_DB", os.path.join(STATE_DIR, "state.db"))

# ---- Polling ----
POLL_DB_SEC = env("POLL_DB_SEC", 1, int)
TAIL_LIMIT = env("TAIL_LIMIT", 250, int)
LOG_LEVEL = env("LOG_LEVEL", "INFO")
LOG_JSON = env("LOG_JSON", False, bool)

# ---- Webhook to Maubot ----
MAUBOT_WEBHOOK_URL = env("MAUBOT_WEBHOOK_URL", "")
MAUBOT_WEBHOOK_TOKEN = env("MAUBOT_WEBHOOK_TOKEN", "")

# ---- HMAC (optional) ----
HMAC_SECRET = env("HMAC_SECRET", "")
HMAC_HEADER = env("HMAC_HEADER", "X-Bridge-HMAC")

# ---- HTTP API ----
HTTP_BIND = env("HTTP_BIND", "127.0.0.1")
HTTP_PORT = env("HTTP_PORT", 8808, int)
HTTP_TOKEN = env("HTTP_TOKEN", "")

# ---- Media (proven scan strategy) ----
MAX_ATTACH_MB = env("MAX_ATTACH_MB", 25, int)
MEDIA_EXTS = tuple(
    x.strip()
    for x in env("MEDIA_EXTS", "avif,jpg,jpeg,png,webp,gif,mp4,m4a,ogg,oga,wav", str).split(",")
    if x.strip()
)

_raw_roots = env("SEARCH_ROOTS", "high,preview,low,audio,", str).split(",")
SEARCH_ROOTS = tuple(x.strip() for x in _raw_roots)  # keep "" if present

PENDING_MEDIA_MAX_SEC = env("PENDING_MEDIA_MAX_SEC", 60, int)
PENDING_MEDIA_RESCAN_SEC = env("PENDING_MEDIA_RESCAN_SEC", 1.0, float)
PENDING_MEDIA_MAX_ATTEMPTS = env("PENDING_MEDIA_MAX_ATTEMPTS", 30, int)

SQLITE_TIMEOUT = env("SQLITE_TIMEOUT", 2.5, float)

# ---- ADB/UI automation ----
ADB_BIN = env("ADB_BIN", "adb")
ADB_SERIAL = env("ADB_SERIAL", "")
ADB_TARGET = env("ADB_TARGET", "")  # e.g. redroid12:5555
GARMIN_PKG = env("GARMIN_PKG", "com.garmin.android.apps.messenger")
GARMIN_ACT = env("GARMIN_ACT", ".activity.MainActivity")

ADB_DEFAULT_SLEEP = env("ADB_DEFAULT_SLEEP", 0.8, float)
ADB_COMPOSE_SLEEP = env("ADB_COMPOSE_SLEEP", 1.2, float)
ADB_DISMISS_BACK_TWICE = env("ADB_DISMISS_BACK_TWICE", True, bool)

# ---- ADBKeyBoard / Unicode injection ----
ADBKEYBOARD_IME = env("ADBKEYBOARD_IME", "com.android.adbkeyboard/.AdbIME")
ADBKEYBOARD_B64_ACTION = env("ADBKEYBOARD_B64_ACTION", "ADB_INPUT_B64")
ADBKEYBOARD_B64_EXTRA = env("ADBKEYBOARD_B64_EXTRA", "msg")
QSB_ACTIVITY = env("QSB_ACTIVITY", "com.android.quicksearchbox/.SearchActivity")

# Existing-thread focus logic
EXISTING_THREAD_FIRST_BURST_TABS = env("EXISTING_THREAD_FIRST_BURST_TABS", 3, int)
EXISTING_THREAD_MAX_TABS = env("EXISTING_THREAD_MAX_TABS", 8, int)
EXISTING_THREAD_FALLBACK_UIA = env("EXISTING_THREAD_FALLBACK_UIA", True, bool)

# ---- Outbound DB correlation ----
OUTBOUND_DB_POLL_SEC = env("OUTBOUND_DB_POLL_SEC", 0.5, float)
OUTBOUND_DB_POLL_MAX_SEC = env("OUTBOUND_DB_POLL_MAX_SEC", 12, int)
OUTBOUND_ENQUEUE_WAIT_NEWTHREAD_SEC = env("OUTBOUND_ENQUEUE_WAIT_NEWTHREAD_SEC", 8, int)

# ---- Retry / backfill controls ----
INBOUND_MODE = env("INBOUND_MODE", "tail")  # tail | backfill
INBOUND_BACKFILL_SINCE_EPOCH = env("INBOUND_BACKFILL_SINCE_EPOCH", 0, int)

INBOUND_RETRY_BASE_SEC = env("INBOUND_RETRY_BASE_SEC", 2.0, float)
INBOUND_RETRY_MAX_SEC = env("INBOUND_RETRY_MAX_SEC", 300.0, float)
INBOUND_MAX_ATTEMPTS = env("INBOUND_MAX_ATTEMPTS", 30, int)

OUTBOUND_RETRY_BASE_SEC = env("OUTBOUND_RETRY_BASE_SEC", 2.0, float)
OUTBOUND_RETRY_MAX_SEC = env("OUTBOUND_RETRY_MAX_SEC", 300.0, float)
OUTBOUND_MAX_ATTEMPTS = env("OUTBOUND_MAX_ATTEMPTS", 30, int)

POST_OUTBOUND_RESULTS = env("POST_OUTBOUND_RESULTS", True, bool)

# ---- Reaction / echo fallback tuning ----
RECENT_OUTBOUND_WINDOW_SEC = env("RECENT_OUTBOUND_WINDOW_SEC", 180, int)  # best-effort echo suppression window

# ---- Internal ----
_LEVELS = {"DEBUG": 10, "INFO": 20, "WARN": 30, "WARNING": 30, "ERROR": 40}
_CUR_LEVEL = _LEVELS.get(str(LOG_LEVEL).upper(), 20)

_RE_GARMIN_INPUT = re.compile(r"app:id/newMessageInputEditText")


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime())


def _safe_kv(v: Any) -> str:
    try:
        s = str(v)
        return s if len(s) <= 260 else s[:260] + "…"
    except Exception:
        return "<unprintable>"


def log(level: str, msg: str, **fields: Any) -> None:
    lvl = _LEVELS.get(level.upper(), 20)
    if lvl < _CUR_LEVEL:
        return
    if LOG_JSON:
        payload = {"ts": _now_iso(), "level": level.upper(), "msg": msg}
        payload.update(fields or {})
        print(json.dumps(payload, ensure_ascii=False), flush=True)
        return
    suffix = ""
    if fields:
        suffix = " " + " ".join(f"{k}={_safe_kv(v)}" for k, v in fields.items())
    print(f"{_now_iso()} [{level.upper():5}] {msg}{suffix}", flush=True)


def _jitter() -> float:
    return random.uniform(0.0, 0.25)


def _backoff(base_sec: float, max_sec: float, attempt: int) -> float:
    # attempt is 1-based
    try:
        a = max(1, int(attempt))
    except Exception:
        a = 1
    sec = base_sec * (2 ** (a - 1))
    sec = min(float(max_sec), float(sec))
    return float(sec) + _jitter()


def garmin_db_conn() -> sqlite3.Connection:
    uri = f"file:{DB_PATH}?mode=ro&cache=shared"
    con = sqlite3.connect(uri, uri=True, timeout=SQLITE_TIMEOUT)
    con.row_factory = sqlite3.Row
    try:
        con.execute("PRAGMA read_uncommitted=1;")
    except Exception:
        pass
    return con


def state_db_conn() -> sqlite3.Connection:
    os.makedirs(STATE_DIR, exist_ok=True)
    con = sqlite3.connect(STATE_DB, timeout=10.0)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    return con


STATE_SCHEMA = r"""
CREATE TABLE IF NOT EXISTS conversation_link (
    internet_conversation_id TEXT NOT NULL PRIMARY KEY,
    matrix_room_id TEXT NOT NULL,
    account_id TEXT,
    participants_json TEXT,
    created_ts INTEGER NOT NULL,
    updated_ts INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS inbound_delivery (
    delivery_id TEXT NOT NULL PRIMARY KEY,
    internet_conversation_id TEXT NOT NULL,
    message_row_id INTEGER,
    origin INTEGER,
    sent_time INTEGER,
    sort_time INTEGER,
    from_addr TEXT,
    text TEXT,
    media_attachment_id TEXT,
    acked INTEGER NOT NULL DEFAULT 0,
    delivered_ts INTEGER,
    last_error TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_ts INTEGER,
    last_attempt_ts INTEGER
);

CREATE TABLE IF NOT EXISTS outbound_job (
    job_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    matrix_room_id TEXT NOT NULL,
    matrix_event_id TEXT NOT NULL,
    internet_conversation_id TEXT,
    kind TEXT NOT NULL,                 -- text | new_thread
    text TEXT,
    recipients_json TEXT,
    created_ts INTEGER NOT NULL,
    updated_ts INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued',
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    result_json TEXT,
    next_attempt_ts INTEGER
);
CREATE UNIQUE INDEX IF NOT EXISTS outbound_unique_event ON outbound_job(matrix_room_id, matrix_event_id);

CREATE TABLE IF NOT EXISTS outbound_correlation (
    ota_uuid TEXT NOT NULL PRIMARY KEY,
    matrix_room_id TEXT NOT NULL,
    matrix_event_id TEXT NOT NULL,
    created_ts INTEGER NOT NULL
);

-- New: persistent mapping for echo suppression + reactions
CREATE TABLE IF NOT EXISTS matrix_event_map (
  conv_id TEXT NOT NULL,
  garmin_ota_uuid TEXT NOT NULL,
  matrix_room_id TEXT NOT NULL,
  matrix_event_id TEXT NOT NULL,
  kind TEXT NOT NULL,          -- 'message' | 'reaction'
  source TEXT NOT NULL,        -- 'garmin' | 'matrix'
  emoji TEXT,                  -- reaction only
  target_ota_uuid TEXT,        -- reaction only
  created_ts INTEGER NOT NULL,
  PRIMARY KEY (conv_id, garmin_ota_uuid)
);
CREATE INDEX IF NOT EXISTS matrix_event_map_target
  ON matrix_event_map(conv_id, target_ota_uuid, emoji, created_ts);

-- New: inbound reaction deliveries (retry + ack)
CREATE TABLE IF NOT EXISTS inbound_reaction_delivery (
  delivery_id TEXT NOT NULL PRIMARY KEY,
  conv_id TEXT NOT NULL,
  reaction_ota_uuid TEXT NOT NULL,
  target_ota_uuid TEXT NOT NULL,
  operation INTEGER NOT NULL,
  emoji TEXT NOT NULL,
  acked INTEGER NOT NULL DEFAULT 0,
  attempts INTEGER NOT NULL DEFAULT 0,
  next_attempt_ts INTEGER,
  last_attempt_ts INTEGER,
  delivered_ts INTEGER,
  last_error TEXT
);

-- Optional: best-effort outbound echo suppression by (conv, text hash) within a time window
CREATE TABLE IF NOT EXISTS recent_outbound (
  matrix_room_id TEXT NOT NULL,
  matrix_event_id TEXT NOT NULL,
  internet_conversation_id TEXT,
  text_sha1 TEXT,
  created_ts INTEGER NOT NULL,
  PRIMARY KEY(matrix_room_id, matrix_event_id)
);
CREATE INDEX IF NOT EXISTS recent_outbound_lookup
  ON recent_outbound(internet_conversation_id, text_sha1, created_ts);

CREATE TABLE IF NOT EXISTS cursor (
    name TEXT NOT NULL PRIMARY KEY,
    value TEXT
);
"""


def _table_cols(con: sqlite3.Connection, table: str) -> List[str]:
    try:
        rows = con.execute(f"PRAGMA table_info({table})").fetchall()
        return [str(r[1]) for r in rows]
    except Exception:
        return []


def _ensure_migrations(con: sqlite3.Connection) -> None:
    # Older DBs created by earlier builds won't have new columns.
    cols = set(_table_cols(con, "inbound_delivery"))
    if cols:
        if "attempts" not in cols:
            con.execute("ALTER TABLE inbound_delivery ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0")
        if "next_attempt_ts" not in cols:
            con.execute("ALTER TABLE inbound_delivery ADD COLUMN next_attempt_ts INTEGER")
        if "last_attempt_ts" not in cols:
            con.execute("ALTER TABLE inbound_delivery ADD COLUMN last_attempt_ts INTEGER")

    cols2 = set(_table_cols(con, "outbound_job"))
    if cols2 and "next_attempt_ts" not in cols2:
        con.execute("ALTER TABLE outbound_job ADD COLUMN next_attempt_ts INTEGER")

    # New tables are created by schema; no legacy migration required.
    con.commit()


def init_state() -> None:
    with state_db_conn() as con:
        con.executescript(STATE_SCHEMA)
        _ensure_migrations(con)
        con.commit()


def epoch_s(ts: Any) -> Optional[int]:
    if ts is None:
        return None
    try:
        s = int(ts)
        if s > 1_000_000_000_000:
            s //= 1000
        return s
    except Exception:
        return None


def size_mb(path: str) -> float:
    try:
        return os.path.getsize(path) / (1024 * 1024)
    except Exception:
        return 0.0


def sha1_hex(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()


def adb_text_escape(s: str) -> str:
    s = (s or "").replace("\n", " ").replace("\r", " ")
    return s.replace(" ", "%s")


# ---- HMAC helpers ----

def _hmac_hexdigest(secret: str, body: bytes) -> str:
    return hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()


def _hmac_ok(headers: Dict[str, str], body: bytes) -> bool:
    """
    If HMAC_SECRET is set, require a valid HMAC header.
    """
    if not HMAC_SECRET:
        return True
    got = (headers.get(HMAC_HEADER) or headers.get(HMAC_HEADER.lower()) or "").strip()
    if not got:
        return False
    exp = _hmac_hexdigest(HMAC_SECRET, body)
    return hmac.compare_digest(got, exp)


# ---- Proven media scan (attachment_id only) ----

def media_candidates_by_scan(root_dir: str, attachment_id: str) -> List[str]:
    out: List[str] = []
    if not attachment_id:
        return out
    for sub in SEARCH_ROOTS:
        d = os.path.join(root_dir, sub)  # sub may be "" (root_dir)
        for ext in MEDIA_EXTS:
            p = os.path.join(d, f"{attachment_id}.{ext}")
            if os.path.isfile(p):
                out.append(p)
    return out


def resolve_media(root_dir: str, attachment_id: str) -> Tuple[Optional[str], List[str]]:
    cands = media_candidates_by_scan(root_dir, attachment_id)
    best = cands[0] if cands else None
    return best, cands


def _guess_mime(path: Optional[str]) -> str:
    """
    Guess MIME type from filename extension, with small overrides for known
    edge cases and better Maubot interoperability.
    """
    if not path:
        return "application/octet-stream"

    ext = os.path.splitext(path)[1].lower().lstrip(".")
    overrides = {
        "avif": "image/avif",
        "ogg": "audio/ogg",
        "oga": "audio/ogg",
        "m4a": "audio/mp4",
    }
    if ext in overrides:
        return overrides[ext]

    mt, _ = mimetypes.guess_type(path)
    return mt or "application/octet-stream"


def post_json(url: str, payload: Dict[str, Any], bearer: str = "", timeout: int = 20, sign_hmac: bool = True) -> Tuple[int, str]:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/json; charset=utf-8")
    if bearer:
        req.add_header("Authorization", f"Bearer {bearer}")
    if sign_hmac and HMAC_SECRET:
        req.add_header(HMAC_HEADER, _hmac_hexdigest(HMAC_SECRET, body))
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        b = resp.read(8192)
        return int(resp.status), b.decode("utf-8", errors="replace")


def load_cursor(name: str, default: str = "") -> str:
    with state_db_conn() as con:
        r = con.execute("SELECT value FROM cursor WHERE name=? LIMIT 1", (name,)).fetchone()
        return str(r["value"]) if r and r["value"] is not None else default


def save_cursor(name: str, value: str) -> None:
    with state_db_conn() as con:
        con.execute(
            "INSERT INTO cursor(name,value) VALUES(?,?) ON CONFLICT(name) DO UPDATE SET value=excluded.value",
            (name, value),
        )
        con.commit()


def upsert_link(internet_conversation_id: str, matrix_room_id: str, account_id: Optional[str] = None, participants_json: Optional[str] = None) -> None:
    now = int(time.time())
    with state_db_conn() as con:
        con.execute(
            """
            INSERT INTO conversation_link(internet_conversation_id, matrix_room_id, account_id, participants_json, created_ts, updated_ts)
            VALUES(?,?,?,?,?,?)
            ON CONFLICT(internet_conversation_id) DO UPDATE SET
              matrix_room_id=excluded.matrix_room_id,
              account_id=COALESCE(excluded.account_id, conversation_link.account_id),
              participants_json=COALESCE(excluded.participants_json, conversation_link.participants_json),
              updated_ts=excluded.updated_ts
            """,
            (internet_conversation_id, matrix_room_id, account_id, participants_json, now, now),
        )
        con.commit()


def get_link_by_conversation(internet_conversation_id: str) -> Optional[sqlite3.Row]:
    with state_db_conn() as con:
        return con.execute("SELECT * FROM conversation_link WHERE internet_conversation_id=? LIMIT 1", (internet_conversation_id,)).fetchone()


# ---- New: matrix event mapping helpers ----

def upsert_matrix_event_map(
    conv_id: str,
    garmin_ota_uuid: str,
    matrix_room_id: str,
    matrix_event_id: str,
    kind: str,
    source: str,
    emoji: Optional[str] = None,
    target_ota_uuid: Optional[str] = None,
) -> None:
    now = int(time.time())
    with state_db_conn() as con:
        con.execute(
            """
            INSERT INTO matrix_event_map(
              conv_id, garmin_ota_uuid, matrix_room_id, matrix_event_id, kind, source, emoji, target_ota_uuid, created_ts
            ) VALUES(?,?,?,?,?,?,?,?,?)
            ON CONFLICT(conv_id, garmin_ota_uuid) DO UPDATE SET
              matrix_room_id=excluded.matrix_room_id,
              matrix_event_id=excluded.matrix_event_id,
              kind=excluded.kind,
              source=excluded.source,
              emoji=excluded.emoji,
              target_ota_uuid=excluded.target_ota_uuid,
              created_ts=excluded.created_ts
            """,
            (
                str(conv_id),
                str(garmin_ota_uuid),
                str(matrix_room_id),
                str(matrix_event_id),
                str(kind),
                str(source),
                emoji,
                target_ota_uuid,
                now,
            ),
        )
        con.commit()


def lookup_matrix_event_for_ota(conv_id: str, garmin_ota_uuid: str) -> Optional[sqlite3.Row]:
    with state_db_conn() as con:
        return con.execute(
            "SELECT * FROM matrix_event_map WHERE conv_id=? AND garmin_ota_uuid=? LIMIT 1",
            (str(conv_id), str(garmin_ota_uuid)),
        ).fetchone()


def lookup_reaction_event_by_target(conv_id: str, target_ota_uuid: str, emoji: str) -> Optional[sqlite3.Row]:
    # Return the latest reaction mapping for (target, emoji)
    with state_db_conn() as con:
        return con.execute(
            """
            SELECT *
            FROM matrix_event_map
            WHERE conv_id=?
              AND kind='reaction'
              AND target_ota_uuid=?
              AND emoji=?
            ORDER BY created_ts DESC
            LIMIT 1
            """,
            (str(conv_id), str(target_ota_uuid), str(emoji)),
        ).fetchone()


# ---- Inbound delivery helpers ----

def is_acked(delivery_id: str) -> bool:
    with state_db_conn() as con:
        r = con.execute("SELECT acked FROM inbound_delivery WHERE delivery_id=? LIMIT 1", (delivery_id,)).fetchone()
        return bool(r and int(r["acked"] or 0) == 1)


def ack_delivery(delivery_id: str) -> None:
    now = int(time.time())
    with state_db_conn() as con:
        con.execute("UPDATE inbound_delivery SET acked=1, delivered_ts=?, last_error=NULL WHERE delivery_id=?", (now, delivery_id))
        con.commit()


def note_delivery_error(delivery_id: str, err: str) -> None:
    with state_db_conn() as con:
        con.execute("UPDATE inbound_delivery SET last_error=? WHERE delivery_id=?", (err[:800], delivery_id))
        con.commit()


def schedule_inbound_retry(delivery_id: str, err: str) -> None:
    now = int(time.time())
    with state_db_conn() as con:
        r = con.execute("SELECT attempts FROM inbound_delivery WHERE delivery_id=? LIMIT 1", (delivery_id,)).fetchone()
        attempts = int(r["attempts"] or 0) + 1 if r else 1
        delay = _backoff(INBOUND_RETRY_BASE_SEC, INBOUND_RETRY_MAX_SEC, attempts)
        next_ts = now + int(delay)
        con.execute(
            "UPDATE inbound_delivery SET attempts=?, last_attempt_ts=?, next_attempt_ts=?, last_error=? WHERE delivery_id=?",
            (attempts, now, next_ts, err[:800], delivery_id),
        )
        con.commit()


def upsert_inbound_record(delivery: Dict[str, Any]) -> None:
    with state_db_conn() as con:
        con.execute(
            """
            INSERT INTO inbound_delivery(
              delivery_id, internet_conversation_id, message_row_id, origin, sent_time, sort_time, from_addr, text,
              media_attachment_id, next_attempt_ts
            ) VALUES(?,?,?,?,?,?,?,?,?,NULL)
            ON CONFLICT(delivery_id) DO UPDATE SET
              internet_conversation_id=excluded.internet_conversation_id,
              message_row_id=COALESCE(excluded.message_row_id, inbound_delivery.message_row_id),
              origin=excluded.origin,
              sent_time=excluded.sent_time,
              sort_time=excluded.sort_time,
              from_addr=excluded.from_addr,
              text=excluded.text,
              media_attachment_id=excluded.media_attachment_id
            """,
            (
                delivery["delivery_id"],
                delivery.get("internet_conversation_id"),
                delivery.get("message_row_id"),
                delivery.get("origin"),
                delivery.get("sent_time"),
                delivery.get("sort_time"),
                delivery.get("from"),
                delivery.get("text"),
                delivery.get("media_attachment_id"),
            ),
        )
        con.commit()


# ---- New: inbound reaction delivery helpers ----

def is_reaction_acked(delivery_id: str) -> bool:
    with state_db_conn() as con:
        r = con.execute("SELECT acked FROM inbound_reaction_delivery WHERE delivery_id=? LIMIT 1", (delivery_id,)).fetchone()
        return bool(r and int(r["acked"] or 0) == 1)


def ack_reaction_delivery(delivery_id: str) -> None:
    now = int(time.time())
    with state_db_conn() as con:
        con.execute(
            "UPDATE inbound_reaction_delivery SET acked=1, delivered_ts=?, last_error=NULL WHERE delivery_id=?",
            (now, delivery_id),
        )
        con.commit()


def schedule_reaction_retry(delivery_id: str, err: str) -> None:
    now = int(time.time())
    with state_db_conn() as con:
        r = con.execute("SELECT attempts FROM inbound_reaction_delivery WHERE delivery_id=? LIMIT 1", (delivery_id,)).fetchone()
        attempts = int(r["attempts"] or 0) + 1 if r else 1
        delay = _backoff(INBOUND_RETRY_BASE_SEC, INBOUND_RETRY_MAX_SEC, attempts)
        next_ts = now + int(delay)
        con.execute(
            """
            UPDATE inbound_reaction_delivery
            SET attempts=?, last_attempt_ts=?, next_attempt_ts=?, last_error=?
            WHERE delivery_id=?
            """,
            (attempts, now, next_ts, err[:800], delivery_id),
        )
        con.commit()


def upsert_inbound_reaction_record(d: Dict[str, Any]) -> None:
    # idempotent insert; keep original attempts/next_attempt_ts if already exists
    with state_db_conn() as con:
        con.execute(
            """
            INSERT INTO inbound_reaction_delivery(
              delivery_id, conv_id, reaction_ota_uuid, target_ota_uuid, operation, emoji, acked,
              attempts, next_attempt_ts, last_attempt_ts, delivered_ts, last_error
            ) VALUES(?,?,?,?,?,?,0,0,NULL,NULL,NULL,NULL)
            ON CONFLICT(delivery_id) DO UPDATE SET
              conv_id=excluded.conv_id,
              reaction_ota_uuid=excluded.reaction_ota_uuid,
              target_ota_uuid=excluded.target_ota_uuid,
              operation=excluded.operation,
              emoji=excluded.emoji
            """,
            (
                d["delivery_id"],
                d["conv_id"],
                d["reaction_ota_uuid"],
                d["target_ota_uuid"],
                int(d["operation"]),
                d["emoji"],
            ),
        )
        con.commit()


# ---- Outbound correlation (existing) ----

def add_outbound_correlation(ota_uuid: str, matrix_room_id: str, matrix_event_id: str) -> None:
    now = int(time.time())
    with state_db_conn() as con:
        con.execute(
            "INSERT INTO outbound_correlation(ota_uuid, matrix_room_id, matrix_event_id, created_ts) VALUES(?,?,?,?) "
            "ON CONFLICT(ota_uuid) DO NOTHING",
            (ota_uuid, matrix_room_id, matrix_event_id, now),
        )
        con.commit()


def lookup_outbound_by_ota(ota_uuid: str) -> Optional[sqlite3.Row]:
    with state_db_conn() as con:
        return con.execute("SELECT * FROM outbound_correlation WHERE ota_uuid=? LIMIT 1", (ota_uuid,)).fetchone()


# ---- Optional: recent outbound (echo fallback) ----

def record_recent_outbound(matrix_room_id: str, matrix_event_id: str, conv_id: Optional[str], text: Optional[str]) -> None:
    now = int(time.time())
    txt = (text or "").strip()
    tsh = sha1_hex(txt) if txt else None
    with state_db_conn() as con:
        con.execute(
            """
            INSERT INTO recent_outbound(matrix_room_id, matrix_event_id, internet_conversation_id, text_sha1, created_ts)
            VALUES(?,?,?,?,?)
            ON CONFLICT(matrix_room_id, matrix_event_id) DO UPDATE SET
              internet_conversation_id=excluded.internet_conversation_id,
              text_sha1=excluded.text_sha1,
              created_ts=excluded.created_ts
            """,
            (str(matrix_room_id), str(matrix_event_id), (str(conv_id) if conv_id else None), tsh, now),
        )
        con.commit()


def is_recent_outbound_echo(conv_id: str, text: str) -> bool:
    conv = (conv_id or "").strip()
    txt = (text or "").strip()
    if not conv or not txt:
        return False
    tsh = sha1_hex(txt)
    now = int(time.time())
    cutoff = now - int(RECENT_OUTBOUND_WINDOW_SEC)
    with state_db_conn() as con:
        r = con.execute(
            """
            SELECT 1
            FROM recent_outbound
            WHERE internet_conversation_id=?
              AND text_sha1=?
              AND created_ts >= ?
            ORDER BY created_ts DESC
            LIMIT 1
            """,
            (conv, tsh, cutoff),
        ).fetchone()
        return bool(r)


# ---- Outbound queue (existing) ----

def enqueue_outbound(job: Dict[str, Any]) -> int:
    now = int(time.time())
    with state_db_conn() as con:
        con.execute(
            """
            INSERT INTO outbound_job(
              matrix_room_id, matrix_event_id, internet_conversation_id, kind, text, recipients_json,
              created_ts, updated_ts, status, attempts, next_attempt_ts
            ) VALUES(?,?,?,?,?,?,?,?, 'queued', 0, NULL)
            ON CONFLICT(matrix_room_id, matrix_event_id) DO NOTHING
            """,
            (
                job["matrix_room_id"],
                job["matrix_event_id"],
                job.get("internet_conversation_id"),
                job["kind"],
                job.get("text"),
                job.get("recipients_json"),
                now,
                now,
            ),
        )
        con.commit()
        r = con.execute(
            "SELECT job_id FROM outbound_job WHERE matrix_room_id=? AND matrix_event_id=? LIMIT 1",
            (job["matrix_room_id"], job["matrix_event_id"]),
        ).fetchone()

        jid = int(r["job_id"]) if r else 0

    # Record recent outbound (best-effort echo fallback)
    try:
        record_recent_outbound(
            str(job["matrix_room_id"]),
            str(job["matrix_event_id"]),
            job.get("internet_conversation_id"),
            job.get("text"),
        )
    except Exception:
        pass

    return jid


def next_outbound_job() -> Optional[sqlite3.Row]:
    now = int(time.time())
    with state_db_conn() as con:
        return con.execute(
            """
            SELECT *
            FROM outbound_job
            WHERE status IN ('queued','failed')
              AND (next_attempt_ts IS NULL OR next_attempt_ts <= ?)
            ORDER BY updated_ts ASC, job_id ASC
            LIMIT 1
            """,
            (now,),
        ).fetchone()


def mark_outbound_sending(job_id: int) -> None:
    now = int(time.time())
    with state_db_conn() as con:
        con.execute("UPDATE outbound_job SET status='sending', updated_ts=? WHERE job_id=?", (now, job_id))
        con.commit()


def mark_outbound_failed(job_id: int, err: str) -> None:
    now = int(time.time())
    with state_db_conn() as con:
        r = con.execute("SELECT attempts FROM outbound_job WHERE job_id=? LIMIT 1", (job_id,)).fetchone()
        attempts = int(r["attempts"] or 0) + 1 if r else 1
        if attempts >= int(OUTBOUND_MAX_ATTEMPTS):
            next_ts = None
            status = "dead"
        else:
            delay = _backoff(OUTBOUND_RETRY_BASE_SEC, OUTBOUND_RETRY_MAX_SEC, attempts)
            next_ts = now + int(delay)
            status = "failed"
        con.execute(
            "UPDATE outbound_job SET status=?, updated_ts=?, attempts=?, last_error=?, next_attempt_ts=? WHERE job_id=?",
            (status, now, attempts, err[:800], next_ts, job_id),
        )
        con.commit()


def mark_outbound_sent(job_id: int, result: Optional[Dict[str, Any]] = None) -> None:
    now = int(time.time())
    res_json = json.dumps(result, ensure_ascii=False) if result is not None else None
    with state_db_conn() as con:
        con.execute(
            "UPDATE outbound_job SET status='sent', updated_ts=?, last_error=NULL, result_json=?, next_attempt_ts=NULL WHERE job_id=?",
            (now, res_json, job_id),
        )
        con.commit()


def get_job_result(job_id: int) -> Optional[Dict[str, Any]]:
    with state_db_conn() as con:
        r = con.execute("SELECT result_json FROM outbound_job WHERE job_id=? LIMIT 1", (job_id,)).fetchone()
        if not r or not r["result_json"]:
            return None
        try:
            return json.loads(r["result_json"])
        except Exception:
            return None


# ---- ADB/UI helpers ----

def _adb_base_cmd() -> List[str]:
    cmd = [str(ADB_BIN)]
    if ADB_SERIAL:
        cmd += ["-s", str(ADB_SERIAL)]
    return cmd


def adb(*args: str, timeout: int = 25) -> subprocess.CompletedProcess:
    cmd = _adb_base_cmd() + list(args)
    log("DEBUG", "adb", cmd=" ".join(cmd))
    return subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)


def adb_shell(cmd: str, timeout: int = 25) -> None:
    p = adb("shell", cmd, timeout=timeout)
    if p.returncode != 0:
        raise RuntimeError(f"adb shell failed rc={p.returncode} stderr={p.stderr.decode(errors='replace')[:600]}")
    return None


def adb_shell_out(cmd: str, timeout: int = 25) -> str:
    p = adb("shell", cmd, timeout=timeout)
    if p.returncode != 0:
        raise RuntimeError(f"adb shell failed rc={p.returncode} stderr={p.stderr.decode(errors='replace')[:600]}")
    return (p.stdout or b"").decode("utf-8", errors="replace")


def adb_sleep(sec: float) -> None:
    time.sleep(max(0.0, sec))


def adb_dismiss_overlays() -> None:
    if not ADB_DISMISS_BACK_TWICE:
        return
    try:
        adb_shell("input keyevent 4; input keyevent 4", timeout=10)
    except Exception:
        pass


def ensure_adb_connected() -> None:
    """
    Make ADB connectivity robust across container restarts.
    - Start the adb server if needed
    - Optionally `adb connect $ADB_TARGET` (tcpip)
    - Verify `adb devices` contains at least one device (or the configured -s serial works)
    """
    # Start server (idempotent)
    try:
        p = adb("start-server", timeout=15)
        if p.returncode != 0:
            log("WARN", "adb start-server failed", stderr=p.stderr.decode(errors="replace")[:200])
    except Exception as e:
        log("WARN", "adb start-server exception", err=str(e))

    # If user configured ADB_TARGET, try to connect with retries.
    if ADB_TARGET:
        for i in range(1, 8):
            try:
                p = adb("connect", str(ADB_TARGET), timeout=15)
                out = (p.stdout or b"").decode(errors="replace")
                err = (p.stderr or b"").decode(errors="replace")
                if p.returncode == 0 and ("connected" in out.lower() or "already connected" in out.lower()):
                    log("INFO", "adb connected", target=str(ADB_TARGET))
                    break
                log("WARN", "adb connect failed", target=str(ADB_TARGET), attempt=i, out=out.strip()[:200], err=err.strip()[:200])
            except Exception as e:
                log("WARN", "adb connect exception", target=str(ADB_TARGET), attempt=i, err=str(e))
            time.sleep(_backoff(0.5, 6.0, i))

    # Verify devices (best-effort)
    try:
        p = adb("devices", timeout=15)
        txt = (p.stdout or b"").decode(errors="replace")
        if p.returncode != 0:
            log("WARN", "adb devices failed", stderr=(p.stderr or b"").decode(errors="replace")[:200])
            return
        lines = [ln.strip() for ln in txt.splitlines() if ln.strip()]
        any_device = any("\tdevice" in ln for ln in lines[1:]) if len(lines) >= 2 else False
        if not any_device and not ADB_SERIAL:
            log("WARN", "no adb devices attached (yet)", devices=txt.strip()[:400])
    except Exception as e:
        log("WARN", "adb devices exception", err=str(e))


def ui_dump(path: str = "/sdcard/ui.xml") -> None:
    adb_shell(f"uiautomator dump {path} >/dev/null", timeout=20)


def ui_tap_center_of_node_grep(pattern: str) -> None:
    ui_dump()
    pat = pattern.replace("'", r"'\''")
    sh = f"""sh -lc '
line=$(sed "s/></>\\n</g" /sdcard/ui.xml | grep -F "{pat}" | head -n 1)
test -n "$line" || exit 2
b=$(echo "$line" | sed -n "s/.*bounds=\\\"\\[\\([0-9]*\\),\\([0-9]*\\)\\]\\[\\([0-9]*\\),\\([0-9]*\\)\\]\\\".*/\\1 \\2 \\3 \\4/p")
set -- $b
x=$(( ( $1 + $3 ) / 2 ))
y=$(( ( $2 + $4 ) / 2 ))
input tap $x $y
'"""
    adb_shell(sh, timeout=25)


def ui_tap_first_garmin_edittext() -> None:
    ui_dump()
    sh = r"""sh -lc '
line=$(sed "s/></>\n</g" /sdcard/ui.xml   | grep "package=\"com.garmin.android.apps.messenger\""   | grep "class=\"android.widget.EditText\""   | head -n 1)
test -n "$line" || exit 2
b=$(echo "$line" | sed -n "s/.*bounds=\"\[\([0-9]*\),\([0-9]*\)\]\[\([0-9]*\),\([0-9]*\)\]\".*/\1 \2 \3 \4/p")
set -- $b
x=$(( ( $1 + $3 ) / 2 ))
y=$(( ( $2 + $4 ) / 2 ))
input tap $x $y
'"""
    adb_shell(sh, timeout=25)


def start_activity_view(uri: str) -> None:
    adb_shell(f'am start -a android.intent.action.VIEW -d "{uri}" -n {GARMIN_PKG}/{GARMIN_ACT} -f 0x10000000', timeout=25)


def start_garmin_main_activity() -> None:
    # Start Garmin main activity only (no overlay dismiss required)
    adb_shell(f"am start --user 0 -n {GARMIN_PKG}/{GARMIN_ACT}", timeout=25)


# ---- ADBKeyBoard / keyevent navigation / focus verification ----

def adb_ime_enable_set() -> None:
    # Safe to call repeatedly
    adb_shell(f"ime enable {ADBKEYBOARD_IME}", timeout=15)
    adb_shell(f"ime set {ADBKEYBOARD_IME}", timeout=15)


def adb_qsb_trigger() -> None:
    # Kick IME via Quick Search Box (run before every outbound send)
    adb_shell(f"am start --user 0 -n {QSB_ACTIVITY}", timeout=20)
    adb_sleep(1.0)
    adb_shell("input keyevent 4", timeout=10)  # BACK
    adb_sleep(0.2)


def adb_b64_input(text: str) -> None:
    b64 = base64.b64encode((text or "").encode("utf-8")).decode("ascii")
    adb_shell(f'am broadcast -a {ADBKEYBOARD_B64_ACTION} --es {ADBKEYBOARD_B64_EXTRA} "{b64}"', timeout=20)


def keyevent(code: int, sleep_s: float = 0.12) -> None:
    adb_shell(f"input keyevent {int(code)}", timeout=10)
    adb_sleep(sleep_s)


def nav_tab(n: int, sleep_s: float = 0.08) -> None:
    for _ in range(int(n)):
        keyevent(61, sleep_s)


def nav_enter(n: int = 1, sleep_s: float = 0.18) -> None:
    for _ in range(int(n)):
        keyevent(66, sleep_s)


def is_garmin_input_served() -> bool:
    out = adb_shell_out("dumpsys input_method", timeout=12)
    return bool(_RE_GARMIN_INPUT.search(out))


def focus_existing_thread_input() -> int:
    """
    Existing-thread focus strategy:
    - Run 3x TAB first (configurable) without checking.
    - Then TAB one-by-one, checking each time.
    - Fallback to UIA tap (optional) if still not in input after N.
    Returns the number of TAB keyevents sent (not counting UIA fallback).
    """
    tabs = 0
    max_tabs = max(1, int(EXISTING_THREAD_MAX_TABS))
    first_burst = max(0, min(int(EXISTING_THREAD_FIRST_BURST_TABS), max_tabs))

    if first_burst:
        nav_tab(first_burst)
        tabs += first_burst

    if is_garmin_input_served():
        return tabs

    while tabs < max_tabs:
        nav_tab(1)
        tabs += 1
        if is_garmin_input_served():
            return tabs

    if EXISTING_THREAD_FALLBACK_UIA:
        try:
            ui_tap_center_of_node_grep("com.garmin.android.apps.messenger:id/newMessageInputEditText")
            adb_sleep(0.2)
            if is_garmin_input_served():
                return tabs
        except Exception as e:
            log("WARN", "existing-thread UIA fallback failed", err=str(e))

    raise RuntimeError(f"Could not focus input after {tabs} TABs")


def send_to_existing_thread(thread_id: int, msg: str) -> None:
    """
    Improved existing-thread send:
    - Kick IME (QSB) before every send.
    - Use TAB navigation + dumpsys input_method verification to reach input.
    - Inject Unicode via ADBKeyBoard Base64.
    - Send via TAB + ENTER (as verified).
    """
    adb_dismiss_overlays()

    # Ensure ADBKeyBoard is available, then kick IME (EVERY send)
    adb_ime_enable_set()
    adb_qsb_trigger()

    start_activity_view(f"inreach://messageThread/{int(thread_id)}")
    adb_sleep(ADB_DEFAULT_SLEEP)

    tabs = focus_existing_thread_input()
    log("INFO", "focused existing-thread input", thread_id=int(thread_id), tabs=tabs)

    adb_b64_input(msg)
    adb_sleep(0.25)

    # Move to Send button and send
    nav_tab(1)
    nav_enter(1)


def compose_new_thread_and_send(recipients: List[str], msg: str) -> None:
    """
    Keep your existing (UIA-heavy) approach to reliably reach the message input in a new thread (rare path),
    but use ADBKeyBoard Base64 for the message body and keyevents TAB+ENTER to send.
    """
    if not recipients:
        raise ValueError("recipients must not be empty")

    adb_dismiss_overlays()

    # Ensure ADBKeyBoard is available, then kick IME (EVERY send)
    adb_ime_enable_set()
    adb_qsb_trigger()

    start_activity_view("inreach://composedevicemessage/")
    adb_sleep(ADB_COMPOSE_SLEEP)

    ui_tap_first_garmin_edittext()

    for r in recipients:
        # ASCII-only input for recipient is fine
        adb_shell(f'input text "{adb_text_escape(str(r))}"', timeout=20)
        adb_shell("input keyevent 66", timeout=10)
        adb_sleep(0.3)
        adb_shell("input keyevent 61", timeout=10)
        adb_sleep(0.2)
        adb_shell("input keyevent 23", timeout=10)
        adb_sleep(0.5)

    # Use existing logic to reach the message input field
    ui_tap_center_of_node_grep("com.garmin.android.apps.messenger:id/newMessageInputEditText")
    adb_sleep(0.2)

    # Kick IME again right before Base64 inject (still "before every send")
    adb_qsb_trigger()
    adb_b64_input(msg)
    adb_sleep(0.25)

    # Send: TAB -> ENTER
    nav_tab(1)
    nav_enter(1)


# ---- Outbound correlation ----

def snapshot_conv_cursor(con: sqlite3.Connection, internet_conversation_id: str) -> Tuple[int, int]:
    r = con.execute("SELECT IFNULL(MAX(sort_time),0) AS st, IFNULL(MAX(id),0) AS mid FROM message WHERE internet_conversation_id = ?", (internet_conversation_id,)).fetchone()
    return (int(r["st"] or 0), int(r["mid"] or 0))


def snapshot_global_cursor(con: sqlite3.Connection) -> Tuple[int, int]:
    r = con.execute("SELECT IFNULL(MAX(sort_time),0) AS st, IFNULL(MAX(id),0) AS mid FROM message").fetchone()
    return (int(r["st"] or 0), int(r["mid"] or 0))


def find_new_outgoing_by_text(con: sqlite3.Connection, internet_conversation_id: Optional[str], after_sort_time: int, after_id: int, text: str) -> Tuple[Optional[str], Optional[str], Optional[int]]:
    deadline = time.time() + int(OUTBOUND_DB_POLL_MAX_SEC)
    text = text or ""
    relaxed = text.replace("\\", "")
    while time.time() < deadline:
        if internet_conversation_id:
            rows = con.execute(
                """
                SELECT ota_uuid, internet_conversation_id, id, sort_time, text
                FROM message
                WHERE internet_conversation_id = ?
                  AND origin = 0
                  AND ( sort_time > ? OR (sort_time = ? AND id > ?) )
                ORDER BY sort_time DESC, id DESC
                LIMIT 30
                """,
                (internet_conversation_id, after_sort_time, after_sort_time, after_id),
            ).fetchall()
        else:
            rows = con.execute(
                """
                SELECT ota_uuid, internet_conversation_id, id, sort_time, text
                FROM message
                WHERE origin = 0
                  AND ( sort_time > ? OR (sort_time = ? AND id > ?) )
                ORDER BY sort_time DESC, id DESC
                LIMIT 80
                """,
                (after_sort_time, after_sort_time, after_id),
            ).fetchall()

        for r in rows:
            if (r["text"] or "") == text and r["ota_uuid"]:
                return (str(r["ota_uuid"]), str(r["internet_conversation_id"] or ""), int(r["id"]))
        for r in rows:
            if (r["text"] or "").replace("\\", "") == relaxed and r["ota_uuid"]:
                return (str(r["ota_uuid"]), str(r["internet_conversation_id"] or ""), int(r["id"]))

        time.sleep(max(0.05, float(OUTBOUND_DB_POLL_SEC)))
    return (None, None, None)


def resolve_thread_id_for_conversation(con: sqlite3.Connection, internet_conversation_id: str) -> Optional[int]:
    r = con.execute("SELECT message_thread_id FROM message WHERE internet_conversation_id=? ORDER BY sort_time DESC, id DESC LIMIT 1", (internet_conversation_id,)).fetchone()
    if not r:
        return None
    try:
        return int(r["message_thread_id"]) if r["message_thread_id"] is not None else None
    except Exception:
        return None


# ---- Inbound ----

def compute_delivery_id(row: sqlite3.Row) -> str:
    # Prefer robust UUID-like identifiers if present
    if row["ota_uuid"]:
        return str(row["ota_uuid"])
    if "internet_message_id" in row.keys() and row["internet_message_id"]:
        return str(row["internet_message_id"])
    # Deterministic fallback (should be rare)
    conv = str(row["internet_conversation_id"] or "")
    mid = str(row["id"])
    sent = str(row["sent_time"] or row["sort_time"] or "")
    txt = str(row["text"] or "")
    att = str(row["media_attachment_id"] or "")
    return "fb_" + sha1_hex("|".join([conv, mid, sent, txt, att]))[:24]


def fetch_new_messages(con: sqlite3.Connection, after_sort_time: int, after_id: int) -> List[sqlite3.Row]:
    q = """
        SELECT
          id,
          message_thread_id,
          internet_message_id,
          internet_conversation_id,
          origin,
          status,
          web_transfer_state,
          "from" as from_addr,
          text,
          sort_time,
          sent_time,
          ota_uuid,
          media_attachment_id,
          latitude,
          longitude,
          altitude
        FROM message
        WHERE internet_conversation_id IS NOT NULL
          AND (
                sort_time > ?
             OR (sort_time = ? AND id > ?)
          )
        ORDER BY sort_time ASC, id ASC
        LIMIT ?
    """
    return con.execute(q, (after_sort_time, after_sort_time, after_id, TAIL_LIMIT)).fetchall()


def fetch_message_by_id(con: sqlite3.Connection, message_id: int) -> Optional[sqlite3.Row]:
    q = """
        SELECT
          id,
          message_thread_id,
          internet_message_id,
          internet_conversation_id,
          origin,
          status,
          web_transfer_state,
          "from" as from_addr,
          text,
          sort_time,
          sent_time,
          ota_uuid,
          media_attachment_id,
          latitude,
          longitude,
          altitude
        FROM message
        WHERE id = ?
        LIMIT 1
    """
    return con.execute(q, (int(message_id),)).fetchone()


def fetch_backfill_messages(con: sqlite3.Connection, since_epoch: int, limit: int = 500) -> List[sqlite3.Row]:
    # Backfill oldest-first. If since_epoch is set, bound by sort_time/sent_time >= since.
    if since_epoch and since_epoch > 0:
        q = """
            SELECT
              id,
              message_thread_id,
              internet_message_id,
              internet_conversation_id,
              origin,
              status,
              web_transfer_state,
              "from" as from_addr,
              text,
              sort_time,
              sent_time,
              ota_uuid,
              media_attachment_id,
              latitude,
              longitude,
              altitude
            FROM message
            WHERE internet_conversation_id IS NOT NULL
              AND (IFNULL(sent_time, sort_time) >= ?)
            ORDER BY sort_time ASC, id ASC
            LIMIT ?
        """
        return con.execute(q, (int(since_epoch), int(limit))).fetchall()

    q = """
        SELECT
          id,
          message_thread_id,
          internet_message_id,
          internet_conversation_id,
          origin,
          status,
          web_transfer_state,
          "from" as from_addr,
          text,
          sort_time,
          sent_time,
          ota_uuid,
          media_attachment_id,
          latitude,
          longitude,
          altitude
        FROM message
        WHERE internet_conversation_id IS NOT NULL
        ORDER BY sort_time ASC, id ASC
        LIMIT ?
    """
    return con.execute(q, (int(limit),)).fetchall()


# ---- New: Reactions polling ----

def fetch_new_reactions(con: sqlite3.Connection, after_sort_time: int, after_id: int) -> List[sqlite3.Row]:
    q = """
    SELECT
      rm.id                       AS reaction_msg_row_id,
      rm.ota_uuid                 AS reaction_ota_uuid,
      rm.internet_conversation_id AS conv_id,
      rm.origin                   AS reaction_origin,
      rm.sort_time                AS reaction_sort_time,
      rr.operation                AS operation,
      rr.emoji                    AS emoji,
      rr.target_message_id        AS target_msg_row_id,
      tm.ota_uuid                 AS target_ota_uuid
    FROM message rm
    JOIN reaction_record rr ON rr.reaction_message_id = rm.id
    JOIN message tm ON tm.id = rr.target_message_id
    WHERE rm.internet_conversation_id IS NOT NULL
      AND (
            rm.sort_time > ?
         OR (rm.sort_time = ? AND rm.id > ?)
      )
    ORDER BY rm.sort_time ASC, rm.id ASC
    LIMIT ?
    """
    return con.execute(q, (after_sort_time, after_sort_time, after_id, TAIL_LIMIT)).fetchall()


@dataclasses.dataclass
class PendingMedia:
    delivery_id: str
    row: sqlite3.Row
    first_seen_ts: float
    last_scan_ts: float
    attempts: int


def _is_matrix_echo_origin0(conv: str, ota_uuid: Optional[str], text: Optional[str]) -> bool:
    """
    Echo suppression policy for origin=0 messages:
      1) Strong: if mapping exists for ota_uuid with source='matrix' => echo
      2) Weak fallback: if recent_outbound matches (conv + text hash) within a short window => echo
    """
    if ota_uuid:
        m = lookup_matrix_event_for_ota(str(conv), str(ota_uuid))
        if m and str(m["source"] or "").lower() == "matrix":
            return True
    # fallback (best-effort)
    try:
        if conv and text and is_recent_outbound_echo(str(conv), str(text)):
            return True
    except Exception:
        pass
    return False


def forward_to_maubot(con: sqlite3.Connection, r: sqlite3.Row, delivery_id: str) -> bool:
    # IMPORTANT: no conversation_link gate; always forward
    if not MAUBOT_WEBHOOK_URL:
        log("WARN", "MAUBOT_WEBHOOK_URL not set; cannot forward", delivery_id=delivery_id)
        note_delivery_error(delivery_id, "MAUBOT_WEBHOOK_URL not set")
        return False

    origin = int(r["origin"] or 0)
    ota_uuid = r["ota_uuid"]
    conv = r["internet_conversation_id"]
    imid = r["internet_message_id"]

    attach_id = r["media_attachment_id"]
    media = None
    if attach_id:
        best_path, candidates = resolve_media(ROOT_DIR, str(attach_id))

        # Size guard applies only to best_path; candidates are still useful as fallbacks in Maubot.
        if best_path:
            mb = size_mb(best_path)
            if MAX_ATTACH_MB and mb > float(MAX_ATTACH_MB):
                log("WARN", "media too large; not attaching best_path", delivery_id=delivery_id, mb=f"{mb:.2f}", limit=MAX_ATTACH_MB)
                best_path = None

        mime_probe_path = best_path or (candidates[0] if candidates else None)
        media = {
            "attachment_id": str(attach_id),
            "best_path": best_path,
            "candidate_paths": candidates,
            "mime": _guess_mime(mime_probe_path),
            "mime_source": "filename",
        }

    payload = {
        "event_type": "bridge_inbound",
        "delivery_id": delivery_id,
        "ota_uuid": str(ota_uuid) if ota_uuid else None,
        "internet_message_id": str(imid) if imid else None,
        "internet_conversation_id": str(conv),
        # NOTE: matrix_room_id intentionally omitted (Maubot maps conv→room)
        "message": {
            "id": int(r["id"]),
            "origin": origin,
            "from": r["from_addr"],
            "text": r["text"],
            "sent_time": epoch_s(r["sent_time"]),
            "sort_time": epoch_s(r["sort_time"]),
            "latitude": r["latitude"],
            "longitude": r["longitude"],
            "altitude": r["altitude"],
        },
        "media": media,
        "source": "garmin_remote" if origin == 1 else "garmin_local",
    }

    try:
        code, resp = post_json(MAUBOT_WEBHOOK_URL, payload, bearer=MAUBOT_WEBHOOK_TOKEN, timeout=20, sign_hmac=True)
        if 200 <= code < 300:
            log("INFO", "forwarded inbound", delivery_id=delivery_id, conv=str(conv), code=code, origin=origin)
            return True
        log("WARN", "maubot rejected", delivery_id=delivery_id, code=code, resp=resp[:600])
        note_delivery_error(delivery_id, f"maubot {code}: {resp[:300]}")
        return False
    except Exception as e:
        log("WARN", "maubot forward failed", delivery_id=delivery_id, err=str(e))
        note_delivery_error(delivery_id, f"exception: {str(e)[:300]}")
        return False


def forward_reaction_to_maubot(conv_id: str, reaction_ota_uuid: str, target_ota_uuid: str, emoji: str, operation: int, reaction_sort_time: Optional[int]) -> bool:
    if not MAUBOT_WEBHOOK_URL:
        return False

    payload = {
        "event_type": "bridge_inbound_reaction",
        "delivery_id": str(reaction_ota_uuid),
        "internet_conversation_id": str(conv_id),
        "reaction": {
            "reaction_ota_uuid": str(reaction_ota_uuid),
            "target_ota_uuid": str(target_ota_uuid),
            "emoji": str(emoji),
            "operation": int(operation),
            "reaction_sort_time": int(reaction_sort_time or 0),
        },
    }

    try:
        code, resp = post_json(MAUBOT_WEBHOOK_URL, payload, bearer=MAUBOT_WEBHOOK_TOKEN, timeout=20, sign_hmac=True)
        if 200 <= code < 300:
            log("INFO", "forwarded reaction", conv=str(conv_id), reaction_ota=str(reaction_ota_uuid), op=int(operation), emoji=str(emoji))
            return True
        log("WARN", "maubot rejected reaction", conv=str(conv_id), code=code, resp=resp[:400])
        return False
    except Exception as e:
        log("WARN", "maubot reaction forward failed", err=str(e), conv=str(conv_id), reaction_ota=str(reaction_ota_uuid))
        return False


def _due_inbound_deliveries(limit: int = 50) -> List[sqlite3.Row]:
    now = int(time.time())
    with state_db_conn() as con:
        return con.execute(
            """
            SELECT *
            FROM inbound_delivery
            WHERE acked=0
              AND attempts < ?
              AND (next_attempt_ts IS NULL OR next_attempt_ts <= ?)
            ORDER BY COALESCE(next_attempt_ts,0) ASC, COALESCE(sort_time,0) ASC, COALESCE(message_row_id,0) ASC
            LIMIT ?
            """,
            (int(INBOUND_MAX_ATTEMPTS), now, int(limit)),
        ).fetchall()


def _due_reaction_deliveries(limit: int = 50) -> List[sqlite3.Row]:
    now = int(time.time())
    with state_db_conn() as con:
        return con.execute(
            """
            SELECT *
            FROM inbound_reaction_delivery
            WHERE acked=0
              AND attempts < ?
              AND (next_attempt_ts IS NULL OR next_attempt_ts <= ?)
            ORDER BY COALESCE(next_attempt_ts,0) ASC
            LIMIT ?
            """,
            (int(INBOUND_MAX_ATTEMPTS), now, int(limit)),
        ).fetchall()


def _process_inbound_retry_batch(con_garmin: sqlite3.Connection) -> None:
    for d in _due_inbound_deliveries(limit=30):
        did = str(d["delivery_id"])
        if is_acked(did):
            continue
        mid = d["message_row_id"]
        if mid is None:
            continue
        r = fetch_message_by_id(con_garmin, int(mid))
        if not r:
            schedule_inbound_retry(did, "message row not found in garmin db")
            continue

        origin = int(r["origin"] or 0)
        conv = str(r["internet_conversation_id"] or "")
        ota_uuid = r["ota_uuid"]
        if origin == 0 and conv:
            if _is_matrix_echo_origin0(conv, str(ota_uuid) if ota_uuid else None, r["text"]):
                log("DEBUG", "echo suppressed (retry)", conv=conv, ota_uuid=str(ota_uuid or ""))
                ack_delivery(did)
                continue

        attach_id = r["media_attachment_id"]
        if attach_id:
            best_path, _ = resolve_media(ROOT_DIR, str(attach_id))
            if not best_path:
                schedule_inbound_retry(did, "pending media (not yet on disk)")
                continue

        ok = forward_to_maubot(con_garmin, r, did)
        if ok:
            ack_delivery(did)
        else:
            schedule_inbound_retry(did, "maubot delivery failed (retry)")


def _process_reaction_retry_batch() -> None:
    for d in _due_reaction_deliveries(limit=30):
        did = str(d["delivery_id"])
        if is_reaction_acked(did):
            continue

        conv_id = str(d["conv_id"])
        reaction_ota = str(d["reaction_ota_uuid"])
        target_ota = str(d["target_ota_uuid"])
        emoji = str(d["emoji"])
        op = int(d["operation"] or 0)

        ok = forward_reaction_to_maubot(conv_id, reaction_ota, target_ota, emoji, op, None)
        if ok:
            ack_reaction_delivery(did)
        else:
            schedule_reaction_retry(did, "maubot reaction delivery failed (retry)")


def poll_loop(stop_evt: threading.Event) -> None:
    init_state()

    cur = load_cursor("poll_cursor", "0:0")
    try:
        after_sort_time, after_id = [int(x) for x in cur.split(":", 1)]
    except Exception:
        after_sort_time, after_id = 0, 0

    pending: Dict[str, PendingMedia] = {}
    do_backfill = str(INBOUND_MODE).strip().lower() == "backfill"

    log("INFO", "poller starting", poll_sec=POLL_DB_SEC, tail_limit=TAIL_LIMIT, inbound_mode=INBOUND_MODE)

    while not stop_evt.is_set():
        try:
            with garmin_db_conn() as con:
                if do_backfill:
                    rows_bf = fetch_backfill_messages(con, int(INBOUND_BACKFILL_SINCE_EPOCH), limit=500)
                    if rows_bf:
                        log("INFO", "backfill batch", n=len(rows_bf), since=INBOUND_BACKFILL_SINCE_EPOCH)
                        for r in rows_bf:
                            conv = r["internet_conversation_id"]
                            if not conv:
                                continue
                            delivery_id = compute_delivery_id(r)
                            upsert_inbound_record({
                                "delivery_id": delivery_id,
                                "internet_conversation_id": str(conv),
                                "message_row_id": int(r["id"]),
                                "origin": int(r["origin"] or 0),
                                "sent_time": epoch_s(r["sent_time"]),
                                "sort_time": epoch_s(r["sort_time"]),
                                "from": r["from_addr"],
                                "text": r["text"],
                                "media_attachment_id": r["media_attachment_id"],
                            })
                        do_backfill = False
                        log("INFO", "backfill queued; switching to tail")

                _process_inbound_retry_batch(con)
                _process_reaction_retry_batch()

                # 1) normal messages
                rows = fetch_new_messages(con, after_sort_time, after_id)
                for r in rows:
                    after_sort_time = int(r["sort_time"] or 0)
                    after_id = int(r["id"] or 0)
                    save_cursor("poll_cursor", f"{after_sort_time}:{after_id}")

                    conv = r["internet_conversation_id"]
                    if not conv:
                        continue

                    delivery_id = compute_delivery_id(r)

                    upsert_inbound_record({
                        "delivery_id": delivery_id,
                        "internet_conversation_id": str(conv),
                        "message_row_id": int(r["id"]),
                        "origin": int(r["origin"] or 0),
                        "sent_time": epoch_s(r["sent_time"]),
                        "sort_time": epoch_s(r["sort_time"]),
                        "from": r["from_addr"],
                        "text": r["text"],
                        "media_attachment_id": r["media_attachment_id"],
                    })

                    if is_acked(delivery_id):
                        continue

                    origin = int(r["origin"] or 0)
                    ota_uuid = r["ota_uuid"]

                    # NEW: origin=0 handling:
                    # - if it's a Matrix echo => suppress
                    # - otherwise forward to Maubot (so local Garmin typed messages reach Matrix)
                    if origin == 0:
                        if _is_matrix_echo_origin0(str(conv), str(ota_uuid) if ota_uuid else None, r["text"]):
                            log("DEBUG", "echo suppressed", conv=str(conv), ota_uuid=str(ota_uuid or ""))
                            ack_delivery(delivery_id)
                            continue

                    attach_id = r["media_attachment_id"]
                    if attach_id:
                        best_path, _ = resolve_media(ROOT_DIR, str(attach_id))
                        if not best_path:
                            pending[delivery_id] = PendingMedia(delivery_id, r, time.time(), 0.0, 0)
                            log("DEBUG", "queued pending media", delivery_id=delivery_id, attachment_id=str(attach_id))
                            continue

                    delivered = forward_to_maubot(con, r, delivery_id)
                    if delivered:
                        ack_delivery(delivery_id)
                    else:
                        schedule_inbound_retry(delivery_id, "maubot delivery failed (initial)")

                # 2) reactions (separate query, same cursor semantics)
                rrows = fetch_new_reactions(con, after_sort_time, after_id)
                for rr in rrows:
                    # advance cursor to reaction rm.sort_time/id
                    rst = int(rr["reaction_sort_time"] or 0)
                    rid = int(rr["reaction_msg_row_id"] or 0)
                    if (rst > after_sort_time) or (rst == after_sort_time and rid > after_id):
                        after_sort_time = rst
                        after_id = rid
                        save_cursor("poll_cursor", f"{after_sort_time}:{after_id}")

                    conv_id = rr["conv_id"]
                    reaction_ota = rr["reaction_ota_uuid"]
                    target_ota = rr["target_ota_uuid"]
                    emoji = rr["emoji"]
                    op = int(rr["operation"] or 0)

                    if not conv_id or not reaction_ota or not target_ota or not emoji:
                        continue

                    did = str(reaction_ota)
                    upsert_inbound_reaction_record({
                        "delivery_id": did,
                        "conv_id": str(conv_id),
                        "reaction_ota_uuid": str(reaction_ota),
                        "target_ota_uuid": str(target_ota),
                        "operation": int(op),
                        "emoji": str(emoji),
                    })

                    if is_reaction_acked(did):
                        continue

                    ok = forward_reaction_to_maubot(str(conv_id), str(reaction_ota), str(target_ota), str(emoji), int(op), int(rr["reaction_sort_time"] or 0))
                    if ok:
                        ack_reaction_delivery(did)
                    else:
                        schedule_reaction_retry(did, "maubot reaction delivery failed (initial)")

                # pending media resolution (unchanged)
                now = time.time()
                for did, p in list(pending.items()):
                    if is_acked(did):
                        pending.pop(did, None)
                        continue
                    if (now - p.first_seen_ts) > float(PENDING_MEDIA_MAX_SEC) or p.attempts >= int(PENDING_MEDIA_MAX_ATTEMPTS):
                        log("WARN", "pending media timeout; forwarding anyway", delivery_id=did, attempts=p.attempts)
                        delivered = forward_to_maubot(con, p.row, did)
                        if delivered:
                            ack_delivery(did)
                        else:
                            schedule_inbound_retry(did, "maubot delivery failed (pending timeout)")
                        pending.pop(did, None)
                        continue
                    if (now - p.last_scan_ts) < float(PENDING_MEDIA_RESCAN_SEC):
                        continue
                    p.attempts += 1
                    p.last_scan_ts = now
                    pending[did] = p
                    attach_id = p.row["media_attachment_id"]
                    best_path, _ = resolve_media(ROOT_DIR, str(attach_id))
                    if best_path:
                        log("INFO", "pending media resolved", delivery_id=did, path=best_path, attempts=p.attempts)
                        delivered = forward_to_maubot(con, p.row, did)
                        if delivered:
                            ack_delivery(did)
                        else:
                            schedule_inbound_retry(did, "maubot delivery failed (pending resolved)")
                        pending.pop(did, None)

        except Exception as e:
            log("WARN", "poll loop error", err=str(e))

        time.sleep(max(0.05, float(POLL_DB_SEC)))

    log("INFO", "poller stopped")


# ---- Outbound worker ----

def _emit_outbound_result(event: Dict[str, Any]) -> None:
    if not MAUBOT_WEBHOOK_URL or not POST_OUTBOUND_RESULTS:
        return
    payload = {"event_type": "bridge_outbound_result", **event}
    try:
        code, _ = post_json(MAUBOT_WEBHOOK_URL, payload, bearer=MAUBOT_WEBHOOK_TOKEN, timeout=20, sign_hmac=True)
        log("DEBUG", "outbound result posted", code=code)
    except Exception as e:
        log("WARN", "outbound result post failed", err=str(e))


def outbound_worker(stop_evt: threading.Event) -> None:
    init_state()
    log("INFO", "outbound worker starting")
    while not stop_evt.is_set():
        try:
            job = next_outbound_job()
            if not job:
                time.sleep(0.25)
                continue

            job_id = int(job["job_id"])
            room = str(job["matrix_room_id"])
            ev = str(job["matrix_event_id"])
            kind = str(job["kind"]).strip()
            conv = job["internet_conversation_id"]

            mark_outbound_sending(job_id)

            ensure_adb_connected()

            # Guard: ensure ADBKeyBoard IME is enabled/set before sending
            try:
                adb_ime_enable_set()
            except Exception as e:
                log("WARN", "ime enable/set failed (guard)", err=str(e))

            with garmin_db_conn() as con_db:
                if kind == "text":
                    if not conv:
                        mark_outbound_failed(job_id, "internet_conversation_id required for kind=text")
                        continue
                    thread_id = resolve_thread_id_for_conversation(con_db, str(conv))
                    if not thread_id:
                        mark_outbound_failed(job_id, "Could not resolve message_thread_id for conversation (no messages yet).")
                        continue
                    before_st, before_id = snapshot_conv_cursor(con_db, str(conv))
                    text = str(job["text"] or "")
                    try:
                        send_to_existing_thread(int(thread_id), text)
                    except Exception:
                        ensure_adb_connected()
                        raise

                    ota_uuid, conv_from_row, msg_row_id = find_new_outgoing_by_text(con_db, str(conv), before_st, before_id, text)
                    result = {
                        "job_id": job_id,
                        "kind": "text",
                        "matrix_room_id": room,
                        "matrix_event_id": ev,
                        "internet_conversation_id": str(conv_from_row or conv),
                        "message_row_id": msg_row_id,
                        "ota_uuid": ota_uuid,
                        "thread_id_used": int(thread_id),
                    }
                    if ota_uuid:
                        add_outbound_correlation(str(ota_uuid), room, ev)
                        # NEW: record mapping for echo suppression as source='matrix'
                        try:
                            upsert_matrix_event_map(
                                conv_id=str(conv_from_row or conv),
                                garmin_ota_uuid=str(ota_uuid),
                                matrix_room_id=room,
                                matrix_event_id=ev,
                                kind="message",
                                source="matrix",
                            )
                        except Exception:
                            pass
                        log("INFO", "outbound correlated", job_id=job_id, ota_uuid=str(ota_uuid))
                    else:
                        log("WARN", "outbound correlation failed", job_id=job_id)

                    mark_outbound_sent(job_id, result=result)
                    _emit_outbound_result(result)
                    continue

                if kind == "new_thread":
                    try:
                        recipients = json.loads(job["recipients_json"] or "[]")
                    except Exception:
                        recipients = []
                    if not isinstance(recipients, list) or not recipients:
                        mark_outbound_failed(job_id, "recipients_json must be a non-empty JSON list")
                        continue
                    text = str(job["text"] or "")
                    before_st0, before_id0 = snapshot_global_cursor(con_db)
                    try:
                        compose_new_thread_and_send([str(x) for x in recipients], text)
                    except Exception:
                        ensure_adb_connected()
                        raise

                    ota_uuid, conv_from_row, msg_row_id = find_new_outgoing_by_text(con_db, None, before_st0, before_id0, text)

                    result = {
                        "job_id": job_id,
                        "kind": "new_thread",
                        "matrix_room_id": room,
                        "matrix_event_id": ev,
                        "internet_conversation_id": conv_from_row,
                        "message_row_id": msg_row_id,
                        "ota_uuid": ota_uuid,
                        "recipients": [str(x) for x in recipients],
                    }
                    if conv_from_row:
                        upsert_link(str(conv_from_row), room)
                    if ota_uuid and conv_from_row:
                        add_outbound_correlation(str(ota_uuid), room, ev)
                        # NEW: record mapping for echo suppression as source='matrix'
                        try:
                            upsert_matrix_event_map(
                                conv_id=str(conv_from_row),
                                garmin_ota_uuid=str(ota_uuid),
                                matrix_room_id=room,
                                matrix_event_id=ev,
                                kind="message",
                                source="matrix",
                            )
                        except Exception:
                            pass
                        log("INFO", "new_thread correlated", job_id=job_id, ota_uuid=str(ota_uuid), conv=str(conv_from_row))
                    else:
                        log("WARN", "new_thread correlation failed", job_id=job_id)

                    mark_outbound_sent(job_id, result=result)
                    _emit_outbound_result(result)
                    continue

                mark_outbound_failed(job_id, f"Unsupported kind: {kind}")

        except Exception as e:
            log("WARN", "outbound worker error", err=str(e))
            time.sleep(0.8)

    log("INFO", "outbound worker stopped")


# ---- HTTP API ----

def _auth_ok(headers: Dict[str, str]) -> bool:
    if not HTTP_TOKEN:
        return True
    auth = headers.get("authorization") or headers.get("Authorization") or ""
    return auth.startswith("Bearer ") and auth.split(" ", 1)[1].strip() == HTTP_TOKEN


def _read_body(handler: BaseHTTPRequestHandler) -> bytes:
    n = int(handler.headers.get("content-length") or "0")
    return handler.rfile.read(n) if n > 0 else b""


def _parse_json(raw: bytes) -> Dict[str, Any]:
    if not raw:
        return {}
    try:
        return json.loads(raw.decode("utf-8", errors="replace") or "{}")
    except Exception:
        return {}


def _send(handler: BaseHTTPRequestHandler, code: int, payload: Dict[str, Any]) -> None:
    b = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    handler.send_response(code)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(b)))
    handler.end_headers()
    handler.wfile.write(b)


def _parse_query(path: str) -> Tuple[str, Dict[str, List[str]]]:
    u = urlparse(path)
    return u.path, parse_qs(u.query or "")


def _ack_any_delivery(delivery_id: str) -> bool:
    """
    For convenience and backwards compatibility:
    - /inbound/ack will ack message deliveries if present
    - else ack reaction deliveries if present
    """
    did = (delivery_id or "").strip()
    if not did:
        return False
    with state_db_conn() as con:
        r1 = con.execute("SELECT 1 FROM inbound_delivery WHERE delivery_id=? LIMIT 1", (did,)).fetchone()
        if r1:
            now = int(time.time())
            con.execute("UPDATE inbound_delivery SET acked=1, delivered_ts=?, last_error=NULL WHERE delivery_id=?", (now, did))
            con.commit()
            return True
        r2 = con.execute("SELECT 1 FROM inbound_reaction_delivery WHERE delivery_id=? LIMIT 1", (did,)).fetchone()
        if r2:
            now = int(time.time())
            con.execute("UPDATE inbound_reaction_delivery SET acked=1, delivered_ts=?, last_error=NULL WHERE delivery_id=?", (now, did))
            con.commit()
            return True
    return False


class BridgeHandler(BaseHTTPRequestHandler):
    server_version = "garmin-bridge/2.5"

    def do_GET(self):  # noqa: N802
        path, qs = _parse_query(self.path)

        if path == "/healthz":
            return _send(self, 200, {"ok": True})

        # Lookup mapping for a Garmin message ota_uuid -> Matrix event
        if path == "/matrix/lookup":
            if not _auth_ok(dict(self.headers)):
                return _send(self, 401, {"ok": False, "error": "unauthorized"})
            conv = (qs.get("conv") or qs.get("internet_conversation_id") or [""])[0].strip()
            ota = (qs.get("ota") or qs.get("garmin_ota_uuid") or [""])[0].strip()
            if not conv or not ota:
                return _send(self, 400, {"ok": False, "error": "conv and ota required"})
            r = lookup_matrix_event_for_ota(conv, ota)
            if not r:
                return _send(self, 404, {"ok": False, "error": "not found"})
            return _send(self, 200, {"ok": True, "matrix_room_id": r["matrix_room_id"], "matrix_event_id": r["matrix_event_id"], "kind": r["kind"], "source": r["source"]})

        # Lookup latest reaction event for (target_ota_uuid, emoji) in a conversation
        if path == "/matrix/lookup_reaction":
            if not _auth_ok(dict(self.headers)):
                return _send(self, 401, {"ok": False, "error": "unauthorized"})
            conv = (qs.get("conv") or qs.get("internet_conversation_id") or [""])[0].strip()
            target = (qs.get("target_ota") or qs.get("target_ota_uuid") or [""])[0].strip()
            emoji = (qs.get("emoji") or [""])[0]
            if not conv or not target or not emoji:
                return _send(self, 400, {"ok": False, "error": "conv, target_ota and emoji required"})
            r = lookup_reaction_event_by_target(conv, target, emoji)
            if not r:
                return _send(self, 404, {"ok": False, "error": "not found"})
            return _send(self, 200, {"ok": True, "matrix_room_id": r["matrix_room_id"], "reaction_event_id": r["matrix_event_id"], "reaction_ota_uuid": r["garmin_ota_uuid"]})

        return _send(self, 404, {"ok": False, "error": "not found"})

    def do_POST(self):  # noqa: N802
        headers = dict(self.headers)

        if not _auth_ok(headers):
            return _send(self, 401, {"ok": False, "error": "unauthorized"})

        raw = _read_body(self)

        if not _hmac_ok(headers, raw):
            return _send(self, 401, {"ok": False, "error": "bad hmac"})

        body = _parse_json(raw)

        if self.path == "/link":
            conv = (body.get("internet_conversation_id") or "").strip()
            room = (body.get("matrix_room_id") or "").strip()
            if not conv or not room:
                return _send(self, 400, {"ok": False, "error": "internet_conversation_id and matrix_room_id required"})
            upsert_link(conv, room)
            log("INFO", "linked conversation", conv=conv, room=room)
            return _send(self, 200, {"ok": True})

        # NEW: Maubot -> bridge mapping callback
        if self.path == "/matrix/map_event":
            conv = (body.get("internet_conversation_id") or body.get("conv_id") or "").strip()
            ota = (body.get("garmin_ota_uuid") or body.get("ota_uuid") or "").strip()
            room = (body.get("matrix_room_id") or "").strip()
            ev = (body.get("matrix_event_id") or "").strip()
            kind = (body.get("kind") or "message").strip()
            source = (body.get("source") or body.get("from_matrix") or "").strip()

            # normalize source
            if isinstance(source, str):
                s = source.lower()
                if s in ("1", "true", "yes", "on", "matrix"):
                    source = "matrix"
                elif s in ("0", "false", "no", "off", "garmin"):
                    source = "garmin"
                elif not s:
                    source = "garmin"
            else:
                source = "garmin"

            emoji = body.get("emoji")
            target_ota = body.get("target_ota_uuid")

            if not conv or not ota or not room or not ev:
                return _send(self, 400, {"ok": False, "error": "internet_conversation_id, garmin_ota_uuid, matrix_room_id, matrix_event_id required"})

            try:
                upsert_matrix_event_map(conv, ota, room, ev, kind=kind, source=str(source), emoji=(str(emoji) if emoji is not None else None), target_ota_uuid=(str(target_ota) if target_ota is not None else None))
            except Exception as e:
                return _send(self, 500, {"ok": False, "error": f"db error: {str(e)[:200]}"})

            return _send(self, 200, {"ok": True})

        if self.path == "/outbound/enqueue":
            room = (body.get("matrix_room_id") or "").strip()
            ev = (body.get("matrix_event_id") or "").strip()
            kind = (body.get("kind") or "text").strip()
            conv = (body.get("internet_conversation_id") or "").strip() or None
            if not room or not ev:
                return _send(self, 400, {"ok": False, "error": "matrix_room_id and matrix_event_id required"})

            job: Dict[str, Any] = {"matrix_room_id": room, "matrix_event_id": ev, "internet_conversation_id": conv, "kind": kind}

            if kind == "text":
                if not conv:
                    return _send(self, 400, {"ok": False, "error": "internet_conversation_id required for kind=text"})
                job["text"] = body.get("text") or ""
            elif kind == "new_thread":
                recips = body.get("recipients") or body.get("recipients_list")
                if not isinstance(recips, list) or not recips:
                    return _send(self, 400, {"ok": False, "error": "recipients must be a non-empty list"})
                job["recipients_json"] = json.dumps([str(x) for x in recips], ensure_ascii=False)
                job["text"] = body.get("text") or ""
            else:
                return _send(self, 400, {"ok": False, "error": f"unsupported kind: {kind}"})

            jid = enqueue_outbound(job)

            extra: Dict[str, Any] = {}
            if kind == "new_thread" and int(OUTBOUND_ENQUEUE_WAIT_NEWTHREAD_SEC) > 0:
                deadline = time.time() + int(OUTBOUND_ENQUEUE_WAIT_NEWTHREAD_SEC)
                while time.time() < deadline:
                    res = get_job_result(jid)
                    if res and res.get("internet_conversation_id"):
                        extra["internet_conversation_id"] = res.get("internet_conversation_id")
                        break
                    time.sleep(0.25)

            return _send(self, 200, {"ok": True, "job_id": jid, **extra})

        if self.path == "/inbound/ack":
            delivery_id = (body.get("delivery_id") or body.get("ota_uuid") or "").strip()
            if not delivery_id:
                return _send(self, 400, {"ok": False, "error": "delivery_id or ota_uuid required"})
            ok = _ack_any_delivery(delivery_id)
            return _send(self, 200 if ok else 404, {"ok": bool(ok)})

        return _send(self, 404, {"ok": False, "error": "not found"})

    def log_message(self, fmt: str, *args: Any) -> None:
        try:
            line = (fmt % args) if args else fmt
        except Exception:
            line = fmt
        log("DEBUG", "http", path=self.path, line=line)


def http_server(stop_evt: threading.Event) -> None:
    init_state()
    srv = ThreadingHTTPServer((HTTP_BIND, int(HTTP_PORT)), BridgeHandler)
    srv.timeout = 1.0
    log("INFO", "http server listening", bind=HTTP_BIND, port=HTTP_PORT)
    while not stop_evt.is_set():
        srv.handle_request()
    try:
        srv.server_close()
    except Exception:
        pass


def validate_env() -> None:
    if not DB_PATH or not os.path.isfile(DB_PATH):
        raise SystemExit(f"DB_PATH not found or not a file: {DB_PATH!r}")
    if not ROOT_DIR or not os.path.isdir(ROOT_DIR):
        raise SystemExit(f"ROOT_DIR not found or not a directory: {ROOT_DIR!r}")
    if MAUBOT_WEBHOOK_URL:
        u = urlparse(MAUBOT_WEBHOOK_URL)
        if not u.scheme or not u.netloc:
            raise SystemExit(f"MAUBOT_WEBHOOK_URL invalid: {MAUBOT_WEBHOOK_URL!r}")
    try:
        p = subprocess.run(_adb_base_cmd() + ["version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)
        if p.returncode != 0:
            raise SystemExit(f"ADB not working: {p.stderr.decode(errors='replace')[:200]}")
    except FileNotFoundError:
        raise SystemExit(f"ADB_BIN not found: {ADB_BIN!r}")


def main() -> None:
    validate_env()
    init_state()

    ensure_adb_connected()

    # Start Garmin main activity only (per requirement)
    try:
        start_garmin_main_activity()
        log("INFO", "garmin main activity started", pkg=GARMIN_PKG, act=GARMIN_ACT)
    except Exception as e:
        log("WARN", "failed to start garmin main activity", err=str(e))

    # Best-effort: ensure IME is set on startup; outbound path still sets + kicks before every send.
    try:
        adb_ime_enable_set()
        log("INFO", "adbkeyboard ime enabled+set (startup)", ime=ADBKEYBOARD_IME)
    except Exception as e:
        log("WARN", "adbkeyboard ime enable/set failed (startup)", err=str(e))

    stop_evt = threading.Event()

    def _sig(*_a: Any) -> None:
        stop_evt.set()

    signal.signal(signal.SIGINT, _sig)
    signal.signal(signal.SIGTERM, _sig)

    threads = [
        threading.Thread(target=http_server, args=(stop_evt,), daemon=True, name="http"),
        threading.Thread(target=poll_loop, args=(stop_evt,), daemon=True, name="poller"),
        threading.Thread(target=outbound_worker, args=(stop_evt,), daemon=True, name="outbound"),
    ]
    for t in threads:
        t.start()

    while not stop_evt.is_set():
        time.sleep(0.5)

    log("INFO", "bridge exiting")


if __name__ == "__main__":
    main()
