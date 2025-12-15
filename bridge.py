#!/usr/bin/env python3
"""
Garmin Messenger ↔ Matrix (Maubot) bridge.

Key invariants (based on Garmin Messenger message.db schema):
- Canonical conversation identifier is message.internet_conversation_id (UUID-like text).
- Local thread id message.message_thread_id is an ephemeral integer row id (may change across logins).
- Canonical per-message identifier is message.ota_uuid when present (unique index exists for non-NULL values).
- Inbound ingestion must handle BOTH directions:
    origin=1 => remote inbound to this account
    origin=0 => sent from this account (either echo of Matrix->Garmin, or manual send in Garmin app)
- Echo suppression: Matrix->Garmin sends are correlated to ota_uuid once they appear in DB; later poller
  treats those outgoing messages as "echo" and does not forward them back to Matrix.

This bridge:
- Polls Garmin's SQLite DB (read-only) for new messages (text + attachments).
- Resolves media files robustly:
    1) Prefer media_attachment_file table rows (if present) to get concrete file paths.
    2) Fallback to deterministic disk scan by attachment_id across common subfolders and extensions.
- Forwards inbound events to Maubot via HTTP webhook.
- Exposes a small HTTP API for Maubot to:
    - link a Garmin conversation UUID to a Matrix room
    - enqueue outbound sends (text/media)
    - acknowledge deliveries (idempotency)

Outbound sending (Garmin UI automation):
- This file intentionally does NOT hardcode device-specific ADB/UIAutomator sequences.
- Instead, it invokes an external command (GARMIN_SEND_CMD) with a JSON payload; you provide that
  sender implementation (e.g., your robust adb/uiautomator scripts).
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import os
import signal
import sqlite3
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import urllib.request


# --------------------------------------------------------------------------------------
# Env helpers
# --------------------------------------------------------------------------------------

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


# Required inputs
DB_PATH = env("DB_PATH")  # Garmin Messenger message.db (bind-mounted from ReDroid or device)
ROOT_DIR = env("ROOT_DIR")  # Root dir for media files (e.g. .../files/media or exported mirror)

# Bridge state (durable, bridge-owned)
STATE_DIR = env("STATE_DIR", "/var/lib/garmin-bridge")
STATE_DB = env("STATE_DB", os.path.join(STATE_DIR, "state.db"))

# Polling
POLL_DB_SEC = env("POLL_DB_SEC", 1, int)
TAIL_LIMIT = env("TAIL_LIMIT", 250, int)
DEBUG = env("DEBUG", True, bool)
LOG_LEVEL = env("LOG_LEVEL", "INFO")
LOG_JSON = env("LOG_JSON", False, bool)

# Webhook to Maubot
MAUBOT_WEBHOOK_URL = env("MAUBOT_WEBHOOK_URL")
MAUBOT_WEBHOOK_TOKEN = env("MAUBOT_WEBHOOK_TOKEN", "")

# HTTP API
HTTP_BIND = env("HTTP_BIND", "127.0.0.1")
HTTP_PORT = env("HTTP_PORT", 8808, int)
HTTP_TOKEN = env("HTTP_TOKEN", "")

# Outbound sender integration
GARMIN_SEND_CMD = env("GARMIN_SEND_CMD", "")
GARMIN_SEND_TIMEOUT_SEC = env("GARMIN_SEND_TIMEOUT_SEC", 45, int)
OUTBOUND_POLL_AFTER_SEND_SEC = env("OUTBOUND_POLL_AFTER_SEND_SEC", 6, int)

# Media constraints
MAX_ATTACH_MB = env("MAX_ATTACH_MB", 25, int)
MEDIA_EXTS = tuple(x.strip() for x in env("MEDIA_EXTS", "avif,jpg,jpeg,png,webp,gif,mp4,m4a,ogg,oga,wav", str).split(",") if x.strip())
SEARCH_ROOTS = tuple(x.strip() for x in env("SEARCH_ROOTS", "high,preview,low,audio,", str).split(","))

SQLITE_TIMEOUT = env("SQLITE_TIMEOUT", 2.5, float)


# --------------------------------------------------------------------------------------
# Logging
# --------------------------------------------------------------------------------------

_LEVELS = {"DEBUG": 10, "INFO": 20, "WARN": 30, "WARNING": 30, "ERROR": 40}
_CUR_LEVEL = _LEVELS.get(str(LOG_LEVEL).upper(), 20)

def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime())

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

def _safe_kv(v: Any) -> str:
    try:
        s = str(v)
        return s if len(s) <= 260 else s[:260] + "…"
    except Exception:
        return "<unprintable>"


# --------------------------------------------------------------------------------------
# SQLite connections
# --------------------------------------------------------------------------------------

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


# --------------------------------------------------------------------------------------
# State schema
# --------------------------------------------------------------------------------------

STATE_SCHEMA = r"""
CREATE TABLE IF NOT EXISTS conversation_link (
    internet_conversation_id TEXT NOT NULL PRIMARY KEY,
    matrix_room_id TEXT NOT NULL,
    account_id TEXT,
    participants_json TEXT,
    last_seen_thread_row_id INTEGER,
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
    media_type INTEGER,
    attachment_state INTEGER,
    acked INTEGER NOT NULL DEFAULT 0,
    delivered_ts INTEGER,
    last_error TEXT
);

CREATE TABLE IF NOT EXISTS outbound_job (
    job_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    matrix_room_id TEXT NOT NULL,
    matrix_event_id TEXT NOT NULL,
    internet_conversation_id TEXT,
    kind TEXT NOT NULL,
    text TEXT,
    media_path TEXT,
    media_mime TEXT,
    created_ts INTEGER NOT NULL,
    updated_ts INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued',
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS outbound_unique_event ON outbound_job(matrix_room_id, matrix_event_id);

CREATE TABLE IF NOT EXISTS outbound_correlation (
    ota_uuid TEXT NOT NULL PRIMARY KEY,
    matrix_room_id TEXT NOT NULL,
    matrix_event_id TEXT NOT NULL,
    created_ts INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS cursor (
    name TEXT NOT NULL PRIMARY KEY,
    value TEXT
);
"""


def init_state() -> None:
    with state_db_conn() as con:
        con.executescript(STATE_SCHEMA)
        con.commit()


# --------------------------------------------------------------------------------------
# Utilities
# --------------------------------------------------------------------------------------

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


# --------------------------------------------------------------------------------------
# Media discovery (robust)
# --------------------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class MediaCandidate:
    path: str
    quality: Optional[int] = None
    owner: Optional[int] = None
    source: str = "unknown"  # table|scan

def _table_columns(con: sqlite3.Connection, table: str) -> List[str]:
    try:
        rows = con.execute(f"PRAGMA table_info({table});").fetchall()
        return [str(r["name"]) for r in rows]
    except Exception:
        return []

def _normalize_media_path(root_dir: str, p: str) -> str:
    p = (p or "").strip()
    if not p:
        return p
    if os.path.isabs(p) and os.path.isfile(p):
        return p
    cand = os.path.join(root_dir, p)
    if os.path.isfile(cand):
        return cand
    cand = os.path.join(root_dir, os.path.basename(p))
    if os.path.isfile(cand):
        return cand
    return p

def media_candidates_from_table(con: sqlite3.Connection, root_dir: str, attachment_id: str) -> List[MediaCandidate]:
    cols = _table_columns(con, "media_attachment_file")
    if not cols:
        return []
    possible_path_cols = [c for c in ("file_path", "path", "relative_path", "file_name") if c in cols]
    if not possible_path_cols:
        return []
    sel = ", ".join(["attachment_id"] + [c for c in ("owner", "quality") if c in cols] + possible_path_cols)
    q = f"SELECT {sel} FROM media_attachment_file WHERE attachment_id = ?"
    rows = con.execute(q, (attachment_id,)).fetchall()
    out: List[MediaCandidate] = []
    for r in rows:
        owner = r["owner"] if "owner" in r.keys() else None
        quality = r["quality"] if "quality" in r.keys() else None
        for pc in possible_path_cols:
            p = r[pc]
            if not p:
                continue
            np = _normalize_media_path(root_dir, str(p))
            if os.path.isfile(np):
                out.append(MediaCandidate(path=np, quality=quality, owner=owner, source="table"))
    return out

def media_candidates_by_scan(root_dir: str, attachment_id: str) -> List[MediaCandidate]:
    out: List[MediaCandidate] = []
    for sub in SEARCH_ROOTS:
        d = os.path.join(root_dir, sub) if sub else root_dir
        for ext in MEDIA_EXTS:
            p = os.path.join(d, f"{attachment_id}.{ext}")
            if os.path.isfile(p):
                out.append(MediaCandidate(path=p, source="scan"))
    return out

def pick_best_media(cands: List[MediaCandidate]) -> Optional[MediaCandidate]:
    if not cands:
        return None
    def key(c: MediaCandidate):
        src_pri = 0 if c.source == "table" else 1
        q = c.quality if c.quality is not None else -1
        return (src_pri, -q, -size_mb(c.path))
    return sorted(cands, key=key)[0]

def resolve_media(con: sqlite3.Connection, root_dir: str, attachment_id: str) -> Tuple[Optional[str], List[str]]:
    c1 = media_candidates_from_table(con, root_dir, attachment_id)
    c2 = media_candidates_by_scan(root_dir, attachment_id)
    seen = {c.path for c in c1}
    all_cands = c1 + [c for c in c2 if c.path not in seen]
    best = pick_best_media(all_cands)
    return (best.path if best else None, [c.path for c in all_cands])


# --------------------------------------------------------------------------------------
# Webhook to Maubot
# --------------------------------------------------------------------------------------

def post_json(url: str, payload: Dict[str, Any], bearer: str = "", timeout: int = 15) -> Tuple[int, str]:
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json; charset=utf-8")
    if bearer:
        req.add_header("Authorization", f"Bearer {bearer}")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        b = resp.read(2048)
        return int(resp.status), b.decode("utf-8", errors="replace")


# --------------------------------------------------------------------------------------
# Cursor helpers
# --------------------------------------------------------------------------------------

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


# --------------------------------------------------------------------------------------
# Links and idempotency
# --------------------------------------------------------------------------------------

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
        return con.execute(
            "SELECT * FROM conversation_link WHERE internet_conversation_id=? LIMIT 1",
            (internet_conversation_id,),
        ).fetchone()

def get_link_by_room(matrix_room_id: str) -> Optional[sqlite3.Row]:
    with state_db_conn() as con:
        return con.execute(
            "SELECT * FROM conversation_link WHERE matrix_room_id=? ORDER BY updated_ts DESC LIMIT 1",
            (matrix_room_id,),
        ).fetchone()

def is_acked(delivery_id: str) -> bool:
    with state_db_conn() as con:
        r = con.execute("SELECT acked FROM inbound_delivery WHERE delivery_id=? LIMIT 1", (delivery_id,)).fetchone()
        return bool(r and int(r["acked"] or 0) == 1)

def ack_delivery(delivery_id: str) -> None:
    with state_db_conn() as con:
        con.execute("UPDATE inbound_delivery SET acked=1 WHERE delivery_id=?", (delivery_id,))
        con.commit()

def upsert_inbound_record(delivery: Dict[str, Any]) -> None:
    with state_db_conn() as con:
        con.execute(
            """
            INSERT INTO inbound_delivery(
              delivery_id, internet_conversation_id, message_row_id, origin, sent_time, sort_time, from_addr, text,
              media_attachment_id, media_type, attachment_state
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(delivery_id) DO UPDATE SET
              internet_conversation_id=excluded.internet_conversation_id,
              message_row_id=COALESCE(excluded.message_row_id, inbound_delivery.message_row_id),
              origin=excluded.origin,
              sent_time=excluded.sent_time,
              sort_time=excluded.sort_time,
              from_addr=excluded.from_addr,
              text=excluded.text,
              media_attachment_id=excluded.media_attachment_id,
              media_type=excluded.media_type,
              attachment_state=excluded.attachment_state
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
                delivery.get("media_type"),
                delivery.get("attachment_state"),
            ),
        )
        con.commit()

def add_outbound_correlation(ota_uuid: str, matrix_room_id: str, matrix_event_id: str) -> None:
    now = int(time.time())
    with state_db_conn() as con:
        con.execute(
            "INSERT INTO outbound_correlation(ota_uuid, matrix_room_id, matrix_event_id, created_ts) VALUES(?,?,?,?) ON CONFLICT(ota_uuid) DO NOTHING",
            (ota_uuid, matrix_room_id, matrix_event_id, now),
        )
        con.commit()

def lookup_outbound_by_ota(ota_uuid: str) -> Optional[sqlite3.Row]:
    with state_db_conn() as con:
        return con.execute("SELECT * FROM outbound_correlation WHERE ota_uuid=? LIMIT 1", (ota_uuid,)).fetchone()


# --------------------------------------------------------------------------------------
# Outbound queue
# --------------------------------------------------------------------------------------

def enqueue_outbound(job: Dict[str, Any]) -> int:
    now = int(time.time())
    with state_db_conn() as con:
        con.execute(
            """
            INSERT INTO outbound_job(
              matrix_room_id, matrix_event_id, internet_conversation_id, kind, text, media_path, media_mime,
              created_ts, updated_ts, status, attempts
            ) VALUES(?,?,?,?,?,?,?,?,?, 'queued', 0)
            ON CONFLICT(matrix_room_id, matrix_event_id) DO NOTHING
            """,
            (
                job["matrix_room_id"],
                job["matrix_event_id"],
                job.get("internet_conversation_id"),
                job["kind"],
                job.get("text"),
                job.get("media_path"),
                job.get("media_mime"),
                now,
                now,
            ),
        )
        con.commit()
        r = con.execute(
            "SELECT job_id FROM outbound_job WHERE matrix_room_id=? AND matrix_event_id=? LIMIT 1",
            (job["matrix_room_id"], job["matrix_event_id"]),
        ).fetchone()
        return int(r["job_id"]) if r else 0

def next_outbound_job() -> Optional[sqlite3.Row]:
    with state_db_conn() as con:
        return con.execute(
            "SELECT * FROM outbound_job WHERE status IN ('queued','failed') ORDER BY updated_ts ASC, job_id ASC LIMIT 1"
        ).fetchone()

def update_outbound_job(job_id: int, status: str, error: Optional[str] = None) -> None:
    now = int(time.time())
    with state_db_conn() as con:
        con.execute(
            "UPDATE outbound_job SET status=?, updated_ts=?, attempts=attempts+1, last_error=? WHERE job_id=?",
            (status, now, error, job_id),
        )
        con.commit()

def mark_outbound_sent(job_id: int) -> None:
    now = int(time.time())
    with state_db_conn() as con:
        con.execute("UPDATE outbound_job SET status='sent', updated_ts=? WHERE job_id=?", (now, job_id))
        con.commit()

def run_sender(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not GARMIN_SEND_CMD:
        raise RuntimeError("GARMIN_SEND_CMD is not set; outbound sending is not configured.")
    p = subprocess.run(
        [GARMIN_SEND_CMD],
        input=json.dumps(payload).encode("utf-8"),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=GARMIN_SEND_TIMEOUT_SEC,
    )
    out = p.stdout.decode("utf-8", errors="replace").strip()
    err = p.stderr.decode("utf-8", errors="replace").strip()
    if p.returncode != 0:
        raise RuntimeError(f"sender failed rc={p.returncode} stderr={err[:400]}")
    if not out:
        return {"ok": True}
    try:
        return json.loads(out)
    except Exception:
        return {"ok": True, "raw": out[:800]}

def poll_for_new_outgoing_ota(con: sqlite3.Connection, internet_conversation_id: str, since_ts: int, window_sec: int = 30) -> Optional[str]:
    lower = max(0, since_ts - window_sec)
    deadline = time.time() + OUTBOUND_POLL_AFTER_SEND_SEC
    while time.time() < deadline:
        rows = con.execute(
            """
            SELECT ota_uuid, id, sent_time, sort_time
            FROM message
            WHERE internet_conversation_id = ?
              AND origin = 0
              AND (sent_time >= ? OR sort_time >= ?)
            ORDER BY sort_time DESC, id DESC
            LIMIT 10
            """,
            (internet_conversation_id, lower, lower),
        ).fetchall()
        for r in rows:
            if r["ota_uuid"]:
                return str(r["ota_uuid"])
        time.sleep(0.5)
    return None


# --------------------------------------------------------------------------------------
# Poller
# --------------------------------------------------------------------------------------

def compute_delivery_id(row: sqlite3.Row) -> str:
    if row["ota_uuid"]:
        return str(row["ota_uuid"])
    conv = str(row["internet_conversation_id"] or "")
    mid = str(row["id"])
    sent = str(row["sent_time"] or row["sort_time"] or "")
    txt = str(row["text"] or "")
    att = str(row["media_attachment_id"] or "")
    return "fb_" + sha1_hex("|".join([conv, mid, sent, txt, att]))[:24]

def fetch_new_messages(con: sqlite3.Connection, after_sort_time: int, after_id: int) -> List[sqlite3.Row]:
    q = """
        SELECT
          id, message_thread_id, internet_conversation_id,
          origin, status, web_transfer_state, "from" as from_addr,
          text, sort_time, sent_time, ota_uuid,
          media_attachment_id, media_type, attachment_state, duration,
          latitude, longitude, altitude
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

def poll_loop(stop_evt: threading.Event) -> None:
    init_state()
    cur = load_cursor("poll_cursor", "0:0")
    try:
        after_sort_time, after_id = [int(x) for x in cur.split(":", 1)]
    except Exception:
        after_sort_time, after_id = 0, 0

    log("INFO", "poller starting", poll_sec=POLL_DB_SEC, tail_limit=TAIL_LIMIT)

    while not stop_evt.is_set():
        try:
            with garmin_db_conn() as con:
                rows = fetch_new_messages(con, after_sort_time, after_id)

                for r in rows:
                    after_sort_time = int(r["sort_time"] or 0)
                    after_id = int(r["id"] or 0)
                    save_cursor("poll_cursor", f"{after_sort_time}:{after_id}")

                    conv = r["internet_conversation_id"]
                    if not conv:
                        continue

                    delivery_id = compute_delivery_id(r)
                    if is_acked(delivery_id):
                        continue

                    origin = int(r["origin"] or 0)
                    ota_uuid = r["ota_uuid"]
                    if origin == 0 and ota_uuid:
                        corr = lookup_outbound_by_ota(str(ota_uuid))
                        if corr:
                            log("DEBUG", "echo suppressed", ota_uuid=ota_uuid, matrix_event_id=corr["matrix_event_id"])
                            upsert_inbound_record({
                                "delivery_id": delivery_id,
                                "internet_conversation_id": conv,
                                "message_row_id": int(r["id"]),
                                "origin": origin,
                                "sent_time": epoch_s(r["sent_time"]),
                                "sort_time": epoch_s(r["sort_time"]),
                                "from": r["from_addr"],
                                "text": r["text"],
                                "media_attachment_id": r["media_attachment_id"],
                                "media_type": r["media_type"],
                                "attachment_state": r["attachment_state"],
                            })
                            ack_delivery(delivery_id)
                            continue

                    link = get_link_by_conversation(conv)
                    upsert_inbound_record({
                        "delivery_id": delivery_id,
                        "internet_conversation_id": conv,
                        "message_row_id": int(r["id"]),
                        "origin": origin,
                        "sent_time": epoch_s(r["sent_time"]),
                        "sort_time": epoch_s(r["sort_time"]),
                        "from": r["from_addr"],
                        "text": r["text"],
                        "media_attachment_id": r["media_attachment_id"],
                        "media_type": r["media_type"],
                        "attachment_state": r["attachment_state"],
                    })

                    if not link:
                        log("DEBUG", "unlinked conversation - skip forward", conv=conv, id=r["id"])
                        continue
                    if not MAUBOT_WEBHOOK_URL:
                        log("WARN", "MAUBOT_WEBHOOK_URL not set; cannot forward", delivery_id=delivery_id)
                        continue

                    room = link["matrix_room_id"]
                    attach_id = r["media_attachment_id"]
                    media = None
                    if attach_id:
                        best_path, candidates = resolve_media(con, ROOT_DIR, str(attach_id))
                        if best_path:
                            mb = size_mb(best_path)
                            if MAX_ATTACH_MB and mb > float(MAX_ATTACH_MB):
                                best_path = None
                        media = {
                            "attachment_id": str(attach_id),
                            "best_path": best_path,
                            "candidate_paths": candidates,
                            "media_type": r["media_type"],
                            "attachment_state": r["attachment_state"],
                            "duration": r["duration"],
                        }

                    payload = {
                        "delivery_id": delivery_id,
                        "ota_uuid": str(ota_uuid) if ota_uuid else None,
                        "internet_conversation_id": conv,
                        "matrix_room_id": room,
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
                        code, resp = post_json(MAUBOT_WEBHOOK_URL, payload, bearer=MAUBOT_WEBHOOK_TOKEN, timeout=20)
                        if 200 <= code < 300:
                            log("INFO", "forwarded inbound", delivery_id=delivery_id, room=room, code=code)
                        else:
                            log("WARN", "maubot rejected", delivery_id=delivery_id, code=code, resp=resp)
                    except Exception as e:
                        log("WARN", "maubot forward failed", delivery_id=delivery_id, err=str(e))

        except Exception as e:
            log("WARN", "poll loop error", err=str(e))

        time.sleep(POLL_DB_SEC)

    log("INFO", "poller stopped")


# --------------------------------------------------------------------------------------
# Outbound worker
# --------------------------------------------------------------------------------------

def outbound_worker(stop_evt: threading.Event) -> None:
    init_state()
    log("INFO", "outbound worker starting")

    while not stop_evt.is_set():
        try:
            job = next_outbound_job()
            if not job:
                time.sleep(0.5)
                continue

            job_id = int(job["job_id"])
            room = str(job["matrix_room_id"])
            ev = str(job["matrix_event_id"])
            kind = str(job["kind"])
            conv = job["internet_conversation_id"]

            if not conv:
                link = get_link_by_room(room)
                if link:
                    conv = link["internet_conversation_id"]

            if not conv:
                update_outbound_job(job_id, "failed", "No internet_conversation_id for room; link required.")
                continue

            update_outbound_job(job_id, "sending", None)

            payload = {"kind": kind, "internet_conversation_id": conv, "matrix": {"room_id": room, "event_id": ev}}
            if kind == "text":
                payload["text"] = job["text"] or ""
            elif kind == "media":
                if not job["media_path"]:
                    update_outbound_job(job_id, "failed", "media_path missing")
                    continue
                payload["media_path"] = job["media_path"]
                if job["media_mime"]:
                    payload["media_mime"] = job["media_mime"]

            result = run_sender(payload)
            ota_uuid = result.get("ota_uuid")
            if not ota_uuid and isinstance(result.get("garmin"), dict):
                ota_uuid = result["garmin"].get("ota_uuid")

            if not ota_uuid:
                with garmin_db_conn() as con:
                    ota_uuid = poll_for_new_outgoing_ota(con, str(conv), int(time.time()))

            if ota_uuid:
                add_outbound_correlation(str(ota_uuid), room, ev)
                log("INFO", "outbound correlated", job_id=job_id, ota_uuid=str(ota_uuid))

            mark_outbound_sent(job_id)

        except Exception as e:
            log("WARN", "outbound worker error", err=str(e))
            time.sleep(1.0)

    log("INFO", "outbound worker stopped")


# --------------------------------------------------------------------------------------
# HTTP API
# --------------------------------------------------------------------------------------

def _auth_ok(headers: Dict[str, str]) -> bool:
    if not HTTP_TOKEN:
        return True
    auth = headers.get("authorization") or headers.get("Authorization") or ""
    return auth.startswith("Bearer ") and auth.split(" ", 1)[1].strip() == HTTP_TOKEN

def _read_json(handler: BaseHTTPRequestHandler) -> Dict[str, Any]:
    n = int(handler.headers.get("content-length") or "0")
    raw = handler.rfile.read(n) if n > 0 else b"{}"
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

class BridgeHandler(BaseHTTPRequestHandler):
    server_version = "garmin-bridge/1.0"

    def do_GET(self):  # noqa: N802
        if self.path == "/healthz":
            return _send(self, 200, {"ok": True})
        return _send(self, 404, {"ok": False, "error": "not found"})

    def do_POST(self):  # noqa: N802
        if not _auth_ok(dict(self.headers)):
            return _send(self, 401, {"ok": False, "error": "unauthorized"})

        if self.path == "/link":
            body = _read_json(self)
            conv = (body.get("internet_conversation_id") or "").strip()
            room = (body.get("matrix_room_id") or "").strip()
            if not conv or not room:
                return _send(self, 400, {"ok": False, "error": "internet_conversation_id and matrix_room_id required"})
            upsert_link(conv, room)
            log("INFO", "linked conversation", conv=conv, room=room)
            return _send(self, 200, {"ok": True})

        if self.path == "/outbound/enqueue":
            body = _read_json(self)
            room = (body.get("matrix_room_id") or "").strip()
            ev = (body.get("matrix_event_id") or "").strip()
            kind = (body.get("kind") or "text").strip()
            conv = (body.get("internet_conversation_id") or "").strip() or None
            if not room or not ev:
                return _send(self, 400, {"ok": False, "error": "matrix_room_id and matrix_event_id required"})

            job: Dict[str, Any] = {"matrix_room_id": room, "matrix_event_id": ev, "internet_conversation_id": conv, "kind": kind}
            if kind == "text":
                job["text"] = body.get("text") or ""
            elif kind == "media":
                if not body.get("media_path"):
                    return _send(self, 400, {"ok": False, "error": "media_path required for kind=media"})
                job["media_path"] = body.get("media_path")
                job["media_mime"] = body.get("media_mime") or None
            else:
                return _send(self, 400, {"ok": False, "error": f"unsupported kind: {kind}"})

            jid = enqueue_outbound(job)
            return _send(self, 200, {"ok": True, "job_id": jid})

        if self.path == "/inbound/ack":
            body = _read_json(self)
            delivery_id = (body.get("delivery_id") or body.get("ota_uuid") or "").strip()
            if not delivery_id:
                return _send(self, 400, {"ok": False, "error": "delivery_id or ota_uuid required"})
            ack_delivery(delivery_id)
            return _send(self, 200, {"ok": True})

        return _send(self, 404, {"ok": False, "error": "not found"})

    def log_message(self, format: str, *args: Any) -> None:
        if DEBUG:
            log("DEBUG", "http", path=self.path, msg=(format % args))

def http_server(stop_evt: threading.Event) -> None:
    init_state()
    srv = ThreadingHTTPServer((HTTP_BIND, int(HTTP_PORT)), BridgeHandler)
    log("INFO", "http server listening", bind=HTTP_BIND, port=HTTP_PORT)
    while not stop_evt.is_set():
        srv.handle_request()
    try:
        srv.server_close()
    except Exception:
        pass


# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------

def validate_env() -> None:
    if not DB_PATH or not os.path.isfile(DB_PATH):
        raise SystemExit(f"DB_PATH not found or not a file: {DB_PATH!r}")
    if not ROOT_DIR or not os.path.isdir(ROOT_DIR):
        raise SystemExit(f"ROOT_DIR not found or not a directory: {ROOT_DIR!r}")
    if MAUBOT_WEBHOOK_URL:
        u = urlparse(MAUBOT_WEBHOOK_URL)
        if not u.scheme or not u.netloc:
            raise SystemExit(f"MAUBOT_WEBHOOK_URL invalid: {MAUBOT_WEBHOOK_URL!r}")

def main() -> None:
    validate_env()
    init_state()

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
