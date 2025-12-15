#!/usr/bin/env python3
"""
Garmin Messenger ↔ Matrix (Maubot) bridge.

This build uses:
- Minimal, schema-robust SELECT against Garmin `message` table (matching your PRAGMA table_info(message)).
- Proven attachment-on-disk scan strategy (attachment_id only) identical in spirit to your email forwarder.
- Pending media re-scan.
- Outbound worker + echo suppression via ota_uuid correlation.

Enhancement vs prior build:
- Adds `media.mime` derived from filename extension (mimetypes.guess_type) with small overrides for
  avif / ogg / oga / m4a to keep Maubot handling consistent.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import mimetypes
import os
import signal
import sqlite3
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse
import urllib.request


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
GARMIN_PKG = env("GARMIN_PKG", "com.garmin.android.apps.messenger")
GARMIN_ACT = env("GARMIN_ACT", ".activity.MainActivity")

ADB_DEFAULT_SLEEP = env("ADB_DEFAULT_SLEEP", 0.8, float)
ADB_COMPOSE_SLEEP = env("ADB_COMPOSE_SLEEP", 1.2, float)
ADB_DISMISS_BACK_TWICE = env("ADB_DISMISS_BACK_TWICE", True, bool)

# ---- Outbound DB correlation ----
OUTBOUND_DB_POLL_SEC = env("OUTBOUND_DB_POLL_SEC", 0.5, float)
OUTBOUND_DB_POLL_MAX_SEC = env("OUTBOUND_DB_POLL_MAX_SEC", 12, int)
OUTBOUND_ENQUEUE_WAIT_NEWTHREAD_SEC = env("OUTBOUND_ENQUEUE_WAIT_NEWTHREAD_SEC", 8, int)


_LEVELS = {"DEBUG": 10, "INFO": 20, "WARN": 30, "WARNING": 30, "ERROR": 40}
_CUR_LEVEL = _LEVELS.get(str(LOG_LEVEL).upper(), 20)


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
    last_error TEXT
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
    result_json TEXT
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


def post_json(url: str, payload: Dict[str, Any], bearer: str = "", timeout: int = 20) -> Tuple[int, str]:
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json; charset=utf-8")
    if bearer:
        req.add_header("Authorization", f"Bearer {bearer}")
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


def is_acked(delivery_id: str) -> bool:
    with state_db_conn() as con:
        r = con.execute("SELECT acked FROM inbound_delivery WHERE delivery_id=? LIMIT 1", (delivery_id,)).fetchone()
        return bool(r and int(r["acked"] or 0) == 1)


def ack_delivery(delivery_id: str) -> None:
    now = int(time.time())
    with state_db_conn() as con:
        con.execute("UPDATE inbound_delivery SET acked=1, delivered_ts=? WHERE delivery_id=?", (now, delivery_id))
        con.commit()


def upsert_inbound_record(delivery: Dict[str, Any]) -> None:
    with state_db_conn() as con:
        con.execute(
            """
            INSERT INTO inbound_delivery(
              delivery_id, internet_conversation_id, message_row_id, origin, sent_time, sort_time, from_addr, text,
              media_attachment_id
            ) VALUES(?,?,?,?,?,?,?,?,?)
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


def enqueue_outbound(job: Dict[str, Any]) -> int:
    now = int(time.time())
    with state_db_conn() as con:
        con.execute(
            """
            INSERT INTO outbound_job(
              matrix_room_id, matrix_event_id, internet_conversation_id, kind, text, recipients_json,
              created_ts, updated_ts, status, attempts
            ) VALUES(?,?,?,?,?,?,?,?, 'queued', 0)
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
        return int(r["job_id"]) if r else 0


def next_outbound_job() -> Optional[sqlite3.Row]:
    with state_db_conn() as con:
        return con.execute(
            "SELECT * FROM outbound_job WHERE status IN ('queued','failed') ORDER BY updated_ts ASC, job_id ASC LIMIT 1"
        ).fetchone()


def update_outbound_job(job_id: int, status: str, error: Optional[str] = None, result: Optional[Dict[str, Any]] = None) -> None:
    now = int(time.time())
    res_json = json.dumps(result, ensure_ascii=False) if result is not None else None
    with state_db_conn() as con:
        con.execute(
            "UPDATE outbound_job SET status=?, updated_ts=?, attempts=attempts+1, last_error=?, result_json=? WHERE job_id=?",
            (status, now, error, res_json, job_id),
        )
        con.commit()


def mark_outbound_sent(job_id: int, result: Optional[Dict[str, Any]] = None) -> None:
    now = int(time.time())
    res_json = json.dumps(result, ensure_ascii=False) if result is not None else None
    with state_db_conn() as con:
        con.execute("UPDATE outbound_job SET status='sent', updated_ts=?, result_json=? WHERE job_id=?", (now, res_json, job_id))
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


def adb_sleep(sec: float) -> None:
    time.sleep(max(0.0, sec))


def adb_dismiss_overlays() -> None:
    if not ADB_DISMISS_BACK_TWICE:
        return
    try:
        adb_shell("input keyevent 4; input keyevent 4", timeout=10)
    except Exception:
        pass


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


def send_to_existing_thread(thread_id: int, msg: str) -> None:
    adb_dismiss_overlays()
    start_activity_view(f"inreach://messageThread/{int(thread_id)}")
    adb_sleep(ADB_DEFAULT_SLEEP)
    ui_tap_center_of_node_grep("com.garmin.android.apps.messenger:id/newMessageInputEditText")
    adb_shell(f'input text "{adb_text_escape(msg)}"', timeout=20)
    adb_sleep(0.2)
    ui_tap_center_of_node_grep("com.garmin.android.apps.messenger:id/sendButton")


def compose_new_thread_and_send(recipients: List[str], msg: str) -> None:
    if not recipients:
        raise ValueError("recipients must not be empty")
    adb_dismiss_overlays()
    start_activity_view("inreach://composedevicemessage/")
    adb_sleep(ADB_COMPOSE_SLEEP)
    ui_tap_first_garmin_edittext()
    for r in recipients:
        adb_shell(f'input text "{adb_text_escape(str(r))}"', timeout=20)
        adb_shell("input keyevent 66", timeout=10)
        adb_sleep(0.3)
        adb_shell("input keyevent 61", timeout=10)
        adb_sleep(0.2)
        adb_shell("input keyevent 23", timeout=10)
        adb_sleep(0.5)
    ui_tap_center_of_node_grep("com.garmin.android.apps.messenger:id/newMessageInputEditText")
    adb_shell(f'input text "{adb_text_escape(msg)}"', timeout=20)
    adb_sleep(0.2)
    ui_tap_center_of_node_grep("com.garmin.android.apps.messenger:id/sendButton")


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
          id,
          message_thread_id,
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


@dataclasses.dataclass
class PendingMedia:
    delivery_id: str
    row: sqlite3.Row
    first_seen_ts: float
    last_scan_ts: float
    attempts: int


def forward_to_maubot(con: sqlite3.Connection, r: sqlite3.Row, delivery_id: str) -> bool:
    conv = r["internet_conversation_id"]
    link = get_link_by_conversation(str(conv))
    if not link:
        log("DEBUG", "unlinked conversation - skip forward", conv=conv, id=r["id"])
        return False
    if not MAUBOT_WEBHOOK_URL:
        log("WARN", "MAUBOT_WEBHOOK_URL not set; cannot forward", delivery_id=delivery_id)
        return False

    room = link["matrix_room_id"]
    origin = int(r["origin"] or 0)
    ota_uuid = r["ota_uuid"]

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
        "internet_conversation_id": str(conv),
        "matrix_room_id": str(room),
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
            return True
        log("WARN", "maubot rejected", delivery_id=delivery_id, code=code, resp=resp[:600])
        return False
    except Exception as e:
        log("WARN", "maubot forward failed", delivery_id=delivery_id, err=str(e))
        return False


def poll_loop(stop_evt: threading.Event) -> None:
    init_state()
    cur = load_cursor("poll_cursor", "0:0")
    try:
        after_sort_time, after_id = [int(x) for x in cur.split(":", 1)]
    except Exception:
        after_sort_time, after_id = 0, 0

    pending: Dict[str, PendingMedia] = {}
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
                    if origin == 0 and ota_uuid:
                        corr = lookup_outbound_by_ota(str(ota_uuid))
                        if corr:
                            log("DEBUG", "echo suppressed", ota_uuid=ota_uuid, matrix_event_id=corr["matrix_event_id"])
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
                        pending.pop(did, None)

        except Exception as e:
            log("WARN", "poll loop error", err=str(e))

        time.sleep(max(0.05, float(POLL_DB_SEC)))

    log("INFO", "poller stopped")


# ---- Outbound worker ----

def _emit_outbound_result(event: Dict[str, Any]) -> None:
    if not MAUBOT_WEBHOOK_URL:
        return
    payload = {"event_type": "bridge_outbound_result", **event}
    try:
        code, _ = post_json(MAUBOT_WEBHOOK_URL, payload, bearer=MAUBOT_WEBHOOK_TOKEN, timeout=20)
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

            update_outbound_job(job_id, "sending", None)

            with garmin_db_conn() as con_db:
                if kind == "text":
                    if not conv:
                        update_outbound_job(job_id, "failed", "internet_conversation_id required for kind=text")
                        continue
                    thread_id = resolve_thread_id_for_conversation(con_db, str(conv))
                    if not thread_id:
                        update_outbound_job(job_id, "failed", "Could not resolve message_thread_id for conversation (no messages yet).")
                        continue
                    before_st, before_id = snapshot_conv_cursor(con_db, str(conv))
                    text = str(job["text"] or "")
                    send_to_existing_thread(int(thread_id), text)

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
                        update_outbound_job(job_id, "failed", "recipients_json must be a non-empty JSON list")
                        continue
                    text = str(job["text"] or "")
                    before_st0, before_id0 = snapshot_global_cursor(con_db)
                    compose_new_thread_and_send([str(x) for x in recipients], text)
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
                    if ota_uuid:
                        add_outbound_correlation(str(ota_uuid), room, ev)
                        log("INFO", "new_thread correlated", job_id=job_id, ota_uuid=str(ota_uuid), conv=str(conv_from_row))
                    else:
                        log("WARN", "new_thread correlation failed", job_id=job_id)

                    mark_outbound_sent(job_id, result=result)
                    _emit_outbound_result(result)
                    continue

                update_outbound_job(job_id, "failed", f"Unsupported kind: {kind}")

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
    server_version = "garmin-bridge/2.2"

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
            body = _read_json(self)
            delivery_id = (body.get("delivery_id") or body.get("ota_uuid") or "").strip()
            if not delivery_id:
                return _send(self, 400, {"ok": False, "error": "delivery_id or ota_uuid required"})
            ack_delivery(delivery_id)
            return _send(self, 200, {"ok": True})

        return _send(self, 404, {"ok": False, "error": "not found"})

    def log_message(self, format: str, *args: Any) -> None:
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
