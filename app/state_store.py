import os
import sqlite3
import threading
import time
from typing import Any, Dict, List, Optional


class StateStore:
    """SQLite-backed state for subscriptions and deduplication."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._lock = threading.Lock()
        self._init_db()

    # ---------------- internal helpers ----------------
    def _conn(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.db_path, timeout=5.0, isolation_level=None, check_same_thread=False)
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA synchronous=NORMAL;")
        return con

    def _init_db(self) -> None:
        with self._conn() as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS subs (
                    msisdn TEXT NOT NULL,
                    name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    verify_code TEXT,
                    webhook_url TEXT,
                    bearer_token TEXT,
                    created_ts INTEGER,
                    updated_ts INTEGER,
                    PRIMARY KEY (msisdn, name)
                );
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS seen (
                    key TEXT PRIMARY KEY,
                    created_ts INTEGER
                );
                """
            )

    # ---------------- subs ----------------
    def subs_get(self, msisdn: str) -> Dict[str, Any]:
        with self._lock, self._conn() as con:
            cur = con.execute(
                "SELECT name, status, verify_code, webhook_url, bearer_token, created_ts, updated_ts FROM subs WHERE msisdn=?",
                (msisdn,),
            )
            res = {}
            for row in cur.fetchall():
                name, status, verify_code, webhook_url, bearer_token, created_ts, updated_ts = row
                res[name] = {
                    "name": name,
                    "status": status,
                    "verify_code": verify_code or "",
                    "webhook_url": webhook_url or "",
                    "bearer_token": bearer_token or "",
                    "created_ts": int(created_ts or 0),
                    "updated_ts": int(updated_ts or 0),
                }
            return res

    def subs_set(self, msisdn: str, name: str, status: str, verify_code: str, url: str, token: str) -> None:
        now = int(time.time())
        nkey = (name or "").lower()
        with self._lock, self._conn() as con:
            cur = con.execute(
                "SELECT created_ts FROM subs WHERE msisdn=? AND name=?", (msisdn, nkey)
            )
            row = cur.fetchone()
            created_ts = int(row[0]) if row else now
            con.execute(
                """
                INSERT INTO subs (msisdn, name, status, verify_code, webhook_url, bearer_token, created_ts, updated_ts)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(msisdn, name) DO UPDATE SET
                    status=excluded.status,
                    verify_code=excluded.verify_code,
                    webhook_url=excluded.webhook_url,
                    bearer_token=excluded.bearer_token,
                    updated_ts=excluded.updated_ts
                """,
                (msisdn, nkey, status, str(verify_code or ""), url, token, created_ts, now),
            )

    def subs_check_name_exists(self, msisdn: str, name: str) -> bool:
        nkey = (name or "").lower()
        with self._lock, self._conn() as con:
            cur = con.execute("SELECT 1 FROM subs WHERE msisdn=? AND name=?", (msisdn, nkey))
            return cur.fetchone() is not None

    def subs_activate_if_code(self, msisdn: str, name: str, code: str) -> bool:
        nkey = (name or "").lower()
        with self._lock, self._conn() as con:
            cur = con.execute(
                "SELECT verify_code FROM subs WHERE msisdn=? AND name=?", (msisdn, nkey)
            )
            row = cur.fetchone()
            if not row or str(row[0] or "") != str(code):
                return False
            now = int(time.time())
            con.execute(
                "UPDATE subs SET status='active', updated_ts=? WHERE msisdn=? AND name=?",
                (now, msisdn, nkey),
            )
        return True

    def subs_deactivate(self, msisdn: str, name: Optional[str] = None) -> None:
        now = int(time.time())
        with self._lock, self._conn() as con:
            if name:
                nkey = name.lower()
                con.execute(
                    "UPDATE subs SET status='inactive', updated_ts=? WHERE msisdn=? AND name=?",
                    (now, msisdn, nkey),
                )
            else:
                con.execute(
                    "UPDATE subs SET status='inactive', updated_ts=? WHERE msisdn=?",
                    (now, msisdn),
                )

    def subs_rotate_token(self, msisdn: str, name: str, bearer_token: str, webhook_url: Optional[str] = None) -> bool:
        if not msisdn or not name or not bearer_token:
            return False
        now = int(time.time())
        nkey = name.lower()
        with self._lock, self._conn() as con:
            cur = con.execute("SELECT 1 FROM subs WHERE msisdn=? AND name=?", (msisdn, nkey))
            if cur.fetchone() is None:
                return False
            con.execute(
                """
                UPDATE subs
                SET bearer_token=?, webhook_url=COALESCE(?, webhook_url), updated_ts=?
                WHERE msisdn=? AND name=?
                """,
                (bearer_token, webhook_url, now, msisdn, nkey),
            )
        return True

    def active_targets(self, msisdn: str) -> List[Dict[str, Any]]:
        with self._lock, self._conn() as con:
            cur = con.execute(
                """
                SELECT name, status, webhook_url, bearer_token, created_ts, updated_ts
                FROM subs
                WHERE msisdn=? AND status='active'
                """,
                (msisdn,),
            )
            out = []
            for name, status, webhook_url, bearer_token, created_ts, updated_ts in cur.fetchall():
                out.append(
                    {
                        "name": name,
                        "status": status,
                        "webhook_url": webhook_url or "",
                        "bearer_token": bearer_token or "",
                        "created_ts": int(created_ts or 0),
                        "updated_ts": int(updated_ts or 0),
                    }
                )
            return out

    # ---------------- seen ----------------
    def is_seen(self, key: str) -> bool:
        with self._lock, self._conn() as con:
            cur = con.execute("SELECT 1 FROM seen WHERE key=?", (key,))
            return cur.fetchone() is not None

    def add_seen(self, key: str) -> None:
        now = int(time.time())
        with self._lock, self._conn() as con:
            con.execute(
                "INSERT OR IGNORE INTO seen (key, created_ts) VALUES (?, ?)",
                (key, now),
            )

