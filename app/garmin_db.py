import os
import sqlite3
import time
from typing import Iterator, Tuple, Optional, List


def db_conn_ro(db_path: str) -> sqlite3.Connection:
    uri = f"file:{db_path}?mode=ro&cache=shared"
    con = sqlite3.connect(uri, uri=True, timeout=2.5, isolation_level=None, check_same_thread=False)
    try:
        con.execute("PRAGMA read_uncommitted=1;")
    except Exception:
        pass
    return con


def fmt_local(ts_val) -> str:
    try:
        s = int(ts_val)
        if s > 1_000_000_000_000:
            s = int(s / 1000)
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(s))
    except Exception:
        return str(ts_val)


def lookup_msisdn(con: sqlite3.Connection, thread_id: int) -> str:
    try:
        cur = con.execute("SELECT addresses FROM message_thread WHERE id=?", (thread_id,))
        r = cur.fetchone()
        return r[0] if r and r[0] else ""
    except Exception:
        return ""


def iter_new_messages(con: sqlite3.Connection, last_id: int, tail_limit: int) -> Iterator[Tuple[int, str, int, int, Optional[int]]]:
    q = """
    SELECT m.id, COALESCE(m.text,''), m.message_thread_id, m.sent_time, m.media_attachment_id
    FROM message m
    WHERE m.id > ?
    ORDER BY m.id ASC
    LIMIT ?
    """
    for row in con.execute(q, (last_id, tail_limit)):
        yield row


def media_lookup(con: sqlite3.Connection, attach_id: int) -> Tuple[Optional[str], str]:
    q = """
    SELECT mr.media_type, COALESCE(mf.file_id,'')
    FROM media_attachment_record mr
    LEFT JOIN media_attachment_file mf ON mf.attachment_id = mr.attachment_id
    WHERE mr.attachment_id = ?
    ORDER BY IFNULL(mf.fileSize,0) DESC
    LIMIT 1
    """
    cur = con.execute(q, (attach_id,))
    r = cur.fetchone()
    if not r:
        return None, ""
    return r[0], (r[1] or "")


def find_media_path(root_dir: str, media_exts: List[str], file_id: str, attach_id: Optional[str] = None) -> str:
    ids = [x for x in [file_id, attach_id] if x]
    if not ids:
        return ""
    roots = [os.path.join(root_dir, x) for x in ("high", "preview", "low", "audio")]
    for the_id in ids:
        for d in roots:
            for ext in media_exts:
                p = os.path.join(d, f"{the_id}.{ext}")
                if os.path.isfile(p):
                    return p
    return ""
