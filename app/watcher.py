import os
import threading
import time
from typing import Dict, Tuple

from .garmin_db import db_conn_ro, iter_new_messages, lookup_msisdn, media_lookup, find_media_path, fmt_local
from .forwarder import forward_media
from .logutil import log


class Watcher:
    def __init__(self, *, cfg, state):
        self.cfg = cfg
        self.state = state

    def handle_text_commands(self, msisdn: str, text: str):
        t = (text or "").strip()
        if not t:
            return
        parts = t.split()
        if not parts:
            return
        cmd = parts[0].lower()

        if cmd == "sub" and len(parts) >= 3:
            name = parts[1]
            code = parts[2]
            ok = self.state.subs_activate_if_code(msisdn, name, code)
            if ok:
                log(f"Activated sub msisdn={msisdn} name={name}", level="INFO", debug_enabled=self.cfg.debug)
            else:
                log(f"Sub verify failed msisdn={msisdn} name={name}", level="DEBUG", debug_enabled=self.cfg.debug)
            return

        if cmd == "unsub":
            if len(parts) >= 2:
                name = parts[1]
                self.state.subs_deactivate(msisdn, name)
                log(f"Unsub one msisdn={msisdn} name={name}", level="INFO", debug_enabled=self.cfg.debug)
            else:
                self.state.subs_deactivate(msisdn, None)
                log(f"Unsub ALL msisdn={msisdn}", level="INFO", debug_enabled=self.cfg.debug)
            return

    def boot_dump(self):
        if self.cfg.last_n_boot <= 0:
            return
        try:
            with db_conn_ro(self.cfg.db_path) as con:
                q = """
                SELECT m.id, COALESCE(m.text,''), m.message_thread_id, m.sent_time, m.media_attachment_id
                FROM message m
                ORDER BY m.id DESC
                LIMIT ?
                """
                rows = list(con.execute(q, (self.cfg.last_n_boot,)))
                rows.reverse()
                for (mid, text, thread, sent, media_attach) in rows:
                    msisdn = lookup_msisdn(con, thread)
                    if media_attach:
                        _, fid = media_lookup(con, media_attach)
                        attach = str(media_attach)
                        path = find_media_path(self.cfg.root_dir, self.cfg.media_exts, fid, attach)
                        log(f"[BOOT][MEDIA] id={mid} msisdn={msisdn} caption={text!r} attach={attach} file={path!r} sent_local={fmt_local(sent)}",
                            level="DEBUG", debug_enabled=self.cfg.debug)
                    else:
                        log(f"[BOOT][TEXT] id={mid} msisdn={msisdn} text={text!r} sent_local={fmt_local(sent)}",
                            level="DEBUG", debug_enabled=self.cfg.debug)
        except Exception as e:
            log(f"Boot dump failed: {e}", level="DEBUG", debug_enabled=self.cfg.debug)

    def loop(self, stop_evt: threading.Event):
        last_id = 0
        try:
            with db_conn_ro(self.cfg.db_path) as con:
                cur = con.execute("SELECT IFNULL(MAX(id),0) FROM message;")
                row = cur.fetchone()
                last_id = int(row[0] or 0)
        except Exception as e:
            log(f"Init last_id failed: {e}", level="DEBUG", debug_enabled=self.cfg.debug)

        self.boot_dump()
        log(f"[watch] Polling DB every {self.cfg.poll_db_sec}s tail={self.cfg.tail_limit}", level="INFO", debug_enabled=self.cfg.debug)

        pending_media: Dict[str, Tuple[str, int, str, str]] = {}

        while not stop_evt.is_set():
            try:
                with db_conn_ro(self.cfg.db_path) as con:
                    for (mid, text, thread, sent, media_attach) in iter_new_messages(con, last_id, self.cfg.tail_limit):
                        last_id = max(last_id, int(mid))
                        msisdn = lookup_msisdn(con, thread)
                        key = f"msg:{mid}"

                        if media_attach:
                            _, fid = media_lookup(con, media_attach)
                            attach = str(media_attach)
                            path = find_media_path(self.cfg.root_dir, self.cfg.media_exts, fid, attach)

                            pending_media[attach] = (msisdn, mid, text, fid)

                            if path and not self.state.is_seen(key):
                                targets = self.state.active_targets(msisdn)
                                forward_media(
                                    msisdn=msisdn,
                                    msg_id=mid,
                                    attach_id=attach,
                                    path=path,
                                    caption=text,
                                    targets=targets,
                                    debug=self.cfg.debug,
                                    forward_mode=self.cfg.forward_mode,
                                    caption_targeting=self.cfg.caption_targeting,
                                    target_word_strip=self.cfg.target_word_strip,
                                    http_timeout_sec=self.cfg.http_timeout_sec,
                                    retry_backoffs=self.cfg.retry_backoffs,
                                    delete_on_success=self.cfg.delete_on_success,
                                    delete_delay_sec=self.cfg.delete_delay_sec,
                                    deactivate_cb=self.state.subs_deactivate,
                                )
                                self.state.add_seen(key)

                        else:
                            self.handle_text_commands(msisdn, text)

                    # late-appearing media
                    for attach, (msisdn, mid, text, fid) in list(pending_media.items()):
                        path = find_media_path(self.cfg.root_dir, self.cfg.media_exts, fid, attach)
                        if path:
                            key = f"msg:{mid}"
                            if not self.state.is_seen(key):
                                targets = self.state.active_targets(msisdn)
                                forward_media(
                                    msisdn=msisdn,
                                    msg_id=mid,
                                    attach_id=attach,
                                    path=path,
                                    caption=text,
                                    targets=targets,
                                    debug=self.cfg.debug,
                                    forward_mode=self.cfg.forward_mode,
                                    caption_targeting=self.cfg.caption_targeting,
                                    target_word_strip=self.cfg.target_word_strip,
                                    http_timeout_sec=self.cfg.http_timeout_sec,
                                    retry_backoffs=self.cfg.retry_backoffs,
                                    delete_on_success=self.cfg.delete_on_success,
                                    delete_delay_sec=self.cfg.delete_delay_sec,
                                    deactivate_cb=self.state.subs_deactivate,
                                )
                                self.state.add_seen(key)
                            pending_media.pop(attach, None)

            except Exception as e:
                log(f"Loop error: {e}", level="DEBUG", debug_enabled=self.cfg.debug)

            stop_evt.wait(self.cfg.poll_db_sec)
