# app/provision_server.py
from __future__ import annotations

import json
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from typing import Any, Dict, Optional, Tuple, List

from .config import CFG
from .logutil import get_logger
from .adb import AdbClient, AdbError
from .garmin_ui import GarminUI, GarminApp

log = get_logger(__name__)


def _parse_allowlist(raw: str) -> set[str]:
    items = [x.strip() for x in (raw or "").split(",") if x.strip()]
    return set(items)


_ALLOWLIST = _parse_allowlist(CFG.PROVISION_ALLOWLIST)


def _client_ip(handler: BaseHTTPRequestHandler) -> str:
    # handler.client_address is (ip, port)
    try:
        return handler.client_address[0]
    except Exception:
        return ""


class ProvisionServer:
    """
    Minimal HTTP API for outbound control (Maubot -> bridge).
    """

    def __init__(self) -> None:
        self._srv: Optional[ThreadingHTTPServer] = None

    def start(self) -> None:
        self._srv = ThreadingHTTPServer((CFG.PROVISION_BIND, CFG.PROVISION_PORT), _Handler)
        log.info("Provision server listening on http://%s:%s", CFG.PROVISION_BIND, CFG.PROVISION_PORT)
        self._srv.serve_forever()

    def shutdown(self) -> None:
        if self._srv:
            self._srv.shutdown()


class _Handler(BaseHTTPRequestHandler):
    server_version = "GarminBridge/2.0"

    def _send(self, code: int, body: str, content_type: str = "text/plain; charset=utf-8") -> None:
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.end_headers()
        self.wfile.write(body.encode("utf-8"))

    def _read_json(self) -> Optional[Dict[str, Any]]:
        try:
            ln = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(ln)
            return json.loads(raw.decode("utf-8"))
        except Exception:
            return None

    def _auth_ok(self) -> bool:
        # allowlist
        if _ALLOWLIST:
            ip = _client_ip(self)
            if ip not in _ALLOWLIST:
                return False

        # bearer auth
        auth = (self.headers.get("Authorization") or "").strip()
        if not auth.lower().startswith("bearer "):
            return False
        token = auth.split(" ", 1)[1].strip()
        return bool(CFG.PROVISION_SECRET) and token == CFG.PROVISION_SECRET

    def do_POST(self) -> None:
        if not self._auth_ok():
            self._send(401, "unauthorized")
            return

        payload = self._read_json()
        if payload is None:
            self._send(400, "invalid_json")
            return

        adb = AdbClient()
        gu = GarminUI(adb, GarminApp())

        # --- /send: existing thread ---
        if self.path == "/send":
            try:
                thread_id = int(payload.get("thread_id"))
                msg = (payload.get("msg") or "").strip()
                if not msg:
                    self._send(400, "missing_msg")
                    return
                gu.send_to_thread(thread_id=thread_id, msg=msg)
                self._send(200, "ok")
            except (ValueError, TypeError):
                self._send(400, "bad_thread_id")
            except (AdbError, RuntimeError) as e:
                log.warning("send failed: %s", e)
                self._send(503, f"send_failed: {e}")
            except Exception as e:
                log.exception("send crashed")
                self._send(500, f"send_crashed: {e}")
            return

        # --- /start: new conversation ---
        if self.path == "/start":
            try:
                recipients = payload.get("recipients")
                msg = (payload.get("msg") or "").strip()

                if not isinstance(recipients, list) or not recipients:
                    self._send(400, "missing_recipients")
                    return
                recipients = [str(x).strip() for x in recipients if str(x).strip()]
                if not recipients:
                    self._send(400, "missing_recipients")
                    return
                if not msg:
                    self._send(400, "missing_msg")
                    return

                gu.start_thread_and_send(recipients=recipients, msg=msg)
                self._send(200, "ok")
            except (AdbError, RuntimeError, ValueError) as e:
                log.warning("start failed: %s", e)
                self._send(503, f"start_failed: {e}")
            except Exception as e:
                log.exception("start crashed")
                self._send(500, f"start_crashed: {e}")
            return

        self._send(404, "not_found")

    def log_message(self, fmt: str, *args: Any) -> None:
        if CFG.DEBUG:
            super().log_message(fmt, *args)
