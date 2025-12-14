# app/provision_server.py
from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, Optional

from .logutil import get_logger
from .security import bearer_ok, hmac_ok

log = get_logger(__name__)


class ProvisionServer:
    """Minimal HTTP API for outbound control (Maubot -> bridge)."""

    def __init__(
        self,
        bind: str,
        port: int,
        *,
        bearer_secret: str,
        hmac_secret: str,
        allowlist: list[str],
        debug: bool,
        outbound_handler,
    ) -> None:
        self.bind = bind
        self.port = port
        self.bearer_secret = bearer_secret
        self.hmac_secret = hmac_secret
        self.allowlist = set(allowlist or [])
        self.debug = debug
        self.outbound_handler = outbound_handler
        self._srv: Optional[ThreadingHTTPServer] = None

    def start(self) -> ThreadingHTTPServer:
        Handler = self._build_handler()
        self._srv = ThreadingHTTPServer((self.bind, self.port), Handler)
        log.info("Provision server listening on http://%s:%s", self.bind, self.port)
        return self._srv

    def _build_handler(self):
        outer = self

        class _Handler(BaseHTTPRequestHandler):
            server_version = "GarminBridge/2.0"

            def _client_ip(self) -> str:
                try:
                    return self.client_address[0]
                except Exception:
                    return ""

            def _send(self, code: int, body: str, content_type: str = "application/json") -> None:
                self.send_response(code)
                self.send_header("Content-Type", content_type)
                self.end_headers()
                self.wfile.write(body.encode("utf-8"))

            def _read_json(self) -> Optional[Dict[str, Any]]:
                try:
                    ln = int(self.headers.get("Content-Length", "0"))
                    raw = self.rfile.read(ln)
                    if not hmac_ok(self.headers.get("X-Signature", ""), raw, outer.hmac_secret):
                        return None
                    return json.loads(raw.decode("utf-8"))
                except Exception:
                    return None

            def _auth_ok(self) -> bool:
                if outer.allowlist:
                    if self._client_ip() not in outer.allowlist:
                        return False
                return bearer_ok(self.headers.get("Authorization", ""), outer.bearer_secret)

            def do_POST(self) -> None:  # noqa: N802
                if not self._auth_ok():
                    self._send(401, json.dumps({"error": "unauthorized"}))
                    return

                payload = self._read_json()
                if payload is None:
                    self._send(400, json.dumps({"error": "invalid_json_or_signature"}))
                    return

                if self.path == "/send_text":
                    ok, msg = outer.outbound_handler.send_text(payload)
                    code = 200 if ok else 503
                    self._send(code, json.dumps({"ok": ok, "message": msg}))
                    return

                if self.path == "/send_media":
                    ok, msg = outer.outbound_handler.send_media(payload)
                    code = 200 if ok else 503
                    self._send(code, json.dumps({"ok": ok, "message": msg}))
                    return

                self._send(404, json.dumps({"error": "not_found"}))

            def log_message(self, fmt: str, *args: Any) -> None:  # noqa: D401
                if outer.debug:
                    super().log_message(fmt, *args)

        return _Handler

