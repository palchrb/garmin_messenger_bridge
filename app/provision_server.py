import json
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from typing import Optional, Tuple

from .security import bearer_ok, hmac_ok
from .logutil import log


def _read_body(handler: BaseHTTPRequestHandler) -> bytes:
    ln = int(handler.headers.get("Content-Length", "0"))
    return handler.rfile.read(ln)


def _client_ip(handler: BaseHTTPRequestHandler) -> str:
    # best-effort (no proxy assumptions)
    return (handler.client_address[0] or "").strip()


class ProvisionServer:
    def __init__(self, bind: str, port: int, *, bearer_secret: str, hmac_secret: str, allowlist, debug: bool, state_store, outbound_handler):
        self.bind = bind
        self.port = port
        self.bearer_secret = bearer_secret
        self.hmac_secret = hmac_secret
        self.allowlist = set(allowlist or [])
        self.debug = debug
        self.state = state_store
        self.outbound = outbound_handler
        self.httpd: Optional[ThreadingHTTPServer] = None

    def start(self) -> ThreadingHTTPServer:
        outer = self

        class Handler(BaseHTTPRequestHandler):
            server_version = "GarminBridge/2.0"

            def log_message(self, fmt, *args):
                if outer.debug:
                    log("HTTP", fmt % args, level="DEBUG", debug_enabled=True)

            def _bad(self, code: int, msg: str):
                self.send_response(code)
                self.send_header("Content-Type", "text/plain")
                self.end_headers()
                self.wfile.write(msg.encode("utf-8"))

            def _auth(self, body: bytes) -> bool:
                ip = _client_ip(self)
                if outer.allowlist and ip not in outer.allowlist:
                    self._bad(403, "ip_not_allowed")
                    return False

                auth = self.headers.get("Authorization", "")
                if not bearer_ok(auth, outer.bearer_secret):
                    self._bad(401, "bad_token")
                    return False

                sig = self.headers.get("X-Signature", "")
                if not hmac_ok(sig, body, outer.hmac_secret):
                    self._bad(401, "bad_signature")
                    return False

                return True

            def _read_json(self, body: bytes):
                try:
                    return json.loads(body.decode("utf-8"))
                except Exception:
                    return None

            def do_POST(self):
                body = _read_body(self)
                if not self._auth(body):
                    return
                payload = self._read_json(body)
                if payload is None:
                    self._bad(400, "invalid_json")
                    return

                # --- provisioning (maubot -> bridge) ---
                if self.path == "/provision":
                    msisdn = (payload.get("msisdn") or "").strip()
                    name = (payload.get("name") or "").strip()
                    code = (payload.get("verify_code") or "").strip()
                    wh = (payload.get("webhook_url") or "").strip()
                    tok = (payload.get("bearer_token") or "").strip()
                    if not (msisdn and name and code and wh and tok):
                        self._bad(400, "missing_fields")
                        return

                    existed = outer.state.subs_check_name_exists(msisdn, name)
                    outer.state.subs_set(msisdn, name, "pending", code, wh, tok)
                    log(f"Provision {'update' if existed else 'create'}: msisdn={msisdn} name={name}", level="INFO", debug_enabled=outer.debug)
                    self.send_response(200 if existed else 201)
                    self.end_headers()
                    self.wfile.write(b"ok")
                    return

                elif self.path == "/unprovision":
                    msisdn = (payload.get("msisdn") or "").strip()
                    name = (payload.get("name") or "").strip()
                    if not msisdn:
                        self._bad(400, "missing_msisdn")
                        return
                    outer.state.subs_deactivate(msisdn, name if name else None)
                    log(f"Unprovision: msisdn={msisdn} name={name or '*'}", level="INFO", debug_enabled=outer.debug)
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write(b"ok")
                    return

                elif self.path == "/rotate":
                    msisdn = (payload.get("msisdn") or "").strip()
                    name = (payload.get("name") or "").strip()
                    tok = (payload.get("bearer_token") or "").strip()
                    wh = (payload.get("webhook_url") or "").strip()
                    if not (msisdn and name and tok):
                        self._bad(400, "missing_fields")
                        return
                    ok = outer.state.subs_rotate_token(msisdn, name, tok, webhook_url=(wh or None))
                    if not ok:
                        self._bad(404, "not_found")
                        return
                    log(f"Rotate token: msisdn={msisdn} name={name}", level="INFO", debug_enabled=outer.debug)
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write(b"ok")
                    return

                # --- outbound control (maubot -> bridge) ---
                elif self.path == "/send_text":
                    # Expected: {"conversation_id": "...", "text": "...", "sender": "...", "meta": {...}}
                    ok, msg = outer.outbound.send_text(payload)
                    self.send_response(200 if ok else 500)
                    self.end_headers()
                    self.wfile.write((msg or "").encode("utf-8"))
                    return

                elif self.path == "/send_media":
                    ok, msg = outer.outbound.send_media(payload)
                    self.send_response(200 if ok else 500)
                    self.end_headers()
                    self.wfile.write((msg or "").encode("utf-8"))
                    return

                else:
                    self._bad(404, "not_found")

        self.httpd = ThreadingHTTPServer((self.bind, self.port), Handler)
        log(f"Provision HTTP listening on http://{self.bind}:{self.port}", level="INFO", debug_enabled=self.debug)
        return self.httpd
