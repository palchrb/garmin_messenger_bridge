import os
import signal
import sys
import threading
import time
from typing import Any, Dict, Tuple

from .adb import AdbClient
from .config import Config
from .garmin_ui import GarminUI
from .logutil import log
from .provision_server import ProvisionServer
from .state_store import StateStore
from .watcher import Watcher


class OutboundHandler:
    """Adapter that drives GarminUI for outbound sends."""

    def __init__(self, garmin_ui: GarminUI, debug: bool):
        self.garmin_ui = garmin_ui
        self.debug = debug

    def send_text(self, payload: Dict[str, Any]) -> Tuple[bool, str]:
        log(f"[OUTBOUND] send_text payload={payload}", level="INFO", debug_enabled=self.debug)
        msg = (payload.get("msg") or payload.get("text") or "").strip()
        if not msg:
            return False, "missing msg"

        try:
            if "thread_id" in payload and payload.get("thread_id") is not None:
                thread_id = int(payload.get("thread_id"))
                self.garmin_ui.send_to_thread(thread_id=thread_id, msg=msg)
                return True, "sent"

            recipients = payload.get("recipients")
            if isinstance(recipients, list) and recipients:
                cleaned = [str(r).strip() for r in recipients if str(r).strip()]
                if not cleaned:
                    return False, "empty recipients"
                self.garmin_ui.start_thread_and_send(recipients=cleaned, msg=msg)
                return True, "sent"
        except Exception as e:  # pragma: no cover - ADB/Runtime failures
            log(f"[OUTBOUND] send_text failed: {e}", level="ERROR", debug_enabled=self.debug)
            return False, str(e)

        return False, "missing thread_id or recipients"

    def send_media(self, payload: Dict[str, Any]) -> Tuple[bool, str]:
        # Media sending not yet implemented; log and fail gracefully.
        log(f"[OUTBOUND] send_media payload={payload}", level="INFO", debug_enabled=self.debug)
        return False, "send_media not implemented"


def main():
    cfg = Config()

    if not cfg.db_path or not os.path.isfile(cfg.db_path):
        log(f"DB not found: {cfg.db_path}", level="ERROR", debug_enabled=True)
        sys.exit(1)
    if not cfg.root_dir or not os.path.isdir(cfg.root_dir):
        log(f"ROOT_DIR not found: {cfg.root_dir}", level="ERROR", debug_enabled=True)
        sys.exit(1)
    if not cfg.provision_secret or len(cfg.provision_secret) < 16:
        log("Weak or missing PROVISION_SECRET — set a strong value!", level="ERROR", debug_enabled=True)

    os.makedirs(cfg.state_dir, exist_ok=True)

    state = StateStore(cfg.subs_db)

    stop_evt = threading.Event()

    adb = AdbClient(adb_path=cfg.adb_path, target=cfg.adb_target, cmd_timeout_sec=cfg.adb_cmd_timeout_sec)
    garmin_ui = GarminUI(adb)
    outbound = OutboundHandler(garmin_ui, cfg.debug)

    server = ProvisionServer(
        cfg.bind,
        cfg.port,
        bearer_secret=cfg.provision_secret,
        hmac_secret=cfg.hmac_secret,
        allowlist=cfg.allowlist,
        debug=cfg.debug,
        outbound_handler=outbound,
    )
    httpd = server.start()
    http_thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    http_thread.start()

    watcher = Watcher(cfg=cfg, state=state)
    watch_thread = threading.Thread(target=watcher.loop, args=(stop_evt,), daemon=True)
    watch_thread.start()

    def _sig(signum, frame):  # pragma: no cover - signal handler
        log(f"Signal {signum} → shutting down", level="INFO", debug_enabled=True)
        stop_evt.set()
        try:
            httpd.shutdown()
        except Exception:
            pass
        sys.exit(0)

    signal.signal(signal.SIGINT, _sig)
    signal.signal(signal.SIGTERM, _sig)

    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
