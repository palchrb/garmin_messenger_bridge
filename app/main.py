import os
import signal
import sys
import threading
import time

from .config import Config
from .logutil import log
from .state_store import StateStore
from .provision_server import ProvisionServer
from .watcher import Watcher
from .adb import ADBManager


class OutboundHandler:
    """
    Placeholder. Wire in your existing 'working ADB/UIAutomator calls' here.
    Maubot will call /send_text and /send_media on the bridge.
    """
    def __init__(self, adb: ADBManager, debug: bool):
        self.adb = adb
        self.debug = debug

    def send_text(self, payload):
        # TODO: implement with your known-good ADB/uiautomator routines
        # Expected payload example:
        # {"conversation_id":"...", "text":"...", "sender":"@me:server", "meta": {...}}
        log(f"[OUTBOUND] send_text payload={payload}", level="INFO", debug_enabled=self.debug)
        return True, "queued/ok (stub)"

    def send_media(self, payload):
        log(f"[OUTBOUND] send_media payload={payload}", level="INFO", debug_enabled=self.debug)
        return True, "queued/ok (stub)"


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

    state = StateStore(cfg.resolved_subs_json(), cfg.resolved_seen_file())

    stop_evt = threading.Event()

    # ADB keepalive
    adb = ADBManager(adb_target=cfg.adb_target, keepalive_sec=cfg.adb_keepalive_sec, debug=cfg.debug)
    adb_thread = threading.Thread(target=adb.keepalive_loop, args=(stop_evt,), daemon=True)
    adb_thread.start()

    outbound = OutboundHandler(adb, cfg.debug)

    # Provision server
    server = ProvisionServer(
        cfg.bind,
        cfg.port,
        bearer_secret=cfg.provision_secret,
        hmac_secret=cfg.hmac_secret,
        allowlist=cfg.allowlist,
        debug=cfg.debug,
        state_store=state,
        outbound_handler=outbound,
    )
    httpd = server.start()
    http_thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    http_thread.start()

    # Watcher loop
    watcher = Watcher(cfg=cfg, state=state)
    watch_thread = threading.Thread(target=watcher.loop, args=(stop_evt,), daemon=True)
    watch_thread.start()

    def _sig(signum, frame):
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
