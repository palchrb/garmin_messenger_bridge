import os
from dataclasses import dataclass
from typing import List, Optional


def env(name: str, default=None, cast=str):
    v = os.environ.get(name, default)
    if cast is int:
        try:
            return int(v)
        except Exception:
            return int(default) if default is not None else 0
    if cast is float:
        try:
            return float(v)
        except Exception:
            return float(default) if default is not None else 0.0
    return v


def csv_list(name: str, default: str = "") -> List[str]:
    raw = env(name, default, str) or ""
    return [x.strip() for x in raw.split(",") if x.strip()]


@dataclass(frozen=True)
class Config:
    # Garmin DB + media paths (inside container)
    db_path: str = env("DB_PATH", "")
    root_dir: str = env("ROOT_DIR", "")
    root_no_backup: str = env("ROOT_NO_BACKUP", "")

    # Polling
    poll_db_sec: int = env("POLL_DB_SEC", 1, int)
    tail_limit: int = env("TAIL_LIMIT", 200, int)
    last_n_boot: int = env("LAST_N_BOOT", 5, int)

    # Behavior
    debug: bool = env("DEBUG", "1") == "1"
    forward_mode: str = env("FORWARD_MODE", "base64")
    delete_on_success: bool = env("DELETE_ON_SUCCESS", "1") == "1"
    delete_delay_sec: int = env("DELETE_DELAY_SEC", 2, int)
    caption_targeting: bool = env("CAPTION_TARGETING", "1") == "1"
    target_word_strip: bool = env("TARGET_WORD_STRIP", "1") == "1"

    # Provision server
    bind: str = env("PROVISION_BIND", "0.0.0.0")
    port: int = env("PROVISION_PORT", 8788, int)
    provision_secret: str = env("PROVISION_SECRET", "")

    # Optional stronger auth: HMAC signature of body
    hmac_secret: str = env("PROVISION_HMAC_SECRET", "")

    # Optional allowlist (client IPs)
    allowlist: List[str] = csv_list("PROVISION_ALLOWLIST", "")

    # State
    state_dir: str = env("STATE_DIR", "/var/lib/garmin-bridge")
    subs_json: str = env("SUBS_JSON", "")
    seen_file: str = env("SEEN_FILE", "")

    # Media
    media_exts: List[str] = csv_list("MEDIA_EXTS", "avif,jpg,jpeg,png,ogg,oga,mp4,m4a")

    # HTTP client
    http_timeout_sec: int = env("HTTP_TIMEOUT_SEC", 15, int)
    retry_backoffs: List[int] = [int(x) for x in csv_list("RETRY_BACKOFFS", "1,4,10")]

    # ADB keepalive
    adb_target: str = env("ADB_TARGET", "redroid13:5555")
    adb_keepalive_sec: int = env("ADB_KEEPALIVE_SEC", 5, int)

    def resolved_subs_json(self) -> str:
        if self.subs_json:
            return self.subs_json
        return os.path.join(self.state_dir, "subs.json")

    def resolved_seen_file(self) -> str:
        if self.seen_file:
            return self.seen_file
        return os.path.join(self.state_dir, "seen.txt")
