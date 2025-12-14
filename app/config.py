# app/config.py
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import List


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, str(default)))
    except Exception:
        return default


def _env_csv(name: str, default: str) -> List[str]:
    raw = os.environ.get(name, default)
    return [x.strip() for x in raw.split(",") if x.strip()]


@dataclass
class Config:
    # General
    debug: bool = field(default_factory=lambda: _env_bool("DEBUG", True))

    # Garmin data paths
    db_path: str = field(
        default_factory=lambda: _env(
            "DB_PATH",
            "/android-data/data/com.garmin.android.apps.messenger/databases/message.db",
        )
    )
    root_dir: str = field(
        default_factory=lambda: _env(
            "ROOT_DIR",
            "/android-data/data/com.garmin.android.apps.messenger/no_backup",
        )
    )

    # State store
    state_dir: str = field(default_factory=lambda: _env("STATE_DIR", "./state"))
    subs_db: str = field(default_factory=lambda: _env("SUBS_DB", ""))

    # Provision / outbound control API (Maubot -> bridge)
    bind: str = field(default_factory=lambda: _env("PROVISION_BIND", "0.0.0.0"))
    port: int = field(default_factory=lambda: _env_int("PROVISION_PORT", 8788))
    provision_secret: str = field(default_factory=lambda: _env("PROVISION_SECRET", ""))
    hmac_secret: str = field(default_factory=lambda: _env("HMAC_SECRET", ""))
    allowlist: List[str] = field(default_factory=lambda: _env_csv("PROVISION_ALLOWLIST", ""))

    # ADB
    adb_path: str = field(default_factory=lambda: _env("ADB_PATH", "adb"))
    adb_target: str = field(default_factory=lambda: _env("ADB_TARGET", "redroid13:5555"))
    adb_connect_retries: int = field(default_factory=lambda: _env_int("ADB_CONNECT_RETRIES", 30))
    adb_connect_backoff_sec: float = field(default_factory=lambda: _env_float("ADB_CONNECT_BACKOFF_SEC", 1.0))
    adb_cmd_timeout_sec: float = field(default_factory=lambda: _env_float("ADB_CMD_TIMEOUT_SEC", 15.0))
    adb_keepalive_sec: int = field(default_factory=lambda: _env_int("ADB_KEEPALIVE_SEC", 10))

    # Watcher / forwarding
    last_n_boot: int = field(default_factory=lambda: _env_int("LAST_N_BOOT", 0))
    poll_db_sec: int = field(default_factory=lambda: _env_int("POLL_DB_SEC", 2))
    tail_limit: int = field(default_factory=lambda: _env_int("TAIL_LIMIT", 200))
    media_exts: List[str] = field(default_factory=lambda: _env_csv("MEDIA_EXTS", "jpg,jpeg,png,mp4,mp3,amr,aac,webp"))
    forward_mode: str = field(default_factory=lambda: _env("FORWARD_MODE", "file_url"))
    caption_targeting: bool = field(default_factory=lambda: _env_bool("CAPTION_TARGETING", True))
    target_word_strip: bool = field(default_factory=lambda: _env_bool("TARGET_WORD_STRIP", True))
    http_timeout_sec: int = field(default_factory=lambda: _env_int("HTTP_TIMEOUT_SEC", 10))
    retry_backoffs: List[int] = field(default_factory=lambda: [
        b for b in (_env_int("RETRY_BACKOFF_1", 3), _env_int("RETRY_BACKOFF_2", 5)) if b > 0
    ])
    delete_on_success: bool = field(default_factory=lambda: _env_bool("DELETE_ON_SUCCESS", False))
    delete_delay_sec: int = field(default_factory=lambda: _env_int("DELETE_DELAY_SEC", 30))

    def __post_init__(self) -> None:
        # Default to state_dir/statestore.db if not provided explicitly
        if not self.subs_db:
            self.subs_db = os.path.join(self.state_dir, "statestore.db")

