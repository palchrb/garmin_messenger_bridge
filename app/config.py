# app/config.py
from __future__ import annotations

import os
from dataclasses import dataclass


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


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


@dataclass(frozen=True)
class Config:
    # General
    DEBUG: bool = _env("DEBUG", "1") == "1"

    # Provision / outbound control API (Maubot -> bridge)
    PROVISION_BIND: str = _env("PROVISION_BIND", "0.0.0.0")
    PROVISION_PORT: int = _env_int("PROVISION_PORT", 8788)
    PROVISION_SECRET: str = _env("PROVISION_SECRET", "")

    # Optional: allowlist for callers (comma-separated IPs)
    PROVISION_ALLOWLIST: str = _env("PROVISION_ALLOWLIST", "")

    # ADB
    ADB_PATH: str = _env("ADB_PATH", "adb")
    # IMPORTANT in docker network: use service name, NOT localhost.
    ADB_TARGET: str = _env("ADB_TARGET", "redroid13:5555")
    ADB_CONNECT_RETRIES: int = _env_int("ADB_CONNECT_RETRIES", 30)
    ADB_CONNECT_BACKOFF_SEC: float = _env_float("ADB_CONNECT_BACKOFF_SEC", 1.0)
    ADB_CMD_TIMEOUT_SEC: float = _env_float("ADB_CMD_TIMEOUT_SEC", 15.0)


CFG = Config()
