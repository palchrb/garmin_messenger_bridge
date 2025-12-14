import logging
import time
from typing import Any

from .config import Config


def ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def get_logger(name: str = "app") -> logging.Logger:
    """Return a module-level logger with sensible defaults.

    Falls back to INFO unless DEBUG=1/true is set, mirrors the minimal stdout
    logging used elsewhere in the bridge, and avoids duplicate handlers on
    repeated imports.
    """

    cfg = Config()
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
        )
        logger.addHandler(handler)

    logger.setLevel(logging.DEBUG if cfg.debug else logging.INFO)
    logger.propagate = False
    return logger


def log(*a: Any, level: str = "INFO", debug_enabled: bool = True) -> None:
    if level == "DEBUG" and not debug_enabled:
        return
    print(f"{ts()} [{level}] " + " ".join(str(x) for x in a), flush=True)
