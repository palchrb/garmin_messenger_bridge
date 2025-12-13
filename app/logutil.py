import time
from typing import Any


def ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def log(*a: Any, level: str = "INFO", debug_enabled: bool = True) -> None:
    if level == "DEBUG" and not debug_enabled:
        return
    print(f"{ts()} [{level}] " + " ".join(str(x) for x in a), flush=True)
