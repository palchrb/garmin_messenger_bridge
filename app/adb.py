# app/adb.py
from __future__ import annotations

import subprocess
import time
from dataclasses import dataclass
from typing import Optional, Sequence, Union

from .config import Config
from .logutil import get_logger

log = get_logger(__name__)


class AdbError(RuntimeError):
    pass


@dataclass(frozen=True)
class ShellResult:
    returncode: int
    stdout: str
    stderr: str


def _run(cmd: Sequence[str], timeout: float) -> subprocess.CompletedProcess:
    return subprocess.run(
        list(cmd),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
        timeout=timeout,
    )


class AdbClient:
    """
    Minimal ADB client for Redroid/Garmin UI automation.

    This class is shaped specifically to match your GarminUI and ui.py usage:
      - ensure_connected()
      - shell(cmd, check=True/False) -> ShellResult
      - am_start_view(...)
      - input_keyevent / input_tap / input_text
      - sleep_ms
    """

    def __init__(
        self,
        adb_path: Optional[str] = None,
        target: Optional[str] = None,
        cmd_timeout_sec: Optional[float] = None,
    ) -> None:
        cfg = Config()
        self.adb_path = adb_path or cfg.adb_path
        self.target = target or cfg.adb_target
        self.cmd_timeout_sec = float(cmd_timeout_sec or cfg.adb_cmd_timeout_sec)

    def ensure_connected(self) -> None:
        """
        Ensure `adb connect <target>` succeeded.
        Retries with backoff to survive restarts of redroid container.
        """
        last: str = ""
        cfg = Config()
        retries = max(1, int(cfg.adb_connect_retries))
        backoff = float(cfg.adb_connect_backoff_sec)

        for i in range(retries):
            p = _run([self.adb_path, "connect", self.target], timeout=self.cmd_timeout_sec)
            out = (p.stdout or "").strip()
            err = (p.stderr or "").strip()
            combined = (out + "\n" + err).strip()

            # Many adb builds print "already connected" / "connected to ..."
            low = combined.lower()
            if p.returncode == 0 and ("connected" in low or "already connected" in low):
                if i > 0:
                    log.info("adb connected to %s after %d retries", self.target, i)
                return

            # Some adb versions return 0 even though they print a warning;
            # accept as long as it doesn't look like a failure.
            if p.returncode == 0 and not ("failed" in low or "unable" in low):
                return

            last = combined or f"rc={p.returncode}"
            log.warning("adb connect attempt %d/%d failed: %s", i + 1, retries, last)
            time.sleep(backoff)

        raise AdbError(f"ADB connect failed to {self.target}: {last}")

    def shell(self, command: str, check: bool = False, timeout_sec: Optional[float] = None) -> ShellResult:
        """
        Run `adb shell <command>` and return stdout/stderr/returncode.
        """
        timeout = float(timeout_sec or self.cmd_timeout_sec)
        p = _run([self.adb_path, "shell", command], timeout=timeout)
        res = ShellResult(p.returncode, p.stdout or "", p.stderr or "")
        if check and res.returncode != 0:
            raise AdbError(f"adb shell failed rc={res.returncode}: {command}\n{res.stderr.strip()}")
        return res

    def am_start_view(
        self,
        data_uri: str,
        package: str,
        activity: str,
        flags_hex: str = "0x10000000",
    ) -> None:
        """
        Equivalent to:
          adb shell am start -a android.intent.action.VIEW -d "<uri>" -n <pkg>/<act> -f <flags>
        """
        cmd = (
            "am start -a android.intent.action.VIEW "
            f'-d "{data_uri}" '
            f"-n {package}/{activity} "
            f"-f {flags_hex}"
        )
        self.shell(cmd, check=True)

    def input_keyevent(self, keycode: int) -> None:
        self.shell(f"input keyevent {int(keycode)}", check=True)

    def input_tap(self, x: int, y: int) -> None:
        self.shell(f"input tap {int(x)} {int(y)}", check=True)

    def input_text(self, encoded: str) -> None:
        # `encoded` should already be prepared for adb input text
        self.shell(f'input text "{encoded}"', check=True)

    def sleep_ms(self, ms: Union[int, float]) -> None:
        time.sleep(float(ms) / 1000.0)
