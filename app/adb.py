import subprocess
import time
from dataclasses import dataclass
from typing import Tuple, Optional

from .logutil import log


@dataclass
class ADBManager:
    adb_target: str
    keepalive_sec: int
    debug: bool

    def _run(self, args, timeout=10) -> Tuple[bool, str]:
        try:
            p = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=timeout, check=False, text=True)
            out = (p.stdout or "").strip()
            return p.returncode == 0, out
        except Exception as e:
            return False, str(e)

    def connect(self) -> bool:
        ok, out = self._run(["adb", "connect", self.adb_target], timeout=15)
        if ok:
            log(f"ADB connect OK: {self.adb_target} ({out})", level="INFO", debug_enabled=self.debug)
        else:
            log(f"ADB connect FAIL: {self.adb_target} ({out})", level="DEBUG", debug_enabled=self.debug)
        return ok

    def is_connected(self) -> bool:
        ok, out = self._run(["adb", "devices"], timeout=10)
        if not ok:
            return False
        # crude: target shows as "<ip:port>\tdevice"
        for line in out.splitlines():
            if self.adb_target in line and "\tdevice" in line:
                return True
        return False

    def keepalive_loop(self, stop_evt):
        """
        Keeps ADB connected. Also useful after ReDroid restarts.
        """
        while not stop_evt.is_set():
            if not self.is_connected():
                self.connect()
            stop_evt.wait(self.keepalive_sec)
