import json
import os
import threading
import time
from typing import Dict, Any, Optional, List


class StateStore:
    def __init__(self, subs_path: str, seen_path: str):
        self.subs_path = subs_path
        self.seen_path = seen_path
        self._sub_lock = threading.Lock()
        self._seen_lock = threading.Lock()
        self._seen = set()
        os.makedirs(os.path.dirname(self.subs_path), exist_ok=True)
        os.makedirs(os.path.dirname(self.seen_path), exist_ok=True)
        self._load_seen()

    # ---------------- subs ----------------
    def _load_subs(self) -> Dict[str, Any]:
        if not os.path.isfile(self.subs_path):
            return {}
        try:
            with open(self.subs_path, "r", encoding="utf-8") as f:
                return json.load(f) or {}
        except Exception:
            return {}

    def _save_subs(self, d: Dict[str, Any]) -> None:
        tmp = self.subs_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(d, f, ensure_ascii=False, indent=2)
        os.replace(tmp, self.subs_path)

    def subs_get(self, msisdn: str) -> Dict[str, Any]:
        with self._sub_lock:
            subs = self._load_subs()
            return subs.get(msisdn) or {}

    def subs_set(self, msisdn: str, name: str, status: str, verify_code: str, url: str, token: str) -> None:
        now = int(time.time())
        nkey = (name or "").lower()
        with self._sub_lock:
            subs = self._load_subs()
            ms = subs.get(msisdn) or {}
            row = ms.get(nkey) or {"name": name, "created_ts": now}
            row.update(
                {
                    "name": name,
                    "status": status,
                    "verify_code": str(verify_code or ""),
                    "webhook_url": url,
                    "bearer_token": token,
                    "updated_ts": now,
                }
            )
            ms[nkey] = row
            subs[msisdn] = ms
            self._save_subs(subs)

    def subs_check_name_exists(self, msisdn: str, name: str) -> bool:
        return (name or "").lower() in self.subs_get(msisdn)

    def subs_activate_if_code(self, msisdn: str, name: str, code: str) -> bool:
        nkey = (name or "").lower()
        with self._sub_lock:
            subs = self._load_subs()
            ms = subs.get(msisdn) or {}
            row = ms.get(nkey)
            if not row:
                return False
            if str(row.get("verify_code", "")) != str(code):
                return False
            row["status"] = "active"
            row["updated_ts"] = int(time.time())
            ms[nkey] = row
            subs[msisdn] = ms
            self._save_subs(subs)
        return True

    def subs_deactivate(self, msisdn: str, name: Optional[str] = None) -> None:
        with self._sub_lock:
            subs = self._load_subs()
            ms = subs.get(msisdn) or {}
            changed = False
            if name:
                nkey = name.lower()
                if nkey in ms:
                    ms[nkey]["status"] = "inactive"
                    ms[nkey]["updated_ts"] = int(time.time())
                    changed = True
            else:
                for k in list(ms.keys()):
                    ms[k]["status"] = "inactive"
                    ms[k]["updated_ts"] = int(time.time())
                    changed = True
            if changed:
                subs[msisdn] = ms
                self._save_subs(subs)

    def subs_rotate_token(self, msisdn: str, name: str, bearer_token: str, webhook_url: Optional[str] = None) -> bool:
        now = int(time.time())
        nkey = (name or "").lower()
        if not msisdn or not nkey or not bearer_token:
            return False
        with self._sub_lock:
            subs = self._load_subs()
            ms = subs.get(msisdn) or {}
            row = ms.get(nkey)
            if not row:
                return False
            row["bearer_token"] = bearer_token
            if webhook_url:
                row["webhook_url"] = webhook_url
            row["updated_ts"] = now
            ms[nkey] = row
            subs[msisdn] = ms
            self._save_subs(subs)
        return True

    def active_targets(self, msisdn: str) -> List[Dict[str, Any]]:
        ms = self.subs_get(msisdn)
        return [v for v in ms.values() if v.get("status") == "active"]

    # ---------------- seen ----------------
    def _load_seen(self) -> None:
        if not os.path.isfile(self.seen_path):
            return
        try:
            with open(self.seen_path, "r", encoding="utf-8") as f:
                self._seen = set(x.strip() for x in f if x.strip())
        except Exception:
            self._seen = set()

    def is_seen(self, key: str) -> bool:
        with self._seen_lock:
            return key in self._seen

    def add_seen(self, key: str) -> None:
        with self._seen_lock:
            if key in self._seen:
                return
            self._seen.add(key)
            with open(self.seen_path, "a", encoding="utf-8") as f:
                f.write(key + "\n")
            # trim if huge
            try:
                if os.path.getsize(self.seen_path) > 1024 * 1024:
                    with open(self.seen_path, "r", encoding="utf-8") as f2:
                        lines = list(f2)[-5000:]
                    with open(self.seen_path, "w", encoding="utf-8") as f3:
                        f3.writelines(lines)
            except Exception:
                pass
