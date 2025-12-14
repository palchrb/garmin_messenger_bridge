import json
import json
import urllib.error
import urllib.request
from typing import Optional, Tuple


def http_post_json(url: str, data_dict: dict, bearer: Optional[str], idem_key: Optional[str], timeout: int) -> Tuple[Optional[int], bytes]:
    body = json.dumps(data_dict).encode("utf-8")
    req = urllib.request.Request(url=url, data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    if bearer:
        req.add_header("Authorization", f"Bearer {bearer}")
    if idem_key:
        req.add_header("Idempotency-Key", idem_key)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.getcode(), resp.read()
    except urllib.error.HTTPError as e:
        try:
            payload = e.read()
        except Exception:
            payload = b""
        return e.code, payload
    except Exception as e:
        return None, str(e).encode("utf-8", errors="ignore")
