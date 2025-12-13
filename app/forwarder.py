import base64
import mimetypes
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Dict, Any, List, Tuple

from .http_client import http_post_json
from .logutil import log


def guess_mime(path: str) -> str:
    mt = mimetypes.guess_type(path)[0]
    return mt or "application/octet-stream"


def split_first_word(s: str) -> Tuple[str, str]:
    s = (s or "").strip()
    if not s:
        return "", ""
    parts = s.split(None, 1)
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], parts[1]


def is_success_or_idem(status: Optional[int]) -> bool:
    if status is None:
        return False
    s = int(status)
    return 200 <= s < 300 or s == 409


def is_permanent(status: Optional[int]) -> bool:
    if status is None:
        return False
    return int(status) in (401, 403, 404, 413, 415)


def is_retryable(status: Optional[int]) -> bool:
    if status is None:
        return True
    s = int(status)
    return s == 429 or (500 <= s < 600)


def forward_media(
    *,
    msisdn: str,
    msg_id: int,
    attach_id: str,
    path: str,
    caption: str,
    targets: List[Dict[str, Any]],
    debug: bool,
    forward_mode: str,
    caption_targeting: bool,
    target_word_strip: bool,
    http_timeout_sec: int,
    retry_backoffs: List[int],
    delete_on_success: bool,
    delete_delay_sec: int,
    deactivate_cb,  # (msisdn, name)->None
):
    if not targets:
        return

    out_caption = caption or ""
    chosen = targets
    if caption_targeting and out_caption:
        first, rest = split_first_word(out_caption)
        if first:
            cand = [t for t in targets if (t.get("name", "").lower() == first.lower())]
            if cand:
                chosen = cand
                if target_word_strip:
                    out_caption = rest

    filename = os.path.basename(path)
    mimetype = guess_mime(path)
    idem_key = f"msg:{msg_id}:att:{attach_id}"

    if forward_mode == "file_url":
        body = {"filename": filename, "mimetype": mimetype, "url": f"file://{path}", "caption": out_caption}
    else:
        with open(path, "rb") as f:
            raw = f.read()
        body = {"filename": filename, "mimetype": mimetype, "data_b64": base64.b64encode(raw).decode("ascii"), "caption": out_caption}

    # Phase 1
    results = {}
    def attempt0(tgt):
        url = tgt.get("webhook_url", "")
        tok = tgt.get("bearer_token", "")
        status, resp = http_post_json(url, body, bearer=tok, idem_key=idem_key, timeout=http_timeout_sec)
        return tgt, status, resp

    with ThreadPoolExecutor(max_workers=max(1, min(8, len(chosen)))) as pool:
        futs = [pool.submit(attempt0, tgt) for tgt in chosen]
        for fut in as_completed(futs):
            tgt, status, resp = fut.result()
            name = tgt.get("name")
            url = tgt.get("webhook_url", "")
            if status is None:
                log(f"POST error (no status) → {url} attempt=0 err={resp.decode(errors='ignore')}", level="DEBUG", debug_enabled=debug)
            elif is_success_or_idem(status):
                log(f"POST {status} → {url} name={name} msisdn={msisdn} id={msg_id}", level="INFO", debug_enabled=debug)
            elif int(status) in (401, 403):
                log(f"POST {status} (auth) → deactivate {name} msisdn={msisdn}", level="INFO", debug_enabled=debug)
                deactivate_cb(msisdn, name)
            results[name] = (status, resp)

    # Phase 2 retries
    all_ok = True
    for tgt in chosen:
        name = tgt.get("name")
        url = tgt.get("webhook_url", "")
        tok = tgt.get("bearer_token", "")
        status, _ = results.get(name, (None, b""))

        if is_success_or_idem(status):
            continue
        if is_permanent(status):
            all_ok = False
            continue
        if not is_retryable(status):
            all_ok = False
            continue

        ok = False
        for attempt, backoff in enumerate(retry_backoffs, start=1):
            time.sleep(backoff)
            s, r = http_post_json(url, body, bearer=tok, idem_key=idem_key, timeout=http_timeout_sec)
            if s is None:
                log(f"POST error (no status) → {url} attempt={attempt} err={r.decode(errors='ignore')}", level="DEBUG", debug_enabled=debug)
                continue
            if is_success_or_idem(s):
                log(f"POST {s} → {url} name={name} msisdn={msisdn} id={msg_id}", level="INFO", debug_enabled=debug)
                ok = True
                break
            if is_permanent(s):
                if int(s) in (401, 403):
                    deactivate_cb(msisdn, name)
                break
            if is_retryable(s):
                continue
            break

        if not ok:
            all_ok = False

    if all_ok and delete_on_success:
        try:
            time.sleep(delete_delay_sec)
            os.remove(path)
            log(f"Deleted media file {path}", level="INFO", debug_enabled=debug)
        except Exception as e:
            log(f"Delete failed {path}: {e}", level="DEBUG", debug_enabled=debug)
