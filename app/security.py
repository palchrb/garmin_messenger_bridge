import hashlib
import hmac
from typing import Optional


def bearer_ok(auth_header: str, expected: str) -> bool:
    if not expected:
        return False
    if not auth_header:
        return False
    parts = auth_header.split(" ", 1)
    if len(parts) != 2:
        return False
    scheme, token = parts[0].lower(), parts[1].strip()
    return scheme == "bearer" and token == expected


def hmac_ok(signature_header: str, body: bytes, secret: str) -> bool:
    """
    Optional second factor:
      X-Signature: sha256=<hex>
    where <hex> = HMAC-SHA256(secret, body)
    """
    if not secret:
        return True  # disabled
    if not signature_header:
        return False
    sig = signature_header.strip()
    if sig.startswith("sha256="):
        sig = sig[len("sha256="):]
    mac = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(sig, mac)
