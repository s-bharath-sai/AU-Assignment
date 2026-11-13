"""
PII protection at rest.

- Phone lookup key: HMAC-SHA256(secret, canonical phone). Deterministic, so the
  same phone always gives the same hash -> we can join events to leads without
  storing the number. A plain SHA-256 would be brute-forceable (only ~4 billion
  Indian mobiles); the HMAC key prevents that.
- Recoverable copy: every PII field (incl. the full phone) is encrypted with
  Fernet (AES-128-CBC + HMAC) so ops could still read it with the key.
- Reports only ever need the last 4 digits, stored separately in clear.

The secret comes from AU_PII_SECRET. The dev default is for local runs only;
changing the secret after ingesting makes old hashes unmatchable.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import os

from cryptography.fernet import Fernet

DEV_SECRET = "dev-only-not-a-real-secret"


def _derive(secret: str, purpose: str) -> bytes:
    # Separate keys for hashing and encryption, derived from one secret.
    return hashlib.sha256(f"{purpose}:{secret}".encode()).digest()


class PiiVault:
    def __init__(self, secret: str):
        self._hmac_key = _derive(secret, "hmac")
        self._fernet = Fernet(base64.urlsafe_b64encode(_derive(secret, "fernet")))

    @classmethod
    def from_env(cls) -> "PiiVault":
        return cls(os.environ.get("AU_PII_SECRET", DEV_SECRET))

    def phone_hash(self, canonical_phone: str) -> str:
        return hmac.new(self._hmac_key, canonical_phone.encode(), hashlib.sha256).hexdigest()

    def encrypt(self, value: str) -> str:
        return self._fernet.encrypt(value.encode()).decode()

    def decrypt(self, token: str) -> str:
        return self._fernet.decrypt(token.encode()).decode()
