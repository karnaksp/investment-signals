"""Stable content identities for versioned configuration."""

from __future__ import annotations

from hashlib import sha256
from typing import Iterable


def content_version(chunks: Iterable[bytes]) -> str:
    digest = sha256()
    for chunk in chunks:
        digest.update(len(chunk).to_bytes(8, "big"))
        digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"
