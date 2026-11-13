"""SHA-256 checksum helpers.

Checksums identify raw documents: ``pi.source_document`` is unique on
``(dataset_id, checksum_sha256)``, so the same content is never loaded twice.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

_CHUNK_SIZE = 1024 * 1024


def sha256_bytes(data: bytes) -> str:
    """Return the lower-case hex SHA-256 digest (64 characters) of ``data``."""
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path | str) -> str:
    """Return the SHA-256 digest of a file, reading it in chunks."""
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(_CHUNK_SIZE), b""):
            digest.update(chunk)
    return digest.hexdigest()
