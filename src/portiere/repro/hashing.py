"""sha256 hashing helpers for the reproducibility manifest.

Cheap to call, deterministic across calls. Hashes file content for
small files; large files (>1 GB) fall back to a "metadata fingerprint"
(name + size + mtime) — full-byte hashing of multi-GB Athena exports
on every pipeline run is an unacceptable tax for marginal identity gain.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

LARGE_FILE_THRESHOLD = 1_000_000_000  # 1 GB


def sha256_bytes(b: bytes) -> str:
    """Return hex sha256 of a bytes value."""
    return hashlib.sha256(b).hexdigest()


def sha256_text(s: str) -> str:
    """Return hex sha256 of a UTF-8-encoded string."""
    return sha256_bytes(s.encode("utf-8"))


def sha256_file(path: str | Path, *, chunk_size: int = 1 << 20) -> str:
    """Return hex sha256 of a file's full content. Streams in 1 MB chunks."""
    p = Path(path)
    h = hashlib.sha256()
    with p.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def sha256_file_or_metadata(path: str | Path) -> str:
    """Return content sha for files <1 GB, else a metadata fingerprint.

    Metadata fingerprint is prefixed ``meta:`` to make the choice visible
    in the manifest. The metadata is name + size + mtime_ns hashed
    together — distinct enough that any practical file mutation flips
    the hash, while avoiding gigabyte streams during routine runs.
    """
    p = Path(path)
    if p.stat().st_size > LARGE_FILE_THRESHOLD:
        meta = f"{p.name}|{p.stat().st_size}|{p.stat().st_mtime_ns}".encode()
        return f"meta:{sha256_bytes(meta)}"
    return sha256_file(p)
