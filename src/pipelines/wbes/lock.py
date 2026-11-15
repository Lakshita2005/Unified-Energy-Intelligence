"""PostgreSQL advisory lock: one process per WBES utility session.

WBES rotates the refresh token on every refresh, so two processes using the
same login would invalidate each other. Every WBES command (ingestion and the
interactive login) holds a session-level advisory lock, keyed on the utility
id, on a dedicated connection for its whole duration. A second process fails
fast with :class:`WbesSessionLocked` instead of waiting.

The lock is in the PI database, so it also protects against two machines
sharing one WBES user, provided both use this database.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.pipelines.wbes.errors import WbesSessionLocked

LOCK_NAMESPACE = "pi.wbes.session"


def advisory_key(utility_id: str) -> int:
    """Stable signed 64-bit key for ``pg_advisory_lock`` derived from the utility id."""
    digest = hashlib.sha256(f"{LOCK_NAMESPACE}:{utility_id.strip().upper()}".encode()).digest()
    return int.from_bytes(digest[:8], "big", signed=True)


@contextmanager
def wbes_session_lock(engine: Engine, utility_id: str) -> Iterator[int]:
    """Hold the WBES session lock for ``utility_id`` while the block runs.

    Raises:
        WbesSessionLocked: Another session (process or machine) holds the lock.
    """
    key = advisory_key(utility_id)
    connection = engine.connect()
    try:
        acquired = connection.execute(text("SELECT pg_try_advisory_lock(:k)"), {"k": key}).scalar_one()
        connection.commit()   # session-level lock: keep it, but do not leave a transaction open
        if not acquired:
            raise WbesSessionLocked("another process is using this WBES session (advisory lock held); "
                                    "only one WBES process may run per utility at a time")
        try:
            yield key
        finally:
            connection.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": key})
            connection.commit()
    finally:
        connection.close()
