"""Advisory locks: one scheduler instance, and one running job per source group.

Session-level PostgreSQL advisory locks (``pg_try_advisory_lock``) on a dedicated connection, held for as
long as the scheduler runs or a job executes (same pattern as :mod:`src.pipelines.wbes.lock`). They are
released automatically if the process dies (PostgreSQL drops the session), so a crash never leaves a
stale lock behind. They work across processes and machines that share the database. ``pi_app`` may use
them without any extra privilege.
"""

from __future__ import annotations

import hashlib
import threading
from typing import Protocol

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

LOCK_NAMESPACE = "pi.jobs"
SCHEDULER_LOCK = "scheduler"


def advisory_key(name: str) -> int:
    """Stable signed 64-bit key for ``pg_advisory_lock`` derived from a lock name."""
    digest = hashlib.sha256(f"{LOCK_NAMESPACE}:{name.strip().upper()}".encode()).digest()
    return int.from_bytes(digest[:8], "big", signed=True)


class LockHandle(Protocol):
    name: str

    def release(self) -> None: ...

    def is_held(self) -> bool:
        """True while this process still owns the lock (False after a lost database session)."""


class LockFactory(Protocol):
    def try_acquire(self, name: str) -> LockHandle | None:
        """The lock, or None if another session holds it (never waits)."""


class AdvisoryLock:
    def __init__(self, name: str, connection: Connection, key: int) -> None:
        self.name = name
        self._connection = connection
        self._key = key
        self._released = False

    def is_held(self) -> bool:
        """Ask PostgreSQL, on the lock's own session, whether this session still holds the lock.

        Any error (connection dropped, server restarted) means the session - and with it the lock - is gone.
        A 64-bit advisory key appears in ``pg_locks`` as classid (high 32 bits) / objid (low 32 bits),
        objsubid 1.
        """
        if self._released:
            return False
        unsigned = self._key & 0xFFFFFFFFFFFFFFFF
        try:
            held = self._connection.execute(text(
                "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND pid = pg_backend_pid() "
                "AND granted AND classid = CAST(:hi AS oid) AND objid = CAST(:lo AS oid) AND objsubid = 1)"),
                {"hi": unsigned >> 32, "lo": unsigned & 0xFFFFFFFF}).scalar_one()
            self._connection.commit()
            return bool(held)
        except Exception:
            return False

    def release(self) -> None:
        if self._released:
            return
        self._released = True
        try:
            self._connection.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": self._key})
            self._connection.commit()
        except Exception:   # the session is already gone: so is the lock
            pass
        finally:
            try:
                self._connection.close()
            except Exception:
                pass

    def __enter__(self) -> AdvisoryLock:
        return self

    def __exit__(self, *exc: object) -> None:
        self.release()


class AdvisoryLockFactory:
    """Session-level advisory locks on dedicated connections of ``engine``."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def try_acquire(self, name: str) -> AdvisoryLock | None:
        key = advisory_key(name)
        connection = self.engine.connect()
        try:
            acquired = connection.execute(text("SELECT pg_try_advisory_lock(:k)"), {"k": key}).scalar_one()
            connection.commit()   # keep the session lock, but no open transaction
        except Exception:
            connection.close()
            raise
        if not acquired:
            connection.close()
            return None
        return AdvisoryLock(name, connection, key)


class InProcessLockFactory:
    """Locks within one process only (tests, and dry runs without a database)."""

    def __init__(self) -> None:
        self._held: set[str] = set()
        self._mutex = threading.Lock()

    def try_acquire(self, name: str) -> LockHandle | None:
        with self._mutex:
            if name in self._held:
                return None
            self._held.add(name)
        factory = self

        class _Handle:
            def __init__(self) -> None:
                self.name = name

            def release(self) -> None:
                with factory._mutex:
                    factory._held.discard(name)

            def is_held(self) -> bool:
                with factory._mutex:
                    return name in factory._held

        return _Handle()

    @property
    def held(self) -> set[str]:
        with self._mutex:
            return set(self._held)

    def lose(self, name: str) -> None:
        """Simulate a lost lock (tests): the holder's is_held() becomes False."""
        with self._mutex:
            self._held.discard(name)
