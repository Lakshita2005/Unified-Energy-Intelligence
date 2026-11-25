"""Time source for the scheduler and worker (injectable, so tests can use a fake clock)."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Protocol
from zoneinfo import ZoneInfo


class Clock(Protocol):
    def now(self) -> datetime:
        """The current instant, timezone-aware (UTC)."""

    def sleep(self, seconds: float) -> None:
        """Wait (a fake clock advances instead)."""


class SystemClock:
    def __init__(self) -> None:
        import threading

        self._event = threading.Event()

    def now(self) -> datetime:
        return datetime.now(timezone.utc)

    def sleep(self, seconds: float) -> None:
        # Interruptible by wake(); used so a stop request does not wait a full tick.
        self._event.wait(max(0.0, seconds))
        self._event.clear()

    def wake(self) -> None:
        self._event.set()


class FakeClock:
    """A controllable clock for tests."""

    def __init__(self, start: datetime) -> None:
        if start.tzinfo is None:
            raise ValueError("FakeClock needs a timezone-aware start")
        self._now = start.astimezone(timezone.utc)
        self.sleeps: list[float] = []

    def now(self) -> datetime:
        return self._now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self._now += timedelta(seconds=seconds)

    def advance(self, **kwargs: float) -> datetime:
        self._now += timedelta(**kwargs)
        return self._now

    def set(self, moment: datetime) -> None:
        self._now = moment.astimezone(timezone.utc)

    def wake(self) -> None:
        pass


def local_now(clock: Clock, tz: ZoneInfo) -> datetime:
    return clock.now().astimezone(tz)


def local_today(clock: Clock, tz: ZoneInfo) -> date:
    return local_now(clock, tz).date()
