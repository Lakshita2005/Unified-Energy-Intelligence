"""Availability-checker framework.

A checker answers one question for its schedule: *what is available at the source that is not stored
yet (or has been revised)?* It compares a light probe of the source (a listing, a snapshot, a single
file) with what PostgreSQL already holds (:class:`SourceState`, read-only), and returns
:class:`~src.scheduler.models.Detection` items. It never writes data and never ingests: the worker
does that by calling the existing runner with the detection's ``target``.

Checkers must not raise for "not published yet": they report it (``not_published`` / ``late``). Other
exceptions propagate and are recorded by the scheduler as ``SOURCE_ERROR`` with their classification.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from typing import Any, ClassVar
from zoneinfo import ZoneInfo

from config.settings import Settings
from src.common.http_client import HttpClient
from src.scheduler.config import ScheduleConfig
from src.scheduler.models import CheckResult, Watermark


@dataclass
class CheckContext:
    """Everything a checker may use."""

    schedule: ScheduleConfig
    now: datetime                                  # aware
    previous_check_at: datetime | None
    http: HttpClient
    settings: Settings
    state: Any                                     # SourceState (read-only database queries)
    watermark: Callable[[str], Watermark | None] = field(default=lambda _key: None)

    @property
    def tz(self) -> ZoneInfo:
        return self.schedule.tz

    @property
    def local_now(self) -> datetime:
        return self.now.astimezone(self.tz)

    @property
    def today(self) -> date:
        return self.local_now.date()

    @property
    def first_check_of_day(self) -> bool:
        """True for the first check of this local day (used for once-a-day revision checks)."""
        if self.previous_check_at is None:
            return True
        return self.previous_check_at.astimezone(self.tz).date() < self.today

    def catchup_days(self) -> int:
        return self.schedule.catchup_max_days if self.schedule.catchup_enabled else 1

    def local_moment(self, day: date, at: time = time(0, 0)) -> datetime:
        return datetime.combine(day, at, self.tz)

    def is_late(self, reference_day: date, hours: float | None) -> bool:
        """True once ``hours`` have passed since local midnight starting ``reference_day``."""
        if hours is None:
            return False
        return self.now >= self.local_moment(reference_day) + timedelta(hours=hours)


class AvailabilityChecker(ABC):
    """Base class; subclasses set ``name`` and implement :meth:`check`."""

    name: ClassVar[str]
    #: parameter name -> default; unknown parameters in the configuration are rejected
    defaults: ClassVar[dict[str, Any]] = {}

    def __init__(self, schedule: ScheduleConfig) -> None:
        unknown = set(schedule.params) - set(self.defaults)
        if unknown:
            raise ValueError(f"{schedule.key}: unknown parameter(s) {sorted(unknown)} for checker {self.name!r}; "
                             f"allowed: {sorted(self.defaults)}")
        self.schedule = schedule
        self.params = {**self.defaults, **schedule.params}
        self.validate()

    def validate(self) -> None:
        """Check parameters; raise ValueError."""

    @abstractmethod
    def check(self, ctx: CheckContext) -> CheckResult:
        """Probe the source and compare with stored state."""


def date_span(start: date, end: date) -> list[date]:
    return [start + timedelta(days=i) for i in range((end - start).days + 1)] if end >= start else []
