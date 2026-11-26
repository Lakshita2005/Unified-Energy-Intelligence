"""Scheduler configuration: ``config/scheduler.toml`` -> :class:`ScheduleConfig`.

The TOML file (reviewed in git) is the source of truth. ``python -m scripts.pi_jobs sync-config`` copies
it into ``pi_jobs.schedule`` so the database holds the configuration the running processes use and the
check/job history can be joined to it. Nothing here is hard-coded in pipeline code.

Format::

    [defaults]
    timezone = "Asia/Kolkata"
    max_attempts = 5
    backoff_base = "2m"          # first retry delay; doubles per attempt
    backoff_max = "30m"
    failure_cooldown = "6h"      # after retries are exhausted, wait this long before re-queuing the item
    catchup_enabled = true
    catchup_max_days = 7

    [[schedule]]
    key = "CEA_COAL_DAILY"
    checker = "cea_daily"
    enabled = false
    source = "CEA"
    datasets = ["CEA_COAL_STOCK_DAILY"]
    lock_group = "CEA"
    every = "1h"                                   # check interval outside the windows ("off" = none)
    windows = [{ start = "12:00", end = "24:00", every = "30m" }]
    [schedule.params]
    reports = ["coal"]

Durations: ``"45s"``, ``"30m"``, ``"6h"``, ``"1d"``. Window times are local (``timezone``); a window whose
end is before its start crosses midnight; ``"24:00"`` means end of day.
"""

from __future__ import annotations

import re
import tomllib
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from src.common.exceptions import ConfigurationError

_DURATION = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([smhd])\s*$", re.IGNORECASE)
_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}
_KEY = re.compile(r"^[A-Z][A-Z0-9_]{1,62}$")
_OFF = {"", "off", "none", "never"}


class SchedulerConfigError(ConfigurationError):
    """The scheduler configuration file is invalid."""


def parse_duration(value: Any, what: str) -> float | None:
    """Seconds for ``"30m"`` etc.; ``None`` for ``"off"``. Plain numbers are seconds."""
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if value <= 0:
            raise SchedulerConfigError(f"{what}: must be positive, got {value!r}")
        return float(value)
    if isinstance(value, str):
        if value.strip().lower() in _OFF:
            return None
        match = _DURATION.match(value)
        if match:
            seconds = float(match.group(1)) * _UNITS[match.group(2).lower()]
            if seconds <= 0:
                raise SchedulerConfigError(f"{what}: must be positive, got {value!r}")
            return seconds
    raise SchedulerConfigError(f"{what}: invalid duration {value!r} (use e.g. '45s', '30m', '6h', '1d' or 'off')")


def parse_clock_time(value: Any, what: str) -> time | None:
    """``"HH:MM"`` -> time; ``"24:00"`` -> None (end of day)."""
    if not isinstance(value, str) or not re.fullmatch(r"\d{1,2}:\d{2}", value.strip()):
        raise SchedulerConfigError(f"{what}: expected 'HH:MM', got {value!r}")
    hours, minutes = (int(part) for part in value.strip().split(":"))
    if (hours, minutes) == (24, 0):
        return None
    if not (0 <= hours < 24 and 0 <= minutes < 60):
        raise SchedulerConfigError(f"{what}: invalid time {value!r}")
    return time(hours, minutes)


@dataclass(frozen=True)
class Window:
    """A local-time window with its own check interval."""

    start: time
    end: time | None          # None = 24:00
    every_seconds: float

    def contains(self, moment: time) -> bool:
        end = self.end
        if end is None:
            return moment >= self.start
        if self.start <= end:
            return self.start <= moment < end
        return moment >= self.start or moment < end   # crosses midnight

    def describe(self) -> str:
        end = "24:00" if self.end is None else self.end.strftime("%H:%M")
        return f"{self.start:%H:%M}-{end} every {self.every_seconds:.0f}s"


@dataclass(frozen=True)
class ScheduleConfig:
    """One schedule: which checker runs, when, and how failures are handled."""

    key: str
    checker: str
    enabled: bool
    source: str
    datasets: tuple[str, ...]
    lock_group: str
    timezone: str
    every_seconds: float | None
    windows: tuple[Window, ...]
    max_attempts: int
    backoff_base_seconds: float
    backoff_max_seconds: float
    failure_cooldown_seconds: float
    catchup_enabled: bool
    catchup_max_days: int
    priority: int = 100
    description: str = ""
    params: dict[str, Any] = field(default_factory=dict)

    @property
    def tz(self) -> ZoneInfo:
        return ZoneInfo(self.timezone)

    def interval_at(self, moment: datetime) -> float | None:
        """Check interval at ``moment`` (aware): the shortest of the windows containing it, else ``every``."""
        local = moment.astimezone(self.tz).time()
        inside = [w.every_seconds for w in self.windows if w.contains(local)]
        return min(inside) if inside else self.every_seconds

    def is_due(self, last_check_at: datetime | None, now: datetime) -> bool:
        interval = self.interval_at(now)
        if interval is None:
            return False
        if last_check_at is None:
            return True
        return (now - last_check_at) >= timedelta(seconds=interval)

    def in_window(self, moment: datetime) -> bool:
        local = moment.astimezone(self.tz).time()
        return any(w.contains(local) for w in self.windows)

    def max_check_gap_seconds(self) -> float | None:
        """Longest gap between two due checks over a day (``is_due`` simulated minute by minute over two days).

        Used by the health check so that a schedule checked only in a window (e.g. 10:00 and 18:00) is not
        reported as stale overnight. None when the schedule is never due.
        """
        t = datetime(2026, 1, 5, tzinfo=self.tz)
        end, step = t + timedelta(days=2), timedelta(minutes=1)
        last: datetime | None = None
        gap = 0.0
        while t < end:
            if self.is_due(last, t):
                if last is not None:
                    gap = max(gap, (t - last).total_seconds())
                last = t
            t += step
        return gap or None

    @property
    def hash(self) -> str:
        return config_hash(self.as_row())

    def as_row(self) -> dict[str, Any]:
        """The configuration as stored in ``pi_jobs.schedule.config`` (also hashed for sync checks)."""
        return {
            "schedule_key": self.key, "checker": self.checker, "enabled": self.enabled, "source_code": self.source,
            "dataset_codes": list(self.datasets), "lock_group": self.lock_group, "timezone": self.timezone,
            "every_seconds": self.every_seconds,
            "windows": [{"start": w.start.strftime("%H:%M"), "end": "24:00" if w.end is None else w.end.strftime("%H:%M"),
                         "every_seconds": w.every_seconds} for w in self.windows],
            "max_attempts": self.max_attempts, "backoff_base_seconds": self.backoff_base_seconds,
            "backoff_max_seconds": self.backoff_max_seconds, "failure_cooldown_seconds": self.failure_cooldown_seconds,
            "catchup_enabled": self.catchup_enabled, "catchup_max_days": self.catchup_max_days,
            "priority": self.priority, "description": self.description, "params": dict(self.params),
        }


def config_hash(row: dict[str, Any]) -> str:
    """Stable hash of a schedule's configuration (``ScheduleConfig.as_row()`` or the synced JSON)."""
    import hashlib
    import json

    text = json.dumps(row, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class SchedulerConfig:
    schedules: dict[str, ScheduleConfig]
    path: Path | None = None

    def enabled(self) -> list[ScheduleConfig]:
        return [s for s in self.schedules.values() if s.enabled]


_DEFAULTS: dict[str, Any] = {
    "timezone": "Asia/Kolkata", "max_attempts": 5, "backoff_base": "2m", "backoff_max": "30m",
    "failure_cooldown": "6h", "catchup_enabled": True, "catchup_max_days": 7, "priority": 100,
}
_SCHEDULE_KEYS = {"key", "checker", "enabled", "source", "datasets", "lock_group", "timezone", "every", "windows",
                  "max_attempts", "backoff_base", "backoff_max", "failure_cooldown", "catchup_enabled",
                  "catchup_max_days", "priority", "description", "params"}


def _schedule(raw: dict[str, Any], defaults: dict[str, Any], position: int) -> ScheduleConfig:
    where = f"schedule #{position} ({raw.get('key', '?')})"
    unknown = set(raw) - _SCHEDULE_KEYS
    if unknown:
        raise SchedulerConfigError(f"{where}: unknown setting(s) {sorted(unknown)}")
    merged = {**defaults, **raw}
    key = merged.get("key")
    if not isinstance(key, str) or not _KEY.fullmatch(key):
        raise SchedulerConfigError(f"{where}: 'key' must be an upper-case identifier, got {key!r}")
    for name in ("checker", "source", "lock_group"):
        if not isinstance(merged.get(name), str) or not merged[name].strip():
            raise SchedulerConfigError(f"{where}: '{name}' is required")
    datasets = merged.get("datasets", [])
    if not isinstance(datasets, list) or not datasets or not all(isinstance(d, str) for d in datasets):
        raise SchedulerConfigError(f"{where}: 'datasets' must be a non-empty list of dataset codes")
    if not isinstance(merged.get("enabled", False), bool):
        raise SchedulerConfigError(f"{where}: 'enabled' must be true or false")
    tz = merged["timezone"]
    try:
        ZoneInfo(tz)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise SchedulerConfigError(f"{where}: unknown timezone {tz!r}") from exc
    windows = []
    for index, window in enumerate(merged.get("windows", []) or []):
        if not isinstance(window, dict) or set(window) - {"start", "end", "every"}:
            raise SchedulerConfigError(f"{where}: window #{index + 1} needs only 'start', 'end' and 'every'")
        start = parse_clock_time(window.get("start"), f"{where} window #{index + 1} start")
        if start is None:
            raise SchedulerConfigError(f"{where}: window #{index + 1} cannot start at 24:00")
        every = parse_duration(window.get("every"), f"{where} window #{index + 1} every")
        if every is None:
            raise SchedulerConfigError(f"{where}: window #{index + 1} needs an interval")
        windows.append(Window(start, parse_clock_time(window.get("end"), f"{where} window #{index + 1} end"), every))
    every = parse_duration(merged.get("every", "off"), f"{where} every")
    if every is None and not windows:
        raise SchedulerConfigError(f"{where}: needs 'every' or at least one window")
    max_attempts = merged["max_attempts"]
    catchup_days = merged["catchup_max_days"]
    if not isinstance(max_attempts, int) or not 1 <= max_attempts <= 20:
        raise SchedulerConfigError(f"{where}: 'max_attempts' must be 1-20")
    if not isinstance(catchup_days, int) or not 0 <= catchup_days <= 31:
        raise SchedulerConfigError(f"{where}: 'catchup_max_days' must be 0-31 (no historical backfill)")
    params = merged.get("params", {}) or {}
    if not isinstance(params, dict):
        raise SchedulerConfigError(f"{where}: 'params' must be a table")
    return ScheduleConfig(
        key=key, checker=merged["checker"].strip(), enabled=merged.get("enabled", False), source=merged["source"],
        datasets=tuple(datasets), lock_group=merged["lock_group"], timezone=tz, every_seconds=every,
        windows=tuple(windows), max_attempts=max_attempts,
        backoff_base_seconds=parse_duration(merged["backoff_base"], f"{where} backoff_base") or 60.0,
        backoff_max_seconds=parse_duration(merged["backoff_max"], f"{where} backoff_max") or 1800.0,
        failure_cooldown_seconds=parse_duration(merged["failure_cooldown"], f"{where} failure_cooldown") or 0.0,
        catchup_enabled=bool(merged["catchup_enabled"]), catchup_max_days=catchup_days,
        priority=int(merged.get("priority", 100)), description=str(merged.get("description", "")), params=params,
    )


def parse_scheduler_config(data: dict[str, Any], path: Path | None = None,
                           default_timezone: str | None = None) -> SchedulerConfig:
    """``default_timezone`` (SCHEDULER_TIMEZONE) applies when the file sets no timezone."""
    unknown = set(data) - {"defaults", "schedule"}
    if unknown:
        raise SchedulerConfigError(f"unknown top-level section(s) {sorted(unknown)}")
    base = dict(_DEFAULTS)
    if default_timezone:
        base["timezone"] = default_timezone
    defaults = {**base, **(data.get("defaults") or {})}
    unknown_defaults = set(defaults) - set(_DEFAULTS)
    if unknown_defaults:
        raise SchedulerConfigError(f"[defaults]: unknown setting(s) {sorted(unknown_defaults)}")
    schedules: dict[str, ScheduleConfig] = {}
    for position, raw in enumerate(data.get("schedule") or [], start=1):
        if not isinstance(raw, dict):
            raise SchedulerConfigError(f"schedule #{position}: must be a table")
        schedule = _schedule(raw, defaults, position)
        if schedule.key in schedules:
            raise SchedulerConfigError(f"duplicate schedule key {schedule.key}")
        schedules[schedule.key] = schedule
    return SchedulerConfig(schedules, path)


def load_scheduler_config(path: Path, default_timezone: str | None = None) -> SchedulerConfig:
    try:
        with path.open("rb") as handle:
            data = tomllib.load(handle)
    except FileNotFoundError as exc:
        raise SchedulerConfigError(f"scheduler configuration not found: {path}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise SchedulerConfigError(f"{path}: invalid TOML: {exc}") from exc
    return parse_scheduler_config(data, path, default_timezone)
