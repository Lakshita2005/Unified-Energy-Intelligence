"""ScheduleConfig.max_check_gap_seconds: the health check's per-schedule staleness limit (no false STALE alerts
for schedules that are only checked inside windows, e.g. CEA PSP at 10:00 and 18:00)."""

from __future__ import annotations

from config.settings import PROJECT_ROOT
from src.scheduler.config import load_scheduler_config
from tests.support.scheduler import schedule


def test_interval_only_schedule_gap_is_its_interval() -> None:
    assert schedule("GAP_EVERY", every="30m").max_check_gap_seconds() == 1800


def test_window_only_schedule_gap_spans_the_night() -> None:
    s = schedule("GAP_WINDOWS", every="off", windows=[{"start": "10:00", "end": "10:30", "every": "30m"},
                                                       {"start": "18:00", "end": "18:30", "every": "30m"}])
    assert s.max_check_gap_seconds() == 16 * 3600                          # 18:00 -> 10:00 next day


def test_window_plus_interval_uses_the_longer_outside_interval() -> None:
    s = schedule("GAP_MIXED", every="2h", windows=[{"start": "12:00", "end": "24:00", "every": "30m"}])
    assert s.max_check_gap_seconds() == 2 * 3600


def test_a_never_due_schedule_cannot_be_configured() -> None:
    import pytest

    from src.scheduler.config import SchedulerConfigError

    with pytest.raises(SchedulerConfigError, match="needs 'every' or at least one window"):
        schedule("GAP_NEVER", every="off")


def test_shipped_schedules_have_the_expected_gaps() -> None:
    shipped = load_scheduler_config(PROJECT_ROOT / "config" / "scheduler.toml").schedules
    hours = {k: s.max_check_gap_seconds() / 3600 for k, s in shipped.items()}
    assert hours["CEA_PSP_MONTHLY"] == 16 and hours["OPEN_METEO_HISTORICAL"] == 24
    assert hours["IEX_RTM"] == 0.5 and hours["GRID_INDIA_PSP"] == 1 and hours["MOSDAC_SOLAR"] == 3
    assert max(hours.values()) <= 24
