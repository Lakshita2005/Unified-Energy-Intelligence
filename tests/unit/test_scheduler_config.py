"""Scheduler configuration: the shipped config/scheduler.toml, durations, local windows and due logic."""

from __future__ import annotations

from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from config.settings import PROJECT_ROOT, Settings
from src.scheduler.checkers import build_checkers
from src.scheduler.config import (
    SchedulerConfigError,
    Window,
    load_scheduler_config,
    parse_duration,
    parse_scheduler_config,
)
from tests.support.scheduler import schedule

IST = ZoneInfo("Asia/Kolkata")
SHIPPED = PROJECT_ROOT / "config" / "scheduler.toml"


def at(hhmm: str, day: int = 25) -> datetime:
    hours, minutes = map(int, hhmm.split(":"))
    return datetime(2026, 11, day, hours, minutes, tzinfo=IST)


# --------------------------------------------------------------------------- shipped configuration
# Activation is approved schedule by schedule; extend this set only together with that approval.
APPROVED_ENABLED = {"IEX_DAM", "IEX_GDAM", "IEX_RTM", "GRID_INDIA_PSP",           # approved 2026-11-26
                    "CEA_COAL_DAILY", "CEA_DGR_DAILY", "CEA_PSP_MONTHLY",         # approved 2026-11-26
                    "MOSDAC_SOLAR", "MOSDAC_WIND",                                # approved 2026-11-26
                    "NPP_DEMAND", "NPP_GENERATION"}                               # approved 2026-11-26


def test_shipped_configuration_is_valid_and_only_approved_schedules_are_enabled() -> None:
    config = load_scheduler_config(SHIPPED)
    checkers = build_checkers(config)          # every checker's parameters are validated too
    assert set(config.schedules) == {
        "CEA_COAL_DAILY", "CEA_DGR_DAILY", "CEA_PSP_MONTHLY", "IEX_DAM", "IEX_GDAM", "IEX_RTM", "GRID_INDIA_PSP",
        "NPP_DEMAND", "NPP_GENERATION", "MOSDAC_SOLAR", "MOSDAC_WIND", "OPEN_METEO_FORECAST",
        "OPEN_METEO_HISTORICAL"}
    assert set(checkers) == set(config.schedules)
    assert {s.key for s in config.enabled()} == APPROVED_ENABLED
    for s in config.schedules.values():
        assert s.timezone == "Asia/Kolkata"
        assert s.catchup_max_days <= 7
    assert config.schedules["OPEN_METEO_HISTORICAL"].catchup_enabled is False


def test_shipped_intervals_match_the_approved_defaults() -> None:
    s = load_scheduler_config(SHIPPED).schedules
    assert s["IEX_RTM"].interval_at(at("03:00")) == 30 * 60
    assert s["IEX_DAM"].interval_at(at("13:00")) == s["IEX_GDAM"].interval_at(at("13:00")) == 15 * 60
    assert s["CEA_COAL_DAILY"].interval_at(at("15:30")) == s["CEA_DGR_DAILY"].interval_at(at("17:00")) == 30 * 60
    assert s["GRID_INDIA_PSP"].interval_at(at("10:30")) == 15 * 60
    assert s["NPP_DEMAND"].interval_at(at("10:30")) == s["NPP_GENERATION"].interval_at(at("23:00")) == 30 * 60
    assert s["MOSDAC_SOLAR"].interval_at(at("10:30")) == 3 * 3600
    assert s["MOSDAC_WIND"].interval_at(at("10:30")) == 3600            # independent, faster recovery
    assert s["MOSDAC_SOLAR"].params["kind"] == "solar" and s["MOSDAC_WIND"].params["kind"] == "wind"
    assert s["OPEN_METEO_FORECAST"].params["slots"] == ["01:30", "07:30", "13:30", "19:30"]   # every 6 h
    assert s["IEX_RTM"].params["market"] == "rtm" and s["GRID_INDIA_PSP"].params["sections"][-1] == "timeseries"


def test_settings_defaults_keep_the_scheduler_off() -> None:
    cfg = Settings(_env_file=None, log_to_file=False)
    assert cfg.scheduler_global_enable is False
    assert cfg.worker_concurrency == 2 and cfg.scheduler_timezone == "Asia/Kolkata"
    assert cfg.scheduler_config_file == SHIPPED.resolve()
    assert cfg.scheduler_log_file_name != cfg.worker_log_file_name


# --------------------------------------------------------------------------- parsing
@pytest.mark.parametrize(("text", "seconds"), [("45s", 45), ("30m", 1800), ("6h", 21600), ("1d", 86400),
                                               ("1.5h", 5400), (90, 90), ("off", None), ("", None)])
def test_parse_duration(text, seconds) -> None:
    assert parse_duration(text, "x") == seconds


@pytest.mark.parametrize("bad", ["0m", "-5m", "5 minutes", "abc", 0, True])
def test_parse_duration_rejects(bad) -> None:
    with pytest.raises(SchedulerConfigError):
        parse_duration(bad, "x")


@pytest.mark.parametrize(("raw", "message"), [
    ({"key": "lower"}, "upper-case"),
    ({"bogus": 1}, "unknown setting"),
    ({"catchup_max_days": 90}, "no historical backfill"),
    ({"timezone": "Mars/Base"}, "unknown timezone"),
    ({"every": "off", "windows": []}, "needs 'every' or at least one window"),
    ({"windows": [{"start": "24:00", "end": "01:00", "every": "5m"}]}, "cannot start at 24:00"),
    ({"datasets": []}, "datasets"),
])
def test_invalid_schedules_are_rejected(raw, message) -> None:
    base = {"key": "OK_KEY", "checker": "fake", "source": "S", "datasets": ["D"], "lock_group": "G", "every": "1h"}
    with pytest.raises(SchedulerConfigError, match=message):
        parse_scheduler_config({"schedule": [{**base, **raw}]})


def test_scheduler_timezone_setting_is_the_fallback_only() -> None:
    raw = {"schedule": [{"key": "TZ_KEY", "checker": "fake", "source": "S", "datasets": ["D"], "lock_group": "G",
                         "every": "1h"}]}
    assert parse_scheduler_config(raw, default_timezone="UTC").schedules["TZ_KEY"].timezone == "UTC"
    with_file_default = {**raw, "defaults": {"timezone": "Asia/Kolkata"}}
    assert parse_scheduler_config(with_file_default, default_timezone="UTC").schedules["TZ_KEY"].timezone == \
        "Asia/Kolkata"


def test_duplicate_keys_and_unknown_checker_are_rejected() -> None:
    base = {"key": "A_KEY", "checker": "nope", "source": "S", "datasets": ["D"], "lock_group": "G", "every": "1h"}
    with pytest.raises(SchedulerConfigError, match="duplicate"):
        parse_scheduler_config({"schedule": [base, base]})
    with pytest.raises(SchedulerConfigError, match="unknown checker"):
        build_checkers(parse_scheduler_config({"schedule": [base]}))
    bad_params = {**base, "checker": "iex", "params": {"market": "dam", "colour": "red"}}
    with pytest.raises(SchedulerConfigError, match="unknown parameter"):
        build_checkers(parse_scheduler_config({"schedule": [bad_params]}))


# --------------------------------------------------------------------------- windows, timezone, due
def test_windows_including_midnight_and_end_of_day() -> None:
    day = Window(time(9, 0), time(14, 0), 900)
    night = Window(time(22, 0), time(2, 0), 600)
    late = Window(time(12, 0), None, 1800)            # 12:00-24:00
    assert day.contains(time(9, 0)) and day.contains(time(13, 59)) and not day.contains(time(14, 0))
    assert night.contains(time(23, 30)) and night.contains(time(1, 59)) and not night.contains(time(2, 0))
    assert late.contains(time(23, 59)) and not late.contains(time(11, 59))


def test_interval_uses_local_time_not_utc() -> None:
    s = schedule(every="2h", windows=[{"start": "09:00", "end": "14:00", "every": "15m"}])
    # 04:00 UTC = 09:30 IST -> inside the window; 02:00 UTC = 07:30 IST -> outside
    assert s.interval_at(datetime(2026, 11, 25, 4, 0, tzinfo=timezone.utc)) == 900
    assert s.interval_at(datetime(2026, 11, 25, 2, 0, tzinfo=timezone.utc)) == 7200
    assert s.in_window(at("13:59")) and not s.in_window(at("14:00"))


def test_is_due_follows_the_interval_in_force() -> None:
    s = schedule(every="off", windows=[{"start": "12:00", "end": "16:00", "every": "15m"}])
    assert s.is_due(None, at("12:00"))
    assert not s.is_due(at("12:00"), at("12:14"))
    assert s.is_due(at("12:00"), at("12:15"))
    assert not s.is_due(None, at("16:00"))            # outside the window and every = off: never due
    overlapping = schedule(every="1h", windows=[{"start": "09:00", "end": "18:00", "every": "30m"},
                                                {"start": "12:00", "end": "13:00", "every": "5m"}])
    assert overlapping.interval_at(at("12:30")) == 300   # the shortest interval of the windows containing it
    assert overlapping.is_due(at("12:30") - timedelta(minutes=5), at("12:30"))
