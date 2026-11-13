"""Tests for src.pipelines.open_meteo.parser (mocked responses only)."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from src.pipelines.open_meteo.api import HOURLY_VARIABLES
from src.pipelines.open_meteo.errors import EmptyResponseError, InvalidResponseError, OpenMeteoApiError
from src.pipelines.open_meteo.parser import parse_hourly_response
from tests.support.open_meteo import response_bytes, response_dict

IST = timezone(timedelta(hours=5, minutes=30))


def _parse(content: bytes, *, is_forecast: bool = True):
    return parse_hourly_response(content, weather_location_id=7, location_code="SITE_A", is_forecast=is_forecast)


def test_variables_map_to_the_expected_columns() -> None:
    assert {v.api_name: v.column for v in HOURLY_VARIABLES} == {
        "temperature_2m": "temperature_2m_c",
        "shortwave_radiation": "shortwave_radiation_wm2",
        "direct_radiation": "direct_radiation_wm2",
        "diffuse_radiation": "diffuse_radiation_wm2",
        "wind_speed_10m": "wind_speed_10m_ms",
        "cloud_cover": "cloud_cover_pct",
        "relative_humidity_2m": "relative_humidity_2m_pct",
        "precipitation": "precipitation_mm",
    }


def test_parses_every_hour_with_exact_values() -> None:
    result = _parse(response_bytes())

    assert result.failures == [] and len(result.records) == 3
    first = result.records[0]
    assert first.key == "SITE_A|2026-11-12T00:00"
    assert first.values == {
        "weather_location_id": 7, "valid_time": datetime(2026, 11, 12, 0, 0, tzinfo=IST), "is_forecast": True,
        "forecast_issued_at": None, "temperature_2m_c": Decimal("21.4"), "shortwave_radiation_wm2": Decimal("0.0"),
        "direct_radiation_wm2": Decimal("0.0"), "diffuse_radiation_wm2": Decimal("0.0"),
        "wind_speed_10m_ms": Decimal("2.35"), "cloud_cover_pct": 12, "relative_humidity_2m_pct": 61,
        "precipitation_mm": Decimal("0.0"),
    }
    assert first.raw["time"] == "2026-11-12T00:00" and first.raw["wind_speed_10m"] == Decimal("2.35")


def test_timestamps_use_the_response_offset_and_identify_instants() -> None:
    valid_time = _parse(response_bytes()).records[0].values["valid_time"]

    assert valid_time.utcoffset() == timedelta(hours=5, minutes=30)
    assert valid_time == datetime(2026, 11, 11, 18, 30, tzinfo=timezone.utc)


def test_forecast_issue_time_is_never_guessed() -> None:
    for record in _parse(response_bytes()).records:
        assert record.values["forecast_issued_at"] is None


def test_historical_flag() -> None:
    assert all(r.values["is_forecast"] is False for r in _parse(response_bytes(), is_forecast=False).records)


def test_null_measurements_are_kept_as_none() -> None:
    content = response_bytes(values={"temperature_2m": [None, 20.9, None], "precipitation": [None, None, None]})

    records = _parse(content).records

    assert records[0].values["temperature_2m_c"] is None
    assert records[1].values["temperature_2m_c"] == Decimal("20.9")
    assert all(r.values["precipitation_mm"] is None for r in records)


def test_non_numeric_value_becomes_parse_failure() -> None:
    content = response_bytes(values={"cloud_cover": [12, "n/a", 100]})

    result = _parse(content)

    assert len(result.records) == 2
    assert result.failures[0].key == "SITE_A|2026-11-12T01:00"
    assert "cloud_cover" in result.failures[0].description
    assert result.failures[0].raw["cloud_cover"] == "n/a"


def test_bad_timestamp_becomes_parse_failure() -> None:
    result = _parse(response_bytes(times=["2026-11-12T00:00", "not-a-time", "2026-11-12T02:00"]))

    assert len(result.records) == 2 and "Invalid timestamp" in result.failures[0].description


def _mutated(**changes: object) -> bytes:
    data = response_dict()
    for path, value in changes.items():
        target = data
        *parents, leaf = path.split("__")
        for parent in parents:
            target = target[parent]
        if value is ...:
            del target[leaf]
        else:
            target[leaf] = value
    return json.dumps(data).encode()


@pytest.mark.parametrize(
    ("content", "message"),
    [
        (b"<html>gateway error</html>", "not valid JSON"),
        (b"[]", "Expected a JSON object"),
        (_mutated(timezone="GMT"), "timezone"),
        (_mutated(utc_offset_seconds=...), "utc_offset_seconds"),
        (_mutated(hourly=...), "'hourly'"),
        (_mutated(hourly__precipitation=...), "precipitation"),
        (_mutated(hourly__temperature_2m=[1.0]), "has 1 values for 3 timestamps"),
        (_mutated(hourly_units__wind_speed_10m="km/h"), "km/h"),
        (_mutated(hourly_units__time="unixtime"), "time format"),
    ],
)
def test_invalid_responses_are_rejected_as_a_whole(content: bytes, message: str) -> None:
    with pytest.raises(InvalidResponseError, match=message):
        _parse(content)


def test_empty_response_is_distinguished_from_invalid() -> None:
    with pytest.raises(EmptyResponseError):
        _parse(response_bytes(times=[], values={name: [] for name in response_dict()["hourly"] if name != "time"}))


def test_api_error_object_is_reported_as_api_error() -> None:
    with pytest.raises(OpenMeteoApiError, match="Cannot initialize"):
        _parse(json.dumps({"error": True, "reason": "Cannot initialize WeatherVariable from invalid String"}).encode())
