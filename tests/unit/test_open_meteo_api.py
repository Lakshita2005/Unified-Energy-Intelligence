"""Tests for src.pipelines.open_meteo.api (mocked HTTP only)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import httpx
import pytest

from src.common.http_client import HttpClient
from src.ingestion.catalog import DATASETS
from src.pipelines.open_meteo import api
from src.pipelines.open_meteo.errors import OpenMeteoApiError
from tests.conftest import SettingsFactory
from tests.support.open_meteo import MockOpenMeteo, response_bytes


def test_endpoints_match_the_registered_datasets() -> None:
    endpoints = {e.definition.code: e.definition.endpoint_or_report for e in DATASETS}

    assert api.FORECAST_URL in endpoints["OPEN_METEO_FORECAST"]
    assert api.ARCHIVE_URL in endpoints["OPEN_METEO_HISTORICAL"]


def test_forecast_params_request_explicit_units_and_timezone() -> None:
    params = api.forecast_params(Decimal("10.5"), Decimal("20.25"), 3)

    assert params == {
        "latitude": "10.5", "longitude": "20.25",
        "hourly": "temperature_2m,shortwave_radiation,direct_radiation,diffuse_radiation,wind_speed_10m,"
                  "cloud_cover,relative_humidity_2m,precipitation",
        "timezone": "Asia/Kolkata", "timeformat": "iso8601", "temperature_unit": "celsius",
        "wind_speed_unit": "ms", "precipitation_unit": "mm", "forecast_days": 3, "past_days": 0,
    }


@pytest.mark.parametrize("days", [0, 17, -1])
def test_forecast_horizon_is_bounded(days: int) -> None:
    with pytest.raises(ValueError):
        api.forecast_params(Decimal(1), Decimal(1), days)


def test_historical_params_and_chunking() -> None:
    params = api.historical_params(Decimal(1), Decimal(2), date(2026, 1, 1), date(2026, 1, 31))
    assert (params["start_date"], params["end_date"]) == ("2026-01-01", "2026-01-31")
    assert "forecast_days" not in params

    chunks = api.split_date_range(date(2026, 1, 1), date(2026, 3, 5))
    assert chunks == [(date(2026, 1, 1), date(2026, 1, 31)), (date(2026, 2, 1), date(2026, 3, 3)),
                      (date(2026, 3, 4), date(2026, 3, 5))]
    assert api.split_date_range(date(2026, 1, 1), date(2026, 1, 1)) == [(date(2026, 1, 1), date(2026, 1, 1))]


@pytest.mark.parametrize(
    ("start", "end", "message"),
    [(date(2026, 1, 5), date(2026, 1, 1), "before"), (date(2026, 1, 1), date(2026, 12, 1), "future"),
     (date(2025, 1, 1), date(2025, 12, 31), "limit")],
)
def test_historical_range_guard(start: date, end: date, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        api.validate_historical_range(start, end, today=date(2026, 11, 12), max_days=31)


def test_api_key_switches_to_customer_host() -> None:
    assert api.endpoint(api.FORECAST_URL, None) == api.FORECAST_URL
    assert api.endpoint(api.FORECAST_URL, "k") == "https://customer-api.open-meteo.com/v1/forecast"
    assert api.endpoint(api.ARCHIVE_URL, "k") == "https://customer-archive-api.open-meteo.com/v1/archive"


def test_call_api_sends_key_but_never_exposes_it(make_settings: SettingsFactory) -> None:
    mock = MockOpenMeteo(response_bytes())
    client = HttpClient(settings=make_settings(), transport=httpx.MockTransport(mock), sleep=lambda _: None)

    response = api.call_api(client, api.FORECAST_URL, {"latitude": "1"}, api_key="SECRET-KEY")

    assert mock.requests[0].url.params["apikey"] == "SECRET-KEY"
    assert "SECRET-KEY" not in response.public_url
    assert response.public_url == "https://api.open-meteo.com/v1/forecast?latitude=1"
    assert response.content == response_bytes()
    assert response.retrieved_at.tzinfo is not None


def test_http_400_reason_is_surfaced(make_settings: SettingsFactory) -> None:
    mock = MockOpenMeteo(httpx.Response(400, json={"error": True, "reason": "Latitude must be in range of -90 to 90°"}))
    client = HttpClient(settings=make_settings(), transport=httpx.MockTransport(mock), sleep=lambda _: None)

    with pytest.raises(OpenMeteoApiError, match="HTTP 400.*Latitude must be in range"):
        api.call_api(client, api.FORECAST_URL, {"latitude": "99"})
    assert len(mock.requests) == 1  # client errors are not retried


def test_transient_server_errors_are_retried(make_settings: SettingsFactory) -> None:
    mock = MockOpenMeteo(httpx.Response(503), response_bytes())
    client = HttpClient(settings=make_settings(), transport=httpx.MockTransport(mock), sleep=lambda _: None)

    assert api.call_api(client, api.FORECAST_URL, {}).content == response_bytes()
    assert len(mock.requests) == 2
