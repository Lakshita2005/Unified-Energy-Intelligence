"""MOSDAC parser, client error handling and location configuration (no database, no network)."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from config.settings import PROJECT_ROOT, Settings
from src.common.exceptions import HttpRequestError, HttpStatusError
from src.common.http_client import HttpClient
from src.pipelines.mosdac import api
from src.pipelines.mosdac.errors import MosdacResponseError
from src.pipelines.mosdac.parse import IST, SOLAR, WIND, Location, parse_series, parse_time
from src.pipelines.mosdac.runner import load_locations
from tests.support.mosdac import API, MockMosdac, body, server_error, solar_records, wind_records

LOC = Location("ESEPL", Decimal("17.4042"), Decimal("77.3753"))
RETRIEVED = datetime(2030, 1, 9, 23, 0, tzinfo=timezone.utc)      # before the synthetic series (2030-01-10 05:30 IST)


def codes(result, attr: str = "warnings") -> list[str]:
    return [c for c, _, _ in getattr(result, attr)]


# --------------------------------------------------------------------------- valid responses

def test_valid_solar_response() -> None:
    result = parse_series(solar_records(), SOLAR, LOC, RETRIEVED)

    assert len(result.records) == 289 and not result.failures
    assert result.warnings == [] and result.info == []
    first = result.records[0].values
    assert first["valid_at"] == datetime(2030, 1, 10, 5, 30, tzinfo=IST)
    assert first["valid_at"].astimezone(timezone.utc) == datetime(2030, 1, 10, 0, 0, tzinfo=timezone.utc)
    assert first["source_time_text"] == "10-01-30 0530"
    assert first["air_temperature_c"] == Decimal("18.117") and first["solar_flux_w_m2"] == Decimal("0.111")
    assert "ghi" not in " ".join(first).lower()                      # flux is never labelled GHI
    assert all(r.values["series_start_at"] == first["valid_at"] for r in result.records)
    assert first["availability_basis"] == "RETRIEVED_AT" and first["source_available_at"] == RETRIEVED
    assert first["source_published_at"] is None
    assert (first["location_code"], first["latitude"], first["longitude"]) == ("ESEPL", LOC.latitude, LOC.longitude)


def test_valid_wind_response_keeps_values_in_neutral_columns() -> None:
    result = parse_series(wind_records(), WIND, LOC, RETRIEVED)

    assert len(result.records) == 289 and not result.failures and not result.warnings
    values = result.records[1].values
    assert values["wind_speed_value"] == Decimal("2.328") and values["wind_direction_value"] == Decimal("7.424")
    assert set(values) >= {"wind_speed_value", "wind_direction_value"}
    assert not {"wind_speed_ms", "wind_direction_deg"} & set(values)  # no unit assumed


def test_json_numbers_and_exact_decimals_are_accepted() -> None:
    payload = [{"time": "10-01-30 0530", "temp": 21.5, "flux": 0}]
    record = parse_series(payload, SOLAR, LOC, RETRIEVED).records[0].values
    assert record["air_temperature_c"] == Decimal("21.5") and record["solar_flux_w_m2"] == Decimal(0)


def test_point_count_other_than_289_is_reported_not_rejected() -> None:
    result = parse_series(solar_records(97), SOLAR, LOC, RETRIEVED)
    assert len(result.records) == 97 and "POINT_COUNT" in codes(result, "info")


def test_series_entirely_before_retrieval_is_flagged_stale() -> None:
    later = datetime(2031, 1, 1, tzinfo=timezone.utc)
    result = parse_series(solar_records(), SOLAR, LOC, later)
    assert len(result.records) == 289 and "STALE_FORECAST" in codes(result)


def test_partly_past_series_is_informational() -> None:
    midway = datetime(2030, 1, 11, 0, 0, tzinfo=timezone.utc)
    result = parse_series(solar_records(), SOLAR, LOC, midway)
    assert "POINTS_BEFORE_RETRIEVAL" in codes(result, "info") and "STALE_FORECAST" not in codes(result)


# --------------------------------------------------------------------------- structure

@pytest.mark.parametrize("payload", [
    {"status": "Internal Server Error", "message": "x"},
    "not a list",
    42,
    [1, 2, 3],
    [{"time": "10-01-30 0530", "temp": "1.0", "flux": "2.0"}, "oops"],
])
def test_unrecognised_structure_raises(payload) -> None:
    with pytest.raises(MosdacResponseError):
        parse_series(payload, SOLAR, LOC, RETRIEVED)


def test_records_without_expected_keys_mean_structure_changed() -> None:
    with pytest.raises(MosdacResponseError, match="structure changed"):
        parse_series([{"timestamp": "10-01-30 0530", "value": "1"}], SOLAR, LOC, RETRIEVED)
    with pytest.raises(MosdacResponseError, match="expected keys"):        # wind keys sent to the solar parser
        parse_series(wind_records(3), SOLAR, LOC, RETRIEVED)


def test_malformed_json_raises() -> None:
    with pytest.raises(MosdacResponseError, match="not valid JSON"):
        api.decode(b"<html>Service unavailable</html>", "solar")
    with pytest.raises(MosdacResponseError):
        api.decode(b'[{"time": "10-01-30 0530", "temp": ', "solar")


def test_empty_list_is_no_data() -> None:
    result = parse_series([], SOLAR, LOC, RETRIEVED)
    assert result.records == [] and codes(result, "info") == ["NO_DATA"]


def test_missing_fields_are_null_and_reported() -> None:
    records = solar_records(4)
    del records[1]["flux"]
    del records[2]["temp"]
    result = parse_series(records, SOLAR, LOC, RETRIEVED)

    assert len(result.records) == 4
    assert result.records[1].values["solar_flux_w_m2"] is None
    assert result.records[2].values["air_temperature_c"] is None
    assert codes(result).count("MISSING_FIELD") == 2


def test_missing_time_rejects_the_record() -> None:
    records = solar_records(3)
    del records[1]["time"]
    result = parse_series(records, SOLAR, LOC, RETRIEVED)
    assert len(result.records) == 2 and len(result.failures) == 1 and result.failures[0].key == "ESEPL|row1"


def test_null_and_empty_values_are_null_and_reported() -> None:
    records = solar_records(3)
    records[0]["flux"] = None
    records[1]["temp"] = ""
    result = parse_series(records, SOLAR, LOC, RETRIEVED)
    assert len(result.records) == 3 and codes(result).count("MISSING_VALUE") == 2
    assert result.records[0].values["solar_flux_w_m2"] is None


@pytest.mark.parametrize("bad", ["abc", "NaN", "Infinity", True, [1], {"v": 1}])
def test_non_numeric_value_rejects_the_record(bad) -> None:
    records = solar_records(3)
    records[1]["temp"] = bad
    result = parse_series(records, SOLAR, LOC, RETRIEVED)
    assert len(result.records) == 2 and len(result.failures) == 1


def test_unexpected_fields_are_reported() -> None:
    records = solar_records(2)
    records[0]["humidity"] = "55"
    result = parse_series(records, SOLAR, LOC, RETRIEVED)
    assert len(result.records) == 2 and "UNDOCUMENTED_FIELDS" in codes(result, "info")


def test_implausible_values_are_kept_and_warned() -> None:
    records = solar_records(3)
    records[0]["flux"] = "-5.0"
    records[1]["temp"] = "75.0"
    result = parse_series(records, SOLAR, LOC, RETRIEVED)
    assert len(result.records) == 3 and codes(result).count("OUT_OF_RANGE") == 2
    wind = wind_records(2)
    wind[0]["direction"] = "400"
    assert "OUT_OF_RANGE" in codes(parse_series(wind, WIND, LOC, RETRIEVED))


# --------------------------------------------------------------------------- columnar form (task brief)

def test_columnar_arrays_of_equal_length_are_accepted() -> None:
    rows = solar_records(5)
    payload = {"time": [r["time"] for r in rows], "temp": [r["temp"] for r in rows], "flux": [r["flux"] for r in rows]}
    result = parse_series(payload, SOLAR, LOC, RETRIEVED)
    assert len(result.records) == 5 and "COLUMNAR_RESPONSE" in codes(result, "info")


def test_unequal_array_lengths_reject_the_response() -> None:
    rows = solar_records(5)
    payload = {"time": [r["time"] for r in rows], "temp": [r["temp"] for r in rows],
               "flux": [r["flux"] for r in rows][:4]}
    with pytest.raises(MosdacResponseError, match="unequal lengths"):
        parse_series(payload, SOLAR, LOC, RETRIEVED)


# --------------------------------------------------------------------------- timestamps

@pytest.mark.parametrize("value", ["2030-01-10 05:30", "10-01-2030 0530", "32-01-30 0530", "10-13-30 0530",
                                   "10-01-30 2460", "10-01-30 05:30", "", None, 1700000000])
def test_invalid_timestamps_are_rejected(value) -> None:
    with pytest.raises(ValueError):
        parse_time(value)
    records = solar_records(3)
    records[1]["time"] = value
    result = parse_series(records, SOLAR, LOC, RETRIEVED)
    assert len(result.records) == 2 and len(result.failures) == 1


def test_timestamp_is_ist() -> None:
    assert parse_time("13-11-26 0530") == datetime(2026, 11, 13, 0, 0, tzinfo=timezone.utc)


def test_duplicate_timestamps_are_reported() -> None:
    records = solar_records(4)
    records.insert(2, dict(records[1]))
    result = parse_series(records, SOLAR, LOC, RETRIEVED)
    assert "DUPLICATE_TIMESTAMP" in codes(result)
    assert len(result.records) == 5            # the loader loads identical copies once / rejects conflicting copies


def test_missing_steps_and_irregular_spacing_are_reported() -> None:
    records = solar_records(10)
    del records[4:6]                           # two 15-minute steps missing
    records[-1]["time"] = records[-1]["time"][:-2] + "07"     # 7 minutes past: not on the 15-minute grid
    result = parse_series(records, SOLAR, LOC, RETRIEVED)
    found = codes(result)
    assert "MISSING_TIMESTAMPS" in found and "IRREGULAR_SPACING" in found
    assert any("2 15-minute step(s) missing" in d for c, d, _ in result.warnings if c == "MISSING_TIMESTAMPS")


def test_out_of_order_times_are_reported_and_series_start_is_the_earliest() -> None:
    records = solar_records(4)
    records.reverse()
    result = parse_series(records, SOLAR, LOC, RETRIEVED)
    assert "OUT_OF_ORDER" in codes(result, "info")
    assert result.series_start_at == datetime(2030, 1, 10, 5, 30, tzinfo=IST)


# --------------------------------------------------------------------------- client reliability

def _client(handler, retries: int = 2) -> tuple[api.MosdacClient, list[float]]:
    sleeps: list[float] = []
    settings = Settings(mosdac_api_base_url=API, http_max_retries=retries, log_to_file=False)
    http = HttpClient(settings=settings, transport=httpx.MockTransport(handler), sleep=sleeps.append)
    return api.MosdacClient(http, settings), sleeps


def test_client_sends_coordinates_exactly() -> None:
    mock = MockMosdac()
    mock.set("solar", solar_records(2))
    client, _ = _client(mock)
    response = client.get("solar", "12.0981737", "76.8406883")
    assert mock.calls == ["solar?lat=12.0981737&lon=76.8406883"]
    assert response.status_code == 200 and response.content == body(solar_records(2))
    assert response.url.endswith("/apienergy/solar?lat=12.0981737&lon=76.8406883")


def test_http_500_is_retried_a_limited_number_of_times() -> None:
    mock = MockMosdac()
    mock.set("wind", server_error)
    client, sleeps = _client(mock, retries=2)
    with pytest.raises(HttpStatusError) as info:
        client.get("wind", "17.4042", "77.3753")
    assert info.value.status_code == 500
    assert len(mock.calls) == 3 and len(sleeps) == 2 and sleeps[1] > sleeps[0] * 1.5   # exponential backoff


def test_http_500_then_success_recovers() -> None:
    mock = MockMosdac()
    mock.set("wind", server_error, wind_records(3))
    client, _ = _client(mock)
    assert client.get("wind", "17.4042", "77.3753").status_code == 200 and len(mock.calls) == 2


def test_ssl_eof_network_failure_is_retried_then_raised() -> None:
    mock = MockMosdac()
    mock.set("solar", httpx.ConnectError("[SSL: UNEXPECTED_EOF_WHILE_READING] EOF occurred in violation of protocol"))
    client, sleeps = _client(mock, retries=2)
    with pytest.raises(HttpRequestError, match="UNEXPECTED_EOF"):
        client.get("solar", "17.4042", "77.3753")
    assert len(mock.calls) == 3 and len(sleeps) == 2


def test_timeout_is_retried_then_raised() -> None:
    mock = MockMosdac()
    mock.set("solar", httpx.ReadTimeout("timed out"))
    client, _ = _client(mock, retries=1)
    with pytest.raises(HttpRequestError, match="ReadTimeout"):
        client.get("solar", "17.4042", "77.3753")
    assert len(mock.calls) == 2


def test_client_error_status_is_not_retried() -> None:
    mock = MockMosdac()
    mock.set("solar", httpx.Response(404, json={"error": "no"}))
    client, _ = _client(mock)
    with pytest.raises(HttpStatusError):
        client.get("solar", "17.4042", "77.3753")
    assert len(mock.calls) == 1


def test_unknown_endpoint_is_refused() -> None:
    client, _ = _client(MockMosdac())
    with pytest.raises(ValueError):
        client.get("rain", "1", "2")  # type: ignore[arg-type]


# --------------------------------------------------------------------------- locations

EXPECTED_PLANTS = {
    "ESEPL": ("17.4042", "77.3753"), "NSPL": ("12.0981737", "76.8406883"), "ASPL": ("16.40612", "78.69986"),
    "GSPL_GAP": ("16.6243", "78.0630"), "GSPL_GUM": ("17.7550", "78.4220"), "PSPL": ("18.41415", "78.21599"),
    "RSPL": ("18.7897", "77.9162"), "TSPL": ("18.0256", "78.7899"),
}


def test_all_eight_plant_configurations() -> None:
    settings = Settings(mosdac_locations_file=PROJECT_ROOT / "config" / "mosdac_locations.toml", log_to_file=False)
    locations = load_locations(settings)

    assert set(locations) == set(EXPECTED_PLANTS)
    for code, (lat, lon) in EXPECTED_PLANTS.items():
        # Exactly the supplied digits are sent (Decimal keeps e.g. the trailing zero of 78.0630).
        assert (str(locations[code].latitude), str(locations[code].longitude)) == (lat, lon), code
        assert locations[code].plant_code is None and locations[code].coordinate_source


def test_location_file_is_separate_from_open_meteo() -> None:
    settings = Settings(log_to_file=False)
    assert settings.mosdac_locations_file != settings.weather_locations_file


def test_missing_locations_file_means_no_locations(tmp_path: Path) -> None:
    assert load_locations(Settings(mosdac_locations_file=tmp_path / "none.toml", log_to_file=False)) == {}
