"""IMD parsing, response structure, state mapping and API client (synthetic data, mocked HTTP)."""

from __future__ import annotations

import json
from datetime import date, datetime, time, timezone
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from config.settings import Settings
from src.common.exceptions import HttpStatusError
from src.common.http_client import HttpClient
from src.pipelines.imd import api
from src.pipelines.imd.errors import ImdAccessDenied, ImdResponseError
from src.pipelines.imd.parse import (
    parse_aws_observations,
    parse_aws_station_mapping,
    parse_city_forecast,
    to_decimal,
)
from src.pipelines.imd.pipeline import ImdCityForecastPipeline, ImdObservationPipeline
from src.pipelines.imd.reference import AWS_STATE_IDS, IMD_STATE_TO_PI, state_code_for
from tests.support.imd import API, MockImd, aws_record, body, city_record, station_record

RETRIEVED = datetime(2026, 11, 10, 6, 0, tzinfo=timezone.utc)
PI_STATES = {"AN", "AP", "AR", "AS", "BR", "CG", "CH", "DH", "DL", "GA", "GJ", "HP", "HR", "JH", "JK", "KA", "KL", "LA",
             "LD", "MH", "ML", "MN", "MP", "MZ", "NL", "OD", "PB", "PY", "RJ", "SK", "TN", "TR", "TS", "UK", "UP", "WB"}


def decoded(records: list) -> list:
    return api.decode_records(body(records), "test")


def codes(result) -> list[str]:
    return [w[0] for w in result.warnings]


# ------------------------------------------------------------------ helpers

@pytest.mark.parametrize(("value", "expected"), [(23.417, Decimal("23.417")), (20, Decimal(20)), ("1003.0",
                         Decimal("1003.0")), (" 7.5 ", Decimal("7.5")), (None, None), ("", None)])
def test_to_decimal(value, expected) -> None:
    assert to_decimal(value, "X") == expected


@pytest.mark.parametrize("value", ["NA", "--", True, "nan", "inf"])
def test_to_decimal_rejects_non_numbers(value) -> None:
    with pytest.raises(ValueError):
        to_decimal(value, "X")


def test_documented_states_map_exactly_to_pi_states() -> None:
    assert len(AWS_STATE_IDS) == 36 and set(IMD_STATE_TO_PI.values()) <= PI_STATES
    assert state_code_for("DELHI") == "DL" and state_code_for(" daman_and_diu ") == "DH"
    assert state_code_for("NEW DELHI") is None and state_code_for("Delhi State") is None and state_code_for(None) is None


def test_response_container_must_be_a_list_of_objects() -> None:
    assert api.decode_records(b'[{"ID": "A", "CURR_TEMP": 23.417}]', "t") == [{"ID": "A",
                                                                               "CURR_TEMP": Decimal("23.417")}]
    for content, message in ((b'{"data": []}', "expected a JSON list"), (b"<html>", "not JSON"),
                             (b"[1, 2]", "non-object")):
        with pytest.raises(ImdResponseError, match=message):
            api.decode_records(content, "t")


# ----------------------------------------------------------------- stations

def test_station_mapping_success_states_and_coordinates() -> None:
    result = parse_aws_station_mapping(decoded([station_record(), station_record(
        ID="SYNB0002", CALL_SIGN="TSB", STATE="DAMAN_AND_DIU")]), RETRIEVED)
    assert not result.failures and not result.warnings
    first = result.records[0].values
    assert (first["network"], first["imd_id"], first["call_sign"], first["state_code"], first["latitude"]) == (
        "AWS_ARG", "SYNA0001", "TSA", "DL", Decimal("28.123"))
    assert result.records[1].values["state_code"] == "DH"
    assert (first["availability_basis"], first["source_available_at"]) == ("RETRIEVED_AT", RETRIEVED)


def test_station_mapping_unmapped_state_missing_id_ambiguity_and_bad_coordinates() -> None:
    result = parse_aws_station_mapping(decoded([
        station_record(STATE="NEW DELHI"),                                 # not an exact documented name
        station_record(ID=None),                                           # missing identity
        station_record(ID="SYNA0001", CALL_SIGN="TSZ"),                    # same ID, other call sign
        station_record(ID="SYNC0003", CALL_SIGN="TSC", Latitude=95.123),   # impossible latitude
        station_record(ID="SYND0004", CALL_SIGN="TSD", Longitude="east"),  # not a number
        station_record(ID="SYNE0005", CALL_SIGN="TSE", ALTITUDE=211.317),  # undocumented field
    ]), RETRIEVED)
    assert len(result.records) == 3 and len(result.failures) == 3
    assert result.records[0].values["state_code"] is None and "UNMAPPED_STATE" in codes(result)
    assert codes(result).count("STATION_ID_AMBIGUOUS") == 1
    assert [i[0] for i in result.info] == ["UNDOCUMENTED_FIELDS"]


def test_station_mapping_with_unrecognised_structure_fails_loudly() -> None:
    with pytest.raises(ImdResponseError, match="not documented"):
        parse_aws_station_mapping(decoded([{"station_id": 1, "name": "X"}]), RETRIEVED)
    assert parse_aws_station_mapping([], RETRIEVED).records == []


# -------------------------------------------------------------- observations

def test_observation_success_exact_values_and_published_time() -> None:
    result = parse_aws_observations(decoded([aws_record()]), RETRIEVED)
    assert not result.failures and not result.warnings
    v = result.records[0].values
    assert (v["observation_date"], v["observation_time"], v["observed_at"]) == (date(2026, 11, 10), time(7), None)
    assert (v["curr_temp_value"], v["mslp_value"], v["feels_like_value"], v["weather_code"]) == (
        Decimal("23.417"), Decimal("1011.237"), Decimal("24.193"), Decimal(5))
    assert v["imd_station_id"] is None and v["state_code"] == "DL"
    assert [i[0] for i in result.info] == ["TIME_ZONE_UNVERIFIED"]          # time zone never assumed


@pytest.mark.parametrize(("zone", "expected"), [("UTC", datetime(2026, 11, 10, 7, tzinfo=timezone.utc)),
                                                ("Asia/Kolkata", datetime(2026, 11, 10, 1, 30, tzinfo=timezone.utc))])
def test_observed_at_only_when_time_zone_configured(zone: str, expected: datetime) -> None:
    result = parse_aws_observations(decoded([aws_record()]), RETRIEVED, zone)
    assert result.records[0].values["observed_at"] == expected and not result.info


def test_observation_missing_fields_values_invalid_numbers_and_ranges() -> None:
    absent = {k: v for k, v in aws_record(ID="SYNF0006", CALL_SIGN="TSF").items() if k != "MSLP"}
    result = parse_aws_observations(decoded([
        absent,                                                            # field absent -> NULL + warning
        aws_record(ID="SYNG0007", CALL_SIGN="TSG", RH=None, WIND_SPEED=""),  # no value -> NULL + warning
        aws_record(ID="SYNH0008", CALL_SIGN="TSH", CURR_TEMP="NA"),          # invalid -> rejected
        aws_record(ID="SYNI0009", CALL_SIGN="TSI", DATE=None),               # missing DATE -> rejected
        aws_record(ID="SYNJ0010", CALL_SIGN="TSJ", TIME="7am"),              # undocumented format -> rejected
        aws_record(ID="SYNK0011", CALL_SIGN="TSK", RH=147.313, NEBULOSITY=9),  # implausible -> kept with warnings
    ]), RETRIEVED)
    assert len(result.records) == 3 and len(result.failures) == 3
    assert result.records[0].values["mslp_value"] is None
    assert {codes(result).count(c) for c in ("MISSING_FIELD", "MISSING_VALUE")} == {1}
    assert codes(result).count("OUT_OF_RANGE") == 2
    assert {r.values["rh_value"] for r in result.records} >= {None, Decimal("147.313")}
    reasons = " ".join(f.description for f in result.failures)
    assert "not a number: 'NA'" in reasons and "without ['DATE']" in reasons and "YYYY-mm-dd / HH:MM:SS" in reasons


def test_all_measurements_missing_is_kept_and_reported() -> None:
    from src.pipelines.imd.parse import AWS_MEASUREMENTS
    empty = aws_record(**{k: None for k in AWS_MEASUREMENTS})
    result = parse_aws_observations(decoded([empty]), RETRIEVED)
    assert len(result.records) == 1 and "ALL_MEASUREMENTS_NULL" in codes(result)


# ---------------------------------------------------------------- forecasts

def test_city_forecast_seven_vintage_rows_and_observed_fields_not_loaded() -> None:
    result = parse_city_forecast(decoded([city_record()]), RETRIEVED)
    assert not result.failures and not result.warnings and len(result.records) == 7
    day1, day7 = result.records[0].values, result.records[6].values
    assert (day1["issue_date"], day1["forecast_date"], day1["lead_day"], day1["max_temp_value"],
            day1["min_temp_value"], day1["forecast_text"]) == (date(2026, 11, 10), date(2026, 11, 10), 1,
                                                               Decimal("29.137"), Decimal("16.317"), "Mainly clear sky")
    assert (day7["forecast_date"], day7["lead_day"], day7["max_temp_value"]) == (date(2026, 11, 16), 7,
                                                                                 Decimal("29.914"))
    assert not any(k in v for r in result.records for v in [r.values] for k in ("Today_Max_temp", "rainfall"))
    assert [i[0] for i in result.info] == ["OBSERVED_FIELDS_NOT_LOADED"]


def test_city_forecast_incomplete_invalid_and_missing() -> None:
    partial = {k: v for k, v in city_record(code="99002").items() if not k.startswith("Day_7")}
    result = parse_city_forecast(decoded([
        partial,                                                     # day 7 absent
        city_record(code="99003", Day_3_Max_Temp="hot"),             # invalid -> that day rejected only
        city_record(code="99004", Day_2_Forecast=None),              # missing text -> NULL + warning
        city_record(code=None),                                      # no station code -> rejected
        city_record(code="99005", Date="10/11/2026"),                # undocumented date format -> rejected
    ]), RETRIEVED)
    by_code = {}
    for r in result.records:
        by_code.setdefault(r.values["station_code"], []).append(r.values["lead_day"])
    assert by_code == {"99002": [1, 2, 3, 4, 5, 6], "99003": [1, 2, 4, 5, 6, 7], "99004": [1, 2, 3, 4, 5, 6, 7]}
    assert len(result.failures) == 3 and "INCOMPLETE_FORECAST" in codes(result) and "MISSING_VALUE" in codes(result)


# -------------------------------------------------------------------- client

def _client(mock: MockImd, **settings_kwargs) -> api.ImdClient:
    settings = Settings(log_to_file=False, imd_api_base_url=API, **{"http_max_retries": 0, **settings_kwargs})
    http = HttpClient(settings=settings, transport=httpx.MockTransport(mock), sleep=lambda _: None)
    return api.ImdClient(http, settings)


def test_key_is_sent_only_when_both_settings_are_configured() -> None:
    mock = MockImd()
    mock.set("/aws_data", [])
    _client(mock).get(api.AWS_DATA_PATH)
    _client(mock, imd_api_key="synthetic-secret-917").get(api.AWS_DATA_PATH)             # header name missing
    response = _client(mock, imd_api_key="synthetic-secret-917", imd_api_key_header="X-Test-Key").get(
        api.AWS_DATA_PATH, {"sid": 7})
    assert ["x-test-key" in h for h in mock.headers_seen] == [False, False, True]
    assert response.url == f"{API}/aws_data?sid=7" and "synthetic-secret-917" not in response.url


def test_access_denied_is_not_retried_and_server_errors_are() -> None:
    mock = MockImd()
    mock.set("/cityforecast", httpx.Response(403))
    with pytest.raises(ImdAccessDenied, match="not whitelisted"):
        _client(mock, http_max_retries=3).get(api.CITY_FORECAST_PATH)
    assert len(mock.calls) == 1
    mock.set("/cityforecast", httpx.Response(503), httpx.Response(503), [city_record()])
    response = _client(mock, http_max_retries=3).get(api.CITY_FORECAST_PATH)
    assert len(mock.calls) == 4 and json.loads(response.content)[0]["Station_Code"] == "99001"
    mock.set("/cityforecast", httpx.Response(500))
    with pytest.raises(HttpStatusError):
        _client(mock, http_max_retries=1).get(api.CITY_FORECAST_PATH)


def test_observation_lineage_never_includes_measurements() -> None:
    assert not set(ImdObservationPipeline.measurement_columns) & ImdObservationPipeline.extra_lineage_columns
    assert not set(ImdObservationPipeline.natural_key) & ImdObservationPipeline.extra_lineage_columns
    assert not {"max_temp_value", "min_temp_value", "forecast_text", "issue_date"} & \
        ImdCityForecastPipeline.extra_lineage_columns


def test_cli_rejects_invalid_state_id(tmp_path: Path) -> None:
    from scripts.ingest_imd import main
    assert main(["aws", "--state-id", "37"], settings=Settings(log_to_file=False, raw_data_dir=tmp_path)) == 2
    assert main(["city-forecast", "--state-id", "7"], settings=Settings(log_to_file=False,
                                                                       raw_data_dir=tmp_path)) == 2
