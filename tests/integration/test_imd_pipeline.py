"""IMD end to end: mocked HTTP, real schema, every test rolled back.

Migration 0008_imd is NOT applied to the database yet; each test applies it inside its own outer
transaction (``tests/support/alembic_tx.py``) and rolls it back, so the tests exercise the real
schema without changing it. No live IMD calls.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import date, time
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.engine import Connection

from config.settings import Settings
from src.common.checksums import sha256_bytes
from src.common.database import DatabaseManager, SavepointDatabase
from src.common.exceptions import ConfigurationError
from src.common.http_client import HttpClient
from src.common.raw_storage import LocalFileSystemBackend, RawStorageManager
from src.ingestion.models import RunStatus
from src.pipelines.imd.runner import ImdRunSummary, run_imd
from tests.support.alembic_tx import current_revision, migrate_in_transaction
from tests.support.imd import API, OBSERVATIONS, STATIONS, MockImd, aws_record, city_record

pytestmark = pytest.mark.integration

TABLES = ("imd_station", "imd_station_observation", "imd_city_forecast")
ID_COLUMN = {"imd_station": "imd_station_id", "imd_station_observation": "id", "imd_city_forecast": "id"}


@pytest.fixture(scope="module")
def manager() -> Iterator[DatabaseManager]:
    try:
        db = DatabaseManager.from_settings(Settings())
    except ConfigurationError as exc:
        pytest.skip(f"PostgreSQL not configured: {exc}")
    yield db
    db.dispose()


@pytest.fixture
def conn(manager: DatabaseManager) -> Iterator[Connection]:
    with manager.engine.connect() as connection:
        outer = connection.begin()
        if current_revision(connection) == "0007_wbes_px":
            migrate_in_transaction(connection, "0008_imd")       # rolled back with the test
        for table in TABLES:
            connection.info[table] = connection.execute(text(
                f"SELECT coalesce(max({ID_COLUMN[table]}), 0) FROM pi.{table}")).scalar()
        try:
            yield connection
        finally:
            outer.rollback()


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(raw_data_dir=tmp_path / "raw", log_to_file=False, imd_api_base_url=API, http_max_retries=0,
                    imd_request_pause_seconds=0)


@pytest.fixture
def storage(tmp_path: Path) -> RawStorageManager:
    return RawStorageManager(LocalFileSystemBackend(tmp_path / "raw"))


@pytest.fixture
def mock() -> MockImd:
    return MockImd()


def _run(conn: Connection, settings: Settings, storage: RawStorageManager, mock: MockImd, command: str,
         **kwargs: Any) -> ImdRunSummary:
    http = HttpClient(settings=settings, transport=httpx.MockTransport(mock), sleep=lambda _: None)
    return run_imd(command, settings=settings, http_client=http, database=SavepointDatabase(conn),
                   raw_storage=storage, sleep=lambda _: None, **kwargs)


def _rows(conn: Connection, table: str) -> list[Any]:
    return conn.execute(text(f"SELECT * FROM pi.{table} WHERE {ID_COLUMN[table]} > :b ORDER BY {ID_COLUMN[table]}"),
                        {"b": conn.info[table]}).all()


def _issues(conn: Connection, run_id: int) -> list[tuple[str, str]]:
    return [tuple(r) for r in conn.execute(text(
        "SELECT issue_code, severity FROM pi.data_quality_issue WHERE ingestion_run_id = :r ORDER BY 1, 2"),
        {"r": run_id})]


def _raw_files(settings: Settings) -> list[Path]:
    return sorted(p for p in Path(settings.raw_data_dir).rglob("*.json") if not p.name.endswith(".meta.json"))


# ----------------------------------------------------------------------- AWS

def test_aws_stations_then_observations_with_entity_mapping(conn, settings, storage, mock) -> None:
    mock.set("/aws_data_mapping", STATIONS)
    mock.set("/aws_data", OBSERVATIONS)

    summary = _run(conn, settings, storage, mock, "aws")

    assert summary.ok and mock.calls == ["/aws_data_mapping", "/aws_data"]
    stations, observations = summary.runs["imd_station"], summary.runs["imd_station_observation"]
    assert (stations.records_inserted, observations.records_inserted) == (2, 3)
    station_rows = {r.call_sign: r for r in _rows(conn, "imd_station")}
    assert (station_rows["TSA"].state_code, station_rows["TSB"].state_code) == ("DL", "DH")
    obs = {r.call_sign: r for r in _rows(conn, "imd_station_observation")}
    assert obs["TSA"].imd_station_id == station_rows["TSA"].imd_station_id
    assert obs["TSX"].imd_station_id is None                                  # unknown station kept, not discarded
    assert ("UNKNOWN_STATION", "WARNING") in _issues(conn, observations.ingestion_run_id)
    a = obs["TSA"]
    assert (a.observation_date, a.observation_time, a.observed_at, a.curr_temp_value, a.feels_like_value) == (
        date(2026, 11, 10), time(7), None, Decimal("23.417"), Decimal("24.193"))
    assert (a.availability_basis, a.source_published_at) == ("RETRIEVED_AT", None) and a.source_available_at == a.retrieved_at
    # Raw files are the bodies as received; source_document checksums match them and their metadata.
    files = _raw_files(settings)
    assert len(files) == 2
    for run, payload in ((stations, STATIONS), (observations, OBSERVATIONS)):
        doc = conn.execute(text("SELECT d.checksum_sha256, d.raw_storage_uri, d.ingestion_run_id, s.dataset_code "
                                "FROM pi.source_document d JOIN pi.source_dataset s USING (dataset_id) "
                                "WHERE d.document_id = :d"), {"d": run.document_ids[0]}).one()
        raw = next(f for f in files if f.as_uri() == doc.raw_storage_uri).read_bytes()
        assert sha256_bytes(raw) == doc.checksum_sha256
        assert json.loads(Path(str(next(f for f in files if f.as_uri() == doc.raw_storage_uri)) + ".meta.json")
                          .read_text())["sha256"] == doc.checksum_sha256
        assert (doc.ingestion_run_id, doc.dataset_code) == (run.ingestion_run_id, "IMD_AWS_OBS")
        assert len(json.loads(raw)) == len(payload)


def test_duplicate_observations_in_one_response(conn, settings, storage, mock) -> None:
    mock.set("/aws_data_mapping", STATIONS)
    mock.set("/aws_data", [aws_record(), aws_record(), aws_record(ID="SYNB0002", CALL_SIGN="TSB"),
                           aws_record(ID="SYNB0002", CALL_SIGN="TSB", CURR_TEMP=31.917)])
    observations = _run(conn, settings, storage, mock, "aws").runs["imd_station_observation"]
    assert observations.status is RunStatus.PARTIAL and observations.records_inserted == 1
    issues = _issues(conn, observations.ingestion_run_id)
    assert ("DUPLICATE_IN_BATCH", "INFO") in issues and issues.count(("CONFLICTING_DUPLICATE_IN_BATCH", "ERROR")) == 2
    assert [r.call_sign for r in _rows(conn, "imd_station_observation")] == ["TSA"]


def test_revised_observation_is_a_new_version_context_change_is_not(conn, settings, storage, mock) -> None:
    mock.set("/aws_data_mapping", STATIONS)
    mock.set("/aws_data", OBSERVATIONS)
    assert _run(conn, settings, storage, mock, "aws").ok
    revised = [aws_record(CURR_TEMP=23.917),                                        # measurement revised
               aws_record(ID="SYNB0002", CALL_SIGN="TSB", STATION="RENAMED STATION B", STATE="DAMAN_AND_DIU",
                          CURR_TEMP=29.613, Latitude=20.417, Longitude=72.837)]     # context only
    mock.set("/aws_data", revised)
    second = _run(conn, settings, storage, mock, "aws").runs["imd_station_observation"]
    assert (second.records_inserted, second.records_unchanged) == (1, 1)
    tsa = [r.curr_temp_value for r in _rows(conn, "imd_station_observation") if r.call_sign == "TSA"]
    assert tsa == [Decimal("23.417"), Decimal("23.917")]                            # both versions kept


def test_idempotent_rerun(conn, settings, storage, mock) -> None:
    mock.set("/aws_data_mapping", STATIONS)
    mock.set("/aws_data", OBSERVATIONS)
    assert _run(conn, settings, storage, mock, "aws").ok
    again = _run(conn, settings, storage, mock, "aws")
    assert all(r.records_inserted == 0 and r.duplicate_documents == 1 for r in again.runs.values())
    assert (len(_rows(conn, "imd_station")), len(_rows(conn, "imd_station_observation"))) == (2, 3)


def test_state_filter_and_empty_response(conn, settings, storage, mock) -> None:
    mock.set("/aws_data_mapping", STATIONS)
    mock.set("/aws_data", [])
    summary = _run(conn, settings, storage, mock, "aws", state_id=7)
    assert summary.ok and mock.calls[-1] == "/aws_data?sid=7"
    assert ("NO_DATA", "INFO") in _issues(conn, summary.runs["imd_station_observation"].ingestion_run_id)


# ------------------------------------------------------------------ forecasts

def test_city_forecast_vintages_are_kept_separately(conn, settings, storage, mock) -> None:
    mock.set("/cityforecast", [city_record("2026-11-10")])
    first = _run(conn, settings, storage, mock, "city-forecast").runs["imd_city_forecast"]
    mock.set("/cityforecast", [city_record("2026-11-11", Day_2_Max_Temp=31.713)])
    second = _run(conn, settings, storage, mock, "city-forecast").runs["imd_city_forecast"]
    assert (first.records_inserted, second.records_inserted) == (7, 7)
    rows = _rows(conn, "imd_city_forecast")
    target = [(r.issue_date, r.lead_day, r.max_temp_value) for r in rows if r.forecast_date == date(2026, 11, 12)]
    assert target == [(date(2026, 11, 10), 3, Decimal("29.470")), (date(2026, 11, 11), 2, Decimal("31.713"))]
    # The same issue re-fetched with one changed day: a new version of that row only; older rows untouched.
    mock.set("/cityforecast", [city_record("2026-11-10", Day_3_Max_Temp=30.117)])
    third = _run(conn, settings, storage, mock, "city-forecast").runs["imd_city_forecast"]
    assert (third.records_inserted, third.records_unchanged) == (1, 6)
    versions = [r.max_temp_value for r in _rows(conn, "imd_city_forecast")
                if (r.issue_date, r.lead_day) == (date(2026, 11, 10), 3)]
    assert versions == [Decimal("29.470"), Decimal("30.117")]
    assert ("OBSERVED_FIELDS_NOT_LOADED", "INFO") in _issues(conn, first.ingestion_run_id)


def test_incomplete_and_invalid_forecasts_are_reported(conn, settings, storage, mock) -> None:
    partial = {k: v for k, v in city_record(code="99002").items() if not k.startswith("Day_7")}
    mock.set("/cityforecast", [partial, city_record(code="99003", Day_4_Min_temp="cold")])
    run = _run(conn, settings, storage, mock, "city-forecast").runs["imd_city_forecast"]
    assert run.status is RunStatus.PARTIAL and (run.records_inserted, run.records_rejected) == (12, 1)
    issues = _issues(conn, run.ingestion_run_id)
    assert ("INCOMPLETE_FORECAST", "WARNING") in issues and ("PARSE_ERROR", "ERROR") in issues


# --------------------------------------------------------------------- errors

def test_access_denied_fails_the_runs_without_data(conn, settings, storage, mock) -> None:
    mock.set("/aws_data_mapping", httpx.Response(403))
    mock.set("/aws_data", httpx.Response(403))
    summary = _run(conn, settings, storage, mock, "aws")
    assert all(r.status is RunStatus.FAILED and "not whitelisted" in r.error for r in summary.runs.values())
    assert _rows(conn, "imd_station") == [] and _raw_files(settings) == []


def test_transient_errors_are_retried(conn, settings, storage, mock) -> None:
    retrying = settings.model_copy(update={"http_max_retries": 2})
    mock.set("/cityforecast", httpx.Response(503), [city_record()])
    run = _run(conn, retrying, storage, mock, "city-forecast").runs["imd_city_forecast"]
    assert run.status is RunStatus.SUCCESS and run.records_inserted == 7 and len(mock.calls) == 2


def test_unrecognised_structure_fails_but_raw_is_kept(conn, settings, storage, mock) -> None:
    mock.set("/cityforecast", {"forecasts": [city_record()]})               # not the documented list
    run = _run(conn, settings, storage, mock, "city-forecast").runs["imd_city_forecast"]
    assert run.status is RunStatus.FAILED and "expected a JSON list" in run.error
    assert len(_raw_files(settings)) == 1 and _rows(conn, "imd_city_forecast") == []


def test_commands_refuse_to_run_without_the_migration(manager, settings, storage, mock) -> None:
    with manager.engine.connect() as connection:
        outer = connection.begin()
        try:
            if current_revision(connection) != "0007_wbes_px":
                pytest.skip("migration 0008_imd is applied; nothing to check")
            with pytest.raises(ConfigurationError, match="0008_imd is not applied"):
                _run(connection, settings, storage, mock, "city-forecast")
            assert mock.calls == []                                           # refused before any request
        finally:
            outer.rollback()
