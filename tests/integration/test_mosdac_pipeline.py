"""MOSDAC end to end: mocked HTTP, real schema, every test rolled back.

Migration 0009_mosdac is NOT applied to the database; each test applies it inside its own outer
transaction (``tests/support/alembic_tx.py``) and rolls it back. Raw files go to a temporary
directory. No live MOSDAC calls.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.engine import Connection

from config.settings import PROJECT_ROOT, Settings
from src.common.checksums import sha256_bytes
from src.common.database import DatabaseManager, SavepointDatabase
from src.common.exceptions import ConfigurationError
from src.common.http_client import HttpClient
from src.common.raw_storage import METADATA_SUFFIX, LocalFileSystemBackend, RawStorageManager
from src.ingestion.models import RunStatus
from src.pipelines.mosdac.errors import MosdacLocationError
from src.pipelines.mosdac.runner import MosdacRunSummary, run_mosdac
from tests.support.alembic_tx import current_revision, migrate_in_transaction
from tests.support.mosdac import API, START, MockMosdac, body, server_error, solar_records, wind_records

pytestmark = pytest.mark.integration

TABLES = ("mosdac_solar_forecast", "mosdac_wind_forecast")
LOCATIONS_FILE = PROJECT_ROOT / "config" / "mosdac_locations.toml"


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
        if current_revision(connection) == "0008_imd":
            migrate_in_transaction(connection, "0009_mosdac")     # rolled back with the test
        for table in TABLES:
            connection.info[table] = connection.execute(text(f"SELECT coalesce(max(id), 0) FROM pi.{table}")).scalar()
        try:
            yield connection
        finally:
            outer.rollback()


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(raw_data_dir=tmp_path / "raw", log_to_file=False, mosdac_api_base_url=API, http_max_retries=1,
                    mosdac_request_pause_seconds=0, mosdac_locations_file=LOCATIONS_FILE)


@pytest.fixture
def storage(tmp_path: Path) -> RawStorageManager:
    return RawStorageManager(LocalFileSystemBackend(tmp_path / "raw"))


@pytest.fixture
def mock() -> MockMosdac:
    return MockMosdac()


def _run(conn: Connection, settings: Settings, storage: RawStorageManager, mock: MockMosdac, command: str,
         locations: list[str] | None = ("ESEPL",), **kwargs: Any) -> MosdacRunSummary:  # type: ignore[assignment]
    http = HttpClient(settings=settings, transport=httpx.MockTransport(mock), sleep=lambda _: None)
    return run_mosdac(command, locations=list(locations) if locations else None, settings=settings, http_client=http,
                      database=SavepointDatabase(conn), raw_storage=storage, sleep=lambda _: None, **kwargs)


def _rows(conn: Connection, table: str) -> list[Any]:
    return conn.execute(text(f"SELECT * FROM pi.{table} WHERE id > :b ORDER BY id"), {"b": conn.info[table]}).all()


def _issues(conn: Connection, run_id: int) -> list[str]:
    return [r[0] for r in conn.execute(text("SELECT issue_code FROM pi.data_quality_issue WHERE ingestion_run_id = :r"),
                                       {"r": run_id})]


# --------------------------------------------------------------------------- valid responses

def test_valid_solar_response_loads_with_full_lineage(conn, settings, storage, mock) -> None:
    mock.set("solar", solar_records())
    summary = _run(conn, settings, storage, mock, "solar")

    result = summary.runs[("ESEPL", "solar")]
    assert summary.ok and result.status is RunStatus.SUCCESS
    assert result.records_inserted == 289 and result.records_rejected == 0
    rows = _rows(conn, "mosdac_solar_forecast")
    assert len(rows) == 289
    first = rows[0]._mapping
    assert first["location_code"] == "ESEPL"
    assert (first["latitude"], first["longitude"]) == (Decimal("17.4042"), Decimal("77.3753"))
    assert first["source_time_text"] == "10-01-30 0530" and first["valid_at"] == first["series_start_at"]
    assert first["air_temperature_c"] == Decimal("18.117") and first["solar_flux_w_m2"] == Decimal("0.111")
    assert first["ingestion_run_id"] == result.ingestion_run_id and first["document_id"] == result.document_ids[0]
    assert first["availability_basis"] == "RETRIEVED_AT" and first["source_available_at"] == first["retrieved_at"]
    assert mock.calls == ["solar?lat=17.4042&lon=77.3753"]

    # Raw storage: original bytes, SHA-256, sidecar, source_document linkage, configured RAW_DATA_DIR.
    doc = conn.execute(text("SELECT * FROM pi.source_document WHERE document_id = :d"),
                       {"d": result.document_ids[0]}).one()._mapping
    path = Path(unquote(urlparse(doc["raw_storage_uri"]).path).lstrip("/"))
    assert path.is_relative_to(settings.raw_data_dir)
    assert doc["checksum_sha256"] == sha256_bytes(path.read_bytes())
    assert doc["source_document_url"].endswith("/apienergy/solar?lat=17.4042&lon=77.3753")
    assert doc["published_at"] is None and doc["retrieved_at"] == first["retrieved_at"]
    key = result.raw_storage_keys[0]
    assert key.startswith("mosdac/mosdac_solar_forecast/esepl/")
    # The registered file is the request-bound document; it embeds the exact response and its SHA-256.
    bound = json.loads(path.read_bytes())
    assert bound["request"] == {"endpoint": "solar", "url": doc["source_document_url"], "location_code": "ESEPL",
                                "latitude": "17.4042", "longitude": "77.3753"}
    assert bound["response_text"].encode() == body(solar_records())
    meta = json.loads(Path(str(path) + METADATA_SUFFIX).read_text())
    assert meta["sha256"] == doc["checksum_sha256"] and meta["metadata"]["ingestion_run_id"] == result.ingestion_run_id
    # The original response body is stored unchanged next to it and linked by key and SHA-256.
    original_key = meta["metadata"]["response_raw_key"]
    assert original_key.startswith("mosdac/mosdac_solar_forecast/esepl-response/")
    assert storage.read_bytes(original_key) == body(solar_records())
    assert sha256_bytes(storage.read_bytes(original_key)) == meta["metadata"]["response_sha256"] \
        == bound["response_sha256"] == storage.read_metadata(original_key)["sha256"]
    assert storage.read_metadata(original_key)["metadata"]["http_status"] == 200


def test_valid_wind_response(conn, settings, storage, mock) -> None:
    mock.set("wind", wind_records())
    summary = _run(conn, settings, storage, mock, "wind")

    assert summary.ok and summary.runs[("ESEPL", "wind")].records_inserted == 289
    row = _rows(conn, "mosdac_wind_forecast")[1]._mapping
    assert (row["wind_speed_value"], row["wind_direction_value"]) == (Decimal("2.328"), Decimal("7.424"))


# --------------------------------------------------------------------------- failures

def test_http_500_fails_the_run_after_limited_retries(conn, settings, storage, mock) -> None:
    mock.set("wind", server_error)
    summary = _run(conn, settings, storage, mock, "wind")

    result = summary.runs[("ESEPL", "wind")]
    assert result.status is RunStatus.FAILED and "500" in (result.error or "")
    assert len(mock.calls) == settings.http_max_retries + 1          # bounded; the fetch is not repeated on top
    assert not summary.ok and _rows(conn, "mosdac_wind_forecast") == []


def test_ssl_eof_fails_the_run_without_loading(conn, settings, storage, mock) -> None:
    mock.set("solar", httpx.ConnectError("[SSL: UNEXPECTED_EOF_WHILE_READING] EOF occurred in violation of protocol"))
    result = _run(conn, settings, storage, mock, "solar").runs[("ESEPL", "solar")]
    assert result.status is RunStatus.FAILED and "EOF" in (result.error or "")
    assert len(mock.calls) == 2 and _rows(conn, "mosdac_solar_forecast") == []


def test_timeout_fails_the_run(conn, settings, storage, mock) -> None:
    mock.set("solar", httpx.ReadTimeout("timed out"))
    result = _run(conn, settings, storage, mock, "solar").runs[("ESEPL", "solar")]
    assert result.status is RunStatus.FAILED and "ReadTimeout" in (result.error or "")


def test_malformed_json_fails_but_raw_body_is_kept(conn, settings, storage, mock) -> None:
    mock.set("solar", b"<html>maintenance</html>")
    result = _run(conn, settings, storage, mock, "solar").runs[("ESEPL", "solar")]

    assert result.status is RunStatus.FAILED and "not valid JSON" in (result.error or "")
    assert _rows(conn, "mosdac_solar_forecast") == []
    keys = storage.list_keys("mosdac", "mosdac_solar_forecast")
    originals = [k for k in keys if "/esepl-response/" in k]
    assert len(keys) == 2 and len(originals) == 1
    assert storage.read_bytes(originals[0]) == b"<html>maintenance</html>"


def test_unequal_array_lengths_fail_but_raw_body_is_kept(conn, settings, storage, mock) -> None:
    rows = solar_records(4)
    mock.set("solar", {"time": [r["time"] for r in rows], "temp": [r["temp"] for r in rows], "flux": ["1.0"]})
    result = _run(conn, settings, storage, mock, "solar").runs[("ESEPL", "solar")]
    assert result.status is RunStatus.FAILED and "unequal lengths" in (result.error or "")
    assert len(storage.list_keys("mosdac", "mosdac_solar_forecast")) == 2      # original + request-bound


def test_missing_fields_invalid_and_duplicate_timestamps_are_partial(conn, settings, storage, mock) -> None:
    records = solar_records(8)
    del records[0]["flux"]                                   # MISSING_FIELD -> NULL, loaded
    records[1]["time"] = "99-99-99 9999"                     # invalid timestamp -> rejected
    records[2]["temp"] = "abc"                               # non-numeric -> rejected
    records.append(dict(records[5]))                         # identical duplicate -> loaded once
    conflicting = dict(records[6])
    conflicting["flux"] = "999.999"
    records.append(conflicting)                              # conflicting duplicate -> both copies rejected
    mock.set("solar", records)
    result = _run(conn, settings, storage, mock, "solar").runs[("ESEPL", "solar")]

    assert result.status is RunStatus.PARTIAL
    rows = _rows(conn, "mosdac_solar_forecast")
    assert len(rows) == 5                                    # 8 - 2 rejected - record 6 (conflict)
    assert rows[0]._mapping["solar_flux_w_m2"] is None
    issues = _issues(conn, result.ingestion_run_id)
    for code in ("MISSING_FIELD", "PARSE_ERROR", "DUPLICATE_TIMESTAMP", "DUPLICATE_IN_BATCH",
                 "CONFLICTING_DUPLICATE_IN_BATCH"):
        assert code in issues, code
    rejects = storage.list_keys("mosdac", "mosdac_solar_forecast-rejected")
    assert len(rejects) == 1                                 # rejected source records kept as raw JSON
    assert len(json.loads(storage.read_bytes(rejects[0]))["rejected"]) >= 4


# --------------------------------------------------------------------------- reruns and vintages

def test_identical_response_rerun_is_idempotent(conn, settings, storage, mock) -> None:
    mock.set("solar", solar_records())
    first = _run(conn, settings, storage, mock, "solar").runs[("ESEPL", "solar")]
    second = _run(conn, settings, storage, mock, "solar").runs[("ESEPL", "solar")]

    assert first.records_inserted == 289
    assert second.status is RunStatus.SUCCESS and second.duplicate_documents == 1 and second.records_inserted == 0
    assert second.document_ids == first.document_ids
    assert len(_rows(conn, "mosdac_solar_forecast")) == 289
    assert "DUPLICATE_DOCUMENT" in _issues(conn, second.ingestion_run_id)
    # The re-fetch is still preserved as its own write-once raw files (original + request-bound, twice).
    assert len(storage.list_keys("mosdac", "mosdac_solar_forecast")) == 4


def test_identical_bodies_for_two_plants_both_load(conn, settings, storage, mock) -> None:
    """The response does not name its location: identical bodies must not make the second plant a duplicate."""
    mock.set("solar", solar_records(5))
    summary = _run(conn, settings, storage, mock, "solar", locations=["ESEPL", "NSPL"])

    assert summary.ok
    assert summary.runs[("NSPL", "solar")].duplicate_documents == 0
    assert summary.runs[("NSPL", "solar")].records_inserted == 5
    assert summary.runs[("ESEPL", "solar")].document_ids != summary.runs[("NSPL", "solar")].document_ids


def test_revised_forecast_vintages_never_overwrite(conn, settings, storage, mock) -> None:
    original = solar_records()
    revised = [dict(r) for r in original]
    for i in (10, 11, 12):
        revised[i]["flux"] = f"{float(revised[i]['flux']) + 1.111:.3f}"
    from datetime import timedelta
    next_series = solar_records(start=START + timedelta(hours=6), offset=0.5)   # a later series (new vintage)
    mock.set("solar", original, revised, next_series)

    first = _run(conn, settings, storage, mock, "solar").runs[("ESEPL", "solar")]
    second = _run(conn, settings, storage, mock, "solar").runs[("ESEPL", "solar")]
    third = _run(conn, settings, storage, mock, "solar").runs[("ESEPL", "solar")]

    assert first.records_inserted == 289
    assert second.records_inserted == 3 and second.records_unchanged == 286 and second.records_updated == 0
    assert third.records_inserted == 289
    rows = [r._mapping for r in _rows(conn, "mosdac_solar_forecast")]
    assert len(rows) == 289 + 3 + 289
    target = rows[10]["valid_at"]
    versions = [r for r in rows if r["valid_at"] == target and r["series_start_at"] == rows[0]["series_start_at"]]
    assert [v["solar_flux_w_m2"] for v in versions] == [Decimal(original[10]["flux"]), Decimal(revised[10]["flux"])]
    assert versions[0]["ingestion_run_id"] != versions[1]["ingestion_run_id"]
    # The same valid time also exists in the later series, as a separate vintage.
    later = [r for r in rows if r["series_start_at"] != rows[0]["series_start_at"]]
    assert len({r["series_start_at"] for r in later}) == 1 and len(later) == 289


# --------------------------------------------------------------------------- independence

def test_solar_succeeds_while_wind_fails(conn, settings, storage, mock) -> None:
    mock.set("solar", solar_records())
    mock.set("wind", server_error)
    summary = _run(conn, settings, storage, mock, "all")

    assert summary.runs[("ESEPL", "solar")].status is RunStatus.SUCCESS
    assert summary.runs[("ESEPL", "wind")].status is RunStatus.FAILED
    assert not summary.ok
    assert len(_rows(conn, "mosdac_solar_forecast")) == 289 and _rows(conn, "mosdac_wind_forecast") == []


def test_all_eight_plants_both_endpoints(conn, settings, storage, mock) -> None:
    mock.set("solar", solar_records(5))
    mock.set("wind", wind_records(5))
    summary = _run(conn, settings, storage, mock, "all", locations=None)

    assert summary.ok and len(summary.runs) == 16
    assert {code for code, _ in summary.runs} == {"ESEPL", "NSPL", "ASPL", "GSPL_GAP", "GSPL_GUM", "PSPL", "RSPL",
                                                  "TSPL"}
    assert "solar?lat=12.0981737&lon=76.8406883" in mock.calls and "wind?lat=16.6243&lon=78.0630" in mock.calls
    stored = {(r._mapping["location_code"], r._mapping["latitude"], r._mapping["longitude"])
              for r in _rows(conn, "mosdac_solar_forecast")}
    assert ("NSPL", Decimal("12.0981737"), Decimal("76.8406883")) in stored and len(stored) == 8
    assert len(_rows(conn, "mosdac_wind_forecast")) == 40
    keys = storage.list_keys("mosdac", "mosdac_wind_forecast")
    assert any(k.startswith("mosdac/mosdac_wind_forecast/gspl_gap/") for k in keys)


def test_moved_location_is_refused_others_continue(conn, settings, storage, mock, tmp_path) -> None:
    mock.set("solar", solar_records(3))
    _run(conn, settings, storage, mock, "solar")
    moved = tmp_path / "moved.toml"
    moved.write_text(LOCATIONS_FILE.read_text().replace("latitude = 17.4042", "latitude = 17.5"))
    summary = _run(conn, settings.model_copy(update={"mosdac_locations_file": moved}), storage, mock, "solar",
                   locations=["ESEPL", "NSPL"])
    assert "ESEPL" in summary.location_errors and ("ESEPL", "solar") not in summary.runs
    assert summary.runs[("NSPL", "solar")].status is RunStatus.SUCCESS and not summary.ok


def test_unknown_location_is_refused_before_any_request(conn, settings, storage, mock) -> None:
    with pytest.raises(MosdacLocationError, match="NOPE"):
        _run(conn, settings, storage, mock, "solar", locations=["NOPE"])
    assert mock.calls == []


def test_refuses_without_migration(manager, settings, storage, mock) -> None:
    with manager.engine.connect() as connection:
        outer = connection.begin()
        try:
            if current_revision(connection) != "0008_imd":
                pytest.skip("0009_mosdac already applied")
            with pytest.raises(ConfigurationError, match="0009_mosdac"):
                _run(connection, settings, storage, mock, "solar")
            assert mock.calls == []
        finally:
            outer.rollback()
