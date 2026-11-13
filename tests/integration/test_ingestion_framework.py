"""End-to-end tests of the ingestion framework against the real ``pi`` schema.

Uses the mock JSON dataset in tests/fixtures. Every test runs inside one outer
transaction that is rolled back, so no rows persist in the database (identity
sequences may advance, which is harmless). Raw files go to a temp directory.
No external sources are contacted.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Connection

from config.settings import Settings
from src.common.checksums import sha256_bytes, sha256_file
from src.common.database import DatabaseManager
from src.common.exceptions import ConfigurationError, HttpRequestError, IngestionError
from src.common.raw_storage import LocalFileSystemBackend, RawStorageManager
from src.ingestion.models import RunStatus, SourceDefinition
from src.ingestion.repositories import IngestionRepository
from tests.support.mock_pipeline import (
    MOCK_AREA,
    MOCK_DATASET,
    MOCK_MARKET,
    MockPricePipeline,
    SavepointDatabase,
    load_fixture,
    modified_fixture,
)

pytestmark = pytest.mark.integration

# As stated in the mock payload (IST); compared as instants, whatever the session time zone.
SOURCE_PUBLISHED_AT = datetime.fromisoformat("2026-11-09T13:05:00+05:30")


@pytest.fixture(scope="module")
def settings() -> Settings:
    return Settings()


@pytest.fixture(scope="module")
def manager(settings: Settings) -> Iterator[DatabaseManager]:
    try:
        db = DatabaseManager.from_settings(settings)
    except ConfigurationError as exc:
        pytest.skip(f"PostgreSQL not configured: {exc}")
    yield db
    db.dispose()


@pytest.fixture
def conn(manager: DatabaseManager) -> Iterator[Connection]:
    """Outer transaction, always rolled back. Seeds mock market reference data."""
    with manager.engine.connect() as connection:
        outer = connection.begin()
        try:
            connection.execute(text("INSERT INTO pi.market_master (market_code, market_name, market_operator) "
                                    "VALUES (:m, 'Mock DAM (tests)', 'Mock operator')"), {"m": MOCK_MARKET})
            connection.execute(text("INSERT INTO pi.market_area (area_code, area_name) "
                                    "VALUES (:a, 'Mock area (tests)')"), {"a": MOCK_AREA})
            yield connection
        finally:
            outer.rollback()


@pytest.fixture
def storage(tmp_path: Path) -> RawStorageManager:
    return RawStorageManager(LocalFileSystemBackend(tmp_path / "raw_data"))


def _pipeline(conn: Connection, storage: RawStorageManager, settings: Settings, payloads: Any,
              **kwargs: Any) -> MockPricePipeline:
    return MockPricePipeline(payloads, database=SavepointDatabase(conn), raw_storage=storage,
                             settings=settings, sleep=lambda _: None, **kwargs)


def _scalar(conn: Connection, sql: str, **params: Any) -> Any:
    return conn.execute(text(sql), params).scalar_one()


def _prices(conn: Connection) -> list[tuple[int, Decimal, int]]:
    return [tuple(r) for r in conn.execute(text(
        "SELECT time_block, price_inr_per_mwh, ingestion_run_id FROM pi.iex_market_price "
        "WHERE market_code = :m ORDER BY time_block, id"), {"m": MOCK_MARKET})]


def _issues(conn: Connection, run_id: int) -> list[tuple[str, str, str | None]]:
    return [tuple(r) for r in conn.execute(text(
        "SELECT issue_code, severity, source_record_key FROM pi.data_quality_issue "
        "WHERE ingestion_run_id = :r ORDER BY issue_code, source_record_key"), {"r": run_id})]


# --------------------------------------------------------------- first run


def test_first_run_loads_valid_records_and_tracks_everything(
    conn: Connection, storage: RawStorageManager, settings: Settings
) -> None:
    payload = load_fixture()

    result = _pipeline(conn, storage, settings, [payload]).run({"delivery_date": "2026-11-10"})

    # Run summary: 9 read; 1 parse failure + 2 validation errors rejected;
    # 6 accepted of which 1 is an identical in-batch duplicate -> 5 inserted.
    assert result.status is RunStatus.PARTIAL
    assert (result.records_read, result.records_inserted, result.records_rejected) == (9, 5, 3)
    assert result.records_unchanged == 1

    run = conn.execute(text("SELECT run_status, records_read, records_inserted, records_rejected, completed_at, "
                            "source_published_at, pipeline_version, parameters FROM pi.ingestion_run "
                            "WHERE ingestion_run_id = :r"), {"r": result.ingestion_run_id}).one()
    assert (run.run_status, run.records_read, run.records_inserted, run.records_rejected) == ("PARTIAL", 9, 5, 3)
    assert run.completed_at is not None and run.pipeline_version == "mock-1.0"
    assert run.parameters == {"delivery_date": "2026-11-10"}
    assert run.source_published_at == SOURCE_PUBLISHED_AT

    # Values are preserved exactly; precision beyond numeric(16,4) is flagged.
    assert [(b, p) for b, p, _ in _prices(conn)] == [
        (1, Decimal("3500.1250")), (2, Decimal("3600.0000")), (3, Decimal("3725.5000")),
        (6, Decimal("10500.0000")), (7, Decimal("3999.1235")),
    ]

    # Every rejected or flagged record is in data_quality_issue.
    codes = [(code, sev) for code, sev, _ in _issues(conn, result.ingestion_run_id)]
    assert sorted(codes) == sorted([
        ("PARSE_ERROR", "ERROR"), ("OUT_OF_RANGE", "ERROR"), ("OUT_OF_RANGE", "ERROR"),
        ("OUT_OF_RANGE", "WARNING"), ("NUMERIC_PRECISION_LOSS", "WARNING"), ("DUPLICATE_IN_BATCH", "INFO"),
    ])
    description = _scalar(conn, "SELECT issue_description FROM pi.data_quality_issue WHERE ingestion_run_id = :r "
                                "AND issue_code = 'PARSE_ERROR'", r=result.ingestion_run_id)
    assert '"not-a-number"' in description  # original value retained


def test_document_metadata_raw_file_and_timestamps(
    conn: Connection, storage: RawStorageManager, settings: Settings
) -> None:
    payload = load_fixture()

    result = _pipeline(conn, storage, settings, [payload]).run()

    document = conn.execute(text(
        "SELECT checksum_sha256, raw_storage_uri, published_at, retrieved_at, content_type, document_name, "
        "source_report_date, ingestion_run_id FROM pi.source_document WHERE document_id = :d"),
        {"d": result.document_ids[0]}).one()
    raw_key = result.raw_storage_keys[0]
    raw_path = storage.backend.base_dir / raw_key

    # Checksum matches both the original payload and the stored file.
    assert document.checksum_sha256 == sha256_bytes(payload) == sha256_file(raw_path)
    assert raw_path.read_bytes() == payload
    assert document.raw_storage_uri == raw_path.as_uri()
    assert raw_key.startswith("mock_exchange/mock_dam_prices/")
    assert document.ingestion_run_id == result.ingestion_run_id
    assert (document.content_type, document.document_name) == ("application/json", "mock_dam_2026-11-10.json")
    assert str(document.source_report_date) == "2026-11-10"

    # Publication time (from the source) and retrieval time (ours) are distinct.
    assert document.published_at == SOURCE_PUBLISHED_AT
    assert document.retrieved_at > document.published_at
    row = conn.execute(text("SELECT retrieved_at, source_published_at FROM pi.iex_market_price "
                            "WHERE market_code = :m AND time_block = 1"), {"m": MOCK_MARKET}).one()
    assert row.retrieved_at == document.retrieved_at
    assert row.source_published_at == document.published_at

    # Rejected records are preserved in full in raw storage.
    rejects_key = next(k for k in result.raw_storage_keys if "-rejected/" in k)
    rejects = storage.read_json(rejects_key)
    assert {entry["stage"] for entry in rejects["rejected"]} == {"parse", "validation"}
    assert {"block": 5, "price": "not-a-number"} in [entry["raw"] for entry in rejects["rejected"]]


# ------------------------------------------------------- idempotency


def test_same_document_is_not_loaded_twice(conn: Connection, storage: RawStorageManager, settings: Settings) -> None:
    payload = load_fixture()
    first = _pipeline(conn, storage, settings, [payload]).run()

    second = _pipeline(conn, storage, settings, [payload]).run()

    assert second.status is RunStatus.SUCCESS
    assert second.duplicate_documents == 1 and second.records_inserted == 0
    assert second.document_ids == first.document_ids
    assert len(_prices(conn)) == 5
    assert _issues(conn, second.ingestion_run_id) == [("DUPLICATE_DOCUMENT", "INFO", sha256_bytes(payload))]
    # Both retrievals were kept in raw storage; nothing was deleted.
    assert len(storage.list_keys("mock_exchange", "mock_dam_prices")) == 2


def test_revised_document_only_loads_changes(conn: Connection, storage: RawStorageManager, settings: Settings) -> None:
    first = _pipeline(conn, storage, settings, [load_fixture()]).run()
    revised = modified_fixture(prices=[
        {"block": 1, "price": Decimal("3550.00")},     # changed -> new version
        {"block": 2, "price": Decimal("3600.00")},     # unchanged
        {"block": 3, "price": Decimal("3725.50")},     # unchanged (same value, different formatting)
        {"block": 8, "price": Decimal("3900.00")},     # new
    ])

    second = _pipeline(conn, storage, settings, [revised]).run()

    assert second.status is RunStatus.SUCCESS
    assert (second.records_inserted, second.records_unchanged, second.records_rejected) == (2, 2, 0)
    block_one = [(p, r) for b, p, r in _prices(conn) if b == 1]
    assert block_one == [(Decimal("3500.1250"), first.ingestion_run_id),
                         (Decimal("3550.0000"), second.ingestion_run_id)]


# ------------------------------------------------ transactions & failure


def test_load_failure_rolls_back_document_but_keeps_raw_file(
    conn: Connection, storage: RawStorageManager, settings: Settings
) -> None:
    # Unknown market violates the foreign key -> the document transaction fails.
    bad = modified_fixture(market="UNKNOWN_MARKET")

    result = _pipeline(conn, storage, settings, [bad]).run()

    assert result.status is RunStatus.FAILED
    assert "ForeignKeyViolation" in (result.error or "")
    run = conn.execute(text("SELECT run_status, error_message, records_inserted FROM pi.ingestion_run "
                            "WHERE ingestion_run_id = :r"), {"r": result.ingestion_run_id}).one()
    assert run.run_status == "FAILED" and "iex_market_price" in run.error_message and run.records_inserted == 0
    assert _scalar(conn, "SELECT count(*) FROM pi.source_document WHERE ingestion_run_id = :r",
                   r=result.ingestion_run_id) == 0
    assert _scalar(conn, "SELECT count(*) FROM pi.data_quality_issue WHERE ingestion_run_id = :r",
                   r=result.ingestion_run_id) == 0
    assert _scalar(conn, "SELECT count(*) FROM pi.iex_market_price WHERE area_code = :a", a=MOCK_AREA) == 0
    assert (storage.backend.base_dir / result.raw_storage_keys[0]).read_bytes() == bad


def test_transient_fetch_failure_is_retried(conn: Connection, storage: RawStorageManager, settings: Settings) -> None:
    attempts: list[int] = []

    def flaky_source() -> list[bytes]:
        attempts.append(1)
        if len(attempts) == 1:
            raise HttpRequestError("GET mock://dam-prices failed: ConnectTimeout")
        return [load_fixture()]

    result = _pipeline(conn, storage, settings, flaky_source).run()

    assert len(attempts) == 2
    assert result.status is RunStatus.PARTIAL and result.records_inserted == 5


def test_persistent_fetch_failure_marks_run_failed(
    conn: Connection, storage: RawStorageManager, settings: Settings
) -> None:
    def down() -> list[bytes]:
        raise HttpRequestError("GET mock://dam-prices failed: ConnectError")

    result = _pipeline(conn, storage, settings, down).run()

    assert result.status is RunStatus.FAILED
    assert _scalar(conn, "SELECT run_status FROM pi.ingestion_run WHERE ingestion_run_id = :r",
                   r=result.ingestion_run_id) == "FAILED"


# ------------------------------------------------------- registration


def test_registration_is_idempotent_and_never_modifies_existing_rows(conn: Connection) -> None:
    repo = IngestionRepository("pi")
    before = conn.execute(text("SELECT source_id, source_name, source_url FROM pi.data_source "
                               "WHERE source_code = 'IEX'")).one()

    source_id = repo.ensure_source(conn, SourceDefinition("IEX", "A different name", "https://other.invalid/"))

    assert source_id == before.source_id
    assert conn.execute(text("SELECT source_id, source_name, source_url FROM pi.data_source "
                             "WHERE source_code = 'IEX'")).one() == before

    mock_id = repo.ensure_source(conn, SourceDefinition("MOCK_EXCHANGE", "Mock"))
    assert repo.ensure_source(conn, SourceDefinition("MOCK_EXCHANGE", "Mock")) == mock_id
    dataset_id = repo.ensure_dataset(conn, mock_id, MOCK_DATASET)
    assert repo.ensure_dataset(conn, mock_id, MOCK_DATASET) == dataset_id
    with pytest.raises(ConfigurationError):
        repo.ensure_dataset(conn, before.source_id, MOCK_DATASET)  # registered under another source


def test_inactive_dataset_is_refused(conn: Connection, storage: RawStorageManager, settings: Settings) -> None:
    repo = IngestionRepository("pi")
    source_id = repo.ensure_source(conn, MockPricePipeline.source)
    dataset_id = repo.ensure_dataset(conn, source_id, MOCK_DATASET)
    conn.execute(text("UPDATE pi.source_dataset SET is_active = false WHERE dataset_id = :d"), {"d": dataset_id})

    with pytest.raises(IngestionError, match="inactive"):
        _pipeline(conn, storage, settings, [load_fixture()]).run()
    assert _scalar(conn, "SELECT count(*) FROM pi.ingestion_run WHERE dataset_id = :d", d=dataset_id) == 0


def test_raw_payload_can_be_stored_in_db(conn: Connection, storage: RawStorageManager, settings: Settings) -> None:
    class StoringPipeline(MockPricePipeline):
        store_raw_payload_in_db = True

    result = StoringPipeline([load_fixture()], database=SavepointDatabase(conn), raw_storage=storage,
                             settings=settings).run()

    payload = _scalar(conn, "SELECT raw_payload FROM pi.source_document WHERE document_id = :d",
                      d=result.document_ids[0])
    assert payload["market"] == MOCK_MARKET and len(payload["prices"]) == 9
    assert json.loads(load_fixture())["delivery_date"] == payload["delivery_date"]
