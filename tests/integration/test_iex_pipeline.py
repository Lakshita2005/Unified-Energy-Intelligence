"""End-to-end IEX tests against the real ``pi`` schema with a mocked API.

Every test runs inside one outer transaction that is rolled back (nothing
persists, including reference rows seeded by the fixture). Assertions only
see rows created inside that transaction. No live API calls.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
import pytest
from sqlalchemy import inspect, text
from sqlalchemy.engine import Connection

from config.settings import Settings
from scripts.ingest_iex import main as cli_main
from src.common.checksums import sha256_bytes
from src.common.database import DatabaseManager, SavepointDatabase
from src.common.exceptions import ConfigurationError
from src.common.http_client import HttpClient
from src.common.raw_storage import LocalFileSystemBackend, RawStorageManager
from src.ingestion.models import RunStatus
from src.loaders.table_loader import TableLoader
from src.pipelines.iex import api
from src.pipelines.iex.area_states import ROWS as AREA_STATE_ROWS
from src.pipelines.iex.area_states import seed_area_states
from src.pipelines.iex.reference import AREA_ROWS, MARKET_ROWS, missing_reference, seed_reference
from src.pipelines.iex.runner import RunSummary, backfill_snapshots, run_iex
from tests.support.iex import DAY, MockIex, area_prices, dam_record, envelope, router, snapshot

pytestmark = pytest.mark.integration

TODAY = date(2026, 11, 12)


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
        connection.info["iex_baseline_id"] = connection.execute(
            text("SELECT coalesce(max(id), 0) FROM pi.iex_market_price")).scalar_one()
        seed_reference(connection, "pi")
        try:
            yield connection
        finally:
            outer.rollback()


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(raw_data_dir=tmp_path / "raw", log_to_file=False, iex_api_base_url="https://iex.test/api/v1")


@pytest.fixture
def storage(tmp_path: Path) -> RawStorageManager:
    return RawStorageManager(LocalFileSystemBackend(tmp_path / "raw"))


def _run(conn: Connection, settings: Settings, storage: RawStorageManager, mock: MockIex, kind: str = "dam",
         start: date = DAY, end: date | None = None, today: date = TODAY) -> RunSummary:
    client = HttpClient(settings=settings, transport=httpx.MockTransport(mock), sleep=lambda _: None)
    return run_iex(kind, start_date=start, end_date=end, settings=settings, http_client=client,  # type: ignore[arg-type]
                   database=SavepointDatabase(conn), raw_storage=storage, today=today, sleep=lambda _: None)


def _rows(conn: Connection, area: str | None = None) -> list[Any]:
    sql = "SELECT * FROM pi.iex_market_price WHERE id > :b" + (" AND area_code = :a" if area else "") + \
          " ORDER BY market_code, area_code, delivery_date, time_block, id"
    return conn.execute(text(sql), {"b": conn.info["iex_baseline_id"], "a": area}).all()


def _issues(conn: Connection, run_id: int) -> list[tuple[str, str]]:
    return [tuple(r) for r in conn.execute(text(
        "SELECT issue_code, severity FROM pi.data_quality_issue WHERE ingestion_run_id = :r ORDER BY 1, 2"),
        {"r": run_id})]


# ------------------------------------------------------------ happy path


def test_dam_end_to_end_mcp_and_13_bid_areas(conn: Connection, settings: Settings, storage: RawStorageManager) -> None:
    mock = MockIex(router(snapshot("dam")))

    summary = _run(conn, settings, storage, mock)

    r = summary.result
    assert summary.ok and r.status is RunStatus.SUCCESS
    assert (r.documents_processed, r.records_read, r.records_inserted, r.records_rejected) == (14, 1344, 1344, 0)
    assert str(mock.requests[0].url) == ("https://iex.test/api/v1/dam/market-snapshot?interval=ONE_FOURTH_HOUR"
                                         "&fromDate=10-11-2026&toDate=10-11-2026")
    assert sorted(req.url.params["bidArea"] for req in mock.requests[1:]) == sorted(api.BID_AREAS)

    mkt = _rows(conn, "MKT")
    assert len(mkt) == 96 and [row.time_block for row in mkt] == list(range(1, 97))
    first = mkt[0]
    assert (first.market_code, first.delivery_date, first.price_inr_per_mwh) == ("IEX_DAM", DAY, Decimal("3010.0100"))
    assert first.source_published_at is None and first.retrieved_at is not None
    assert first.ingestion_run_id == r.ingestion_run_id
    assert _rows(conn, "N1")[0].price_inr_per_mwh == Decimal("2960.0100")
    assert {row.area_code for row in _rows(conn)} == {"MKT", *api.BID_AREAS}

    run = conn.execute(text("SELECT r.run_status, r.records_inserted, r.parameters, d.dataset_code FROM pi.ingestion_run r "
                            "JOIN pi.source_dataset d USING (dataset_id) WHERE r.ingestion_run_id = :r"),
                       {"r": r.ingestion_run_id}).one()
    assert (run.run_status, run.records_inserted, run.dataset_code) == ("SUCCESS", 1344, "IEX_DAM")
    assert run.parameters["areas"] == ["MKT", *api.BID_AREAS]


def _snapshot_table(conn: Connection) -> bool:
    return inspect(conn).has_table("iex_market_snapshot", schema="pi")


def test_snapshot_volumes_follow_the_price_run(conn: Connection, settings: Settings, storage: RawStorageManager) -> None:
    mock = MockIex(router(snapshot("gdam")))

    summary = _run(conn, settings, storage, mock, kind="gdam")

    assert len(mock.requests) == 14                                    # no extra request for the volumes
    if not _snapshot_table(conn):
        assert summary.snapshot is None and "0013_iex_market_snapshot" in summary.snapshot_skipped
        assert summary.ok
        return
    s = summary.snapshot
    assert s.status is RunStatus.SUCCESS and (s.documents_processed, s.records_inserted) == (1, 96)
    assert s.raw_storage_keys == summary.result.raw_storage_keys[:1]  # the price run's stored snapshot file
    row = conn.execute(text("SELECT * FROM pi.iex_market_snapshot WHERE ingestion_run_id = :r AND time_block = 1"),
                       {"r": s.ingestion_run_id}).one()
    assert (row.market_code, row.sell_bid_mw, row.final_scheduled_volume_mw, row.congestion) == \
        ("IEX_GDAM", Decimal("410.9"), Decimal("385.19"), False)
    assert row.wind_mcv_mw == Decimal("9.5") and row.solar_mcv_mw is None and row.document_id is not None


def test_snapshot_backfill_from_stored_files(conn: Connection, settings: Settings, storage: RawStorageManager) -> None:
    if not _snapshot_table(conn):
        pytest.skip("pi.iex_market_snapshot not created (migration 0013_iex_market_snapshot not applied)")
    _run(conn, settings, storage, MockIex(router(snapshot("rtm"))), kind="rtm")

    backfill = backfill_snapshots("rtm", start_date=DAY, end_date=DAY, settings=settings,
                                  database=SavepointDatabase(conn), raw_source=storage)

    assert backfill.ok and backfill.result.duplicate_documents == 1    # already loaded by the live run
    assert backfill.result.records_inserted == 0


def test_raw_documents_and_checksums(conn: Connection, settings: Settings, storage: RawStorageManager) -> None:
    body = snapshot("dam")

    r = _run(conn, settings, storage, MockIex(router(body))).result

    docs = conn.execute(text("SELECT document_name, checksum_sha256, source_document_url, source_report_date, raw_payload "
                             "FROM pi.source_document WHERE ingestion_run_id = :r ORDER BY document_id"),
                        {"r": r.ingestion_run_id}).all()
    assert len(docs) == 14 and len(r.raw_storage_keys) == 14
    assert docs[0].document_name == "iex_dam__mkt__2026-11-10__2026-11-10.json"
    assert docs[0].checksum_sha256 == sha256_bytes(body) and docs[0].raw_payload is None
    assert docs[0].source_report_date == DAY and "market-snapshot" in docs[0].source_document_url
    for key, doc in zip(r.raw_storage_keys, docs):
        assert sha256_bytes(storage.read_bytes(key)) == doc.checksum_sha256
    assert r.raw_storage_keys[0].startswith("iex/iex_dam/mkt_2026-11-10_2026-11-10/")
    assert r.raw_storage_keys[1].startswith("iex/iex_dam/a1_2026-11-10_2026-11-10/")


def test_gdam_uses_unconstrained_mcp(conn: Connection, settings: Settings, storage: RawStorageManager) -> None:
    r = _run(conn, settings, storage, MockIex(router(snapshot("gdam"))), kind="gdam").result

    assert r.status is RunStatus.SUCCESS and r.records_inserted == 1344
    mkt = _rows(conn, "MKT")
    assert mkt[0].market_code == "IEX_GDAM" and mkt[0].price_inr_per_mwh == Decimal("4010.0100")


def test_gdam_before_the_field_change(conn: Connection, settings: Settings, storage: RawStorageManager) -> None:
    day = date(2026, 7, 7)
    body = snapshot("gdam", days=[day], era="before")

    r = _run(conn, settings, storage, MockIex(router(body, lambda a: area_prices(a, days=[day]))), kind="gdam",
             start=day).result

    assert r.status is RunStatus.SUCCESS and len(_rows(conn, "MKT")) == 96


def test_rtm_requests_the_snapshot_only(conn: Connection, settings: Settings, storage: RawStorageManager) -> None:
    mock = MockIex(router(snapshot("rtm")))

    r = _run(conn, settings, storage, mock, kind="rtm").result

    assert len(mock.requests) == 1 and mock.requests[0].url.path.endswith("/rtm/market-snapshot")
    assert r.status is RunStatus.SUCCESS and r.records_inserted == 96
    assert {row.area_code for row in _rows(conn)} == {"MKT"}


def test_multi_day_range_is_one_request_per_document(
    conn: Connection, settings: Settings, storage: RawStorageManager
) -> None:
    days = [date(2026, 11, 8), date(2026, 11, 9), date(2026, 11, 10)]
    mock = MockIex(router(snapshot("rtm", days=days)))

    r = _run(conn, settings, storage, mock, kind="rtm", start=days[0], end=days[-1]).result

    assert len(mock.requests) == 1 and r.records_inserted == 288
    assert sorted({row.delivery_date for row in _rows(conn)}) == days


# ---------------------------------------------------- duplicates / revisions


def test_identical_rerun_inserts_nothing(conn: Connection, settings: Settings, storage: RawStorageManager) -> None:
    _run(conn, settings, storage, MockIex(router(snapshot("dam"))))
    second = _run(conn, settings, storage, MockIex(router(snapshot("dam")))).result

    assert second.status is RunStatus.SUCCESS
    assert (second.duplicate_documents, second.records_inserted) == (14, 0)
    assert len(_rows(conn)) == 1344


def test_revised_price_is_versioned_and_history_kept(
    conn: Connection, settings: Settings, storage: RawStorageManager
) -> None:
    first = _run(conn, settings, storage, MockIex(router(snapshot("rtm"))), kind="rtm").result
    records = json.loads(snapshot("rtm"))["data"]
    records[4]["mcp"] = "9999.99"

    second = _run(conn, settings, storage, MockIex(router(envelope(records))), kind="rtm").result

    assert (second.records_inserted, second.records_unchanged) == (1, 95)
    history = [(row.price_inr_per_mwh, row.ingestion_run_id) for row in _rows(conn) if row.time_block == 5]
    assert history == [(Decimal("3050.0500"), first.ingestion_run_id), (Decimal("9999.9900"), second.ingestion_run_id)]


def test_current_day_rtm_fills_in_without_duplicates(
    conn: Connection, settings: Settings, storage: RawStorageManager
) -> None:
    partial = _run(conn, settings, storage, MockIex(router(snapshot("rtm", blocks=range(1, 71)))), kind="rtm",
                   today=DAY).result
    full = _run(conn, settings, storage, MockIex(router(snapshot("rtm"))), kind="rtm", today=DAY).result

    assert partial.status is RunStatus.SUCCESS and partial.records_inserted == 70
    assert _issues(conn, partial.ingestion_run_id) == [("INCOMPLETE_DAY", "INFO")]
    assert (full.records_inserted, full.records_unchanged) == (26, 70)
    assert _issues(conn, full.ingestion_run_id) == []
    assert len(_rows(conn)) == 96


# ------------------------------------------------- validation / quality


def test_incomplete_and_missing_past_days_are_flagged(
    conn: Connection, settings: Settings, storage: RawStorageManager
) -> None:
    body = envelope([dam_record(date(2026, 11, 9), b, spaced=False) for b in range(1, 95)])

    r = _run(conn, settings, storage, MockIex(router(body)), kind="rtm", start=date(2026, 11, 9), end=DAY).result

    assert r.status is RunStatus.SUCCESS and r.records_inserted == 94
    assert _issues(conn, r.ingestion_run_id) == [("INCOMPLETE_DAY", "WARNING"), ("MISSING_DAY", "WARNING")]


def test_invalid_prices_are_rejected_with_issues_not_discarded(
    conn: Connection, settings: Settings, storage: RawStorageManager
) -> None:
    records = json.loads(snapshot("rtm"))["data"]
    records[0]["mcp"] = "-1.00"        # below floor -> rejected
    records[1]["mcp"] = "20000.01"     # above absolute maximum -> rejected
    records[2]["mcp"] = "15000.00"     # above the 10000 cap -> warning, loaded
    records[3]["mcp"] = None           # null -> parse error (block 4 then has no price: incomplete day)

    r = _run(conn, settings, storage, MockIex(router(envelope(records))), kind="rtm").result

    assert r.status is RunStatus.PARTIAL
    assert (r.records_read, r.records_inserted, r.records_rejected) == (96, 93, 3)
    assert _issues(conn, r.ingestion_run_id) == [
        ("ABOVE_PRICE_CAP", "WARNING"), ("INCOMPLETE_DAY", "WARNING"), ("OUT_OF_RANGE", "ERROR"),
        ("OUT_OF_RANGE", "ERROR"), ("PARSE_ERROR", "ERROR")]
    assert Decimal("15000.0000") in {row.price_inr_per_mwh for row in _rows(conn)}
    assert any(key.startswith("iex/iex_rtm-rejected/") for key in storage.list_keys("iex"))


def test_duplicate_blocks_in_one_response(conn: Connection, settings: Settings, storage: RawStorageManager) -> None:
    records = json.loads(snapshot("rtm"))["data"]
    conflicting = dict(records[0], mcp="1.00")

    r = _run(conn, settings, storage, MockIex(router(envelope(records + [dict(records[1]), conflicting]))),
             kind="rtm").result

    assert r.records_inserted == 95  # block 1 conflicting -> neither copy loaded; block 2 loaded once
    assert ("CONFLICTING_DUPLICATE_IN_BATCH", "ERROR") in _issues(conn, r.ingestion_run_id)
    assert ("DUPLICATE_IN_BATCH", "INFO") in _issues(conn, r.ingestion_run_id)


# --------------------------------------------------------------- failures


@pytest.mark.parametrize(
    ("responder", "fragment"),
    [
        (lambda req: httpx.Response(200, content=envelope([])), "EmptyResponseError"),
        (lambda req: httpx.Response(200, content=envelope([], status=400, message="Data validation error")),
         "IexApiError"),
        (lambda req: httpx.Response(200, content=b"<html>Access denied</html>"), "InvalidResponseError"),
        (lambda req: httpx.Response(200, content=envelope([dam_record(DAY, 1, spaced=False, new_field=1)])),
         "InvalidResponseError"),
        (lambda req: httpx.Response(503, text="unavailable"), "HTTP 503"),  # after retries
        (lambda req: httpx.Response(404, text="Cannot GET"), "IexApiError"),
    ],
)
def test_failures_are_classified_and_load_nothing(
    conn: Connection, settings: Settings, storage: RawStorageManager, responder: Any, fragment: str
) -> None:
    summary = _run(conn, settings, storage, MockIex(responder), kind="rtm")

    assert summary.status is RunStatus.FAILED and fragment in summary.error
    assert _rows(conn) == []
    assert conn.execute(text("SELECT run_status FROM pi.ingestion_run WHERE ingestion_run_id = :r"),
                        {"r": summary.result.ingestion_run_id}).scalar_one() == "FAILED"


def test_timeouts_are_retried(conn: Connection, settings: Settings, storage: RawStorageManager) -> None:
    calls = {"n": 0}

    def flaky(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            raise httpx.ReadTimeout("simulated timeout", request=request)
        return httpx.Response(200, content=snapshot("rtm"))

    r = _run(conn, settings, storage, MockIex(flaky), kind="rtm").result

    assert r.status is RunStatus.SUCCESS and calls["n"] == 2


def test_database_failure_rolls_back_only_that_document(
    conn: Connection, settings: Settings, storage: RawStorageManager, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = TableLoader.load

    def failing_load(self, connection, items):  # type: ignore[no-untyped-def]
        result = original(self, connection, items)
        if items and items[0].row["area_code"] == "N2":
            raise RuntimeError("simulated database failure")
        return result

    monkeypatch.setattr(TableLoader, "load", failing_load)

    summary = _run(conn, settings, storage, MockIex(router(snapshot("dam"))))

    assert summary.status is RunStatus.FAILED and "simulated database failure" in summary.error
    loaded_areas = {row.area_code for row in _rows(conn)}
    assert "N2" not in loaded_areas and {"MKT", "A1", "N1"} <= loaded_areas
    n2_docs = conn.execute(text("SELECT count(*) FROM pi.source_document WHERE ingestion_run_id = :r "
                                "AND document_name LIKE '%\\_\\_n2\\_\\_%'"),
                           {"r": summary.result.ingestion_run_id}).scalar_one()
    assert n2_docs == 0
    assert any("/n2_" in key for key in summary.result.raw_storage_keys)  # raw file kept even so


# -------------------------------------------------------- reference data


def test_reference_seeding_is_idempotent(conn: Connection) -> None:
    again = seed_reference(conn, "pi")

    assert again.inserted == [] and again.differing == []
    assert len(again.unchanged) == len(MARKET_ROWS) + len(AREA_ROWS) == 17


def test_area_states_seed_is_idempotent(conn: Connection) -> None:
    if not inspect(conn).has_table("area_state_map", schema="pi"):
        pytest.skip("pi.area_state_map not created (migration 0012_area_state_map not applied)")

    seed_area_states(conn, "pi")
    again = seed_area_states(conn, "pi")

    assert again.inserted == [] and again.differing == [] and again.overlaps == []
    assert len(again.unchanged) == len(AREA_STATE_ROWS)
    kerala = conn.execute(text("SELECT area_code FROM pi.area_state_map WHERE state_code = 'KL' "
                               "AND effective_to IS NULL")).scalar_one()
    assert kerala == "S3"


def test_missing_reference_is_reported(conn: Connection) -> None:
    assert missing_reference(conn, "pi", "IEX_DAM", ["MKT", "N1"]) == []
    assert missing_reference(conn, "pi", "IEX_NOPE", ["MKT", "ZZ"]) == ["market_master:IEX_NOPE", "market_area:ZZ"]


def test_date_guards_run_before_any_request(conn: Connection, settings: Settings, storage: RawStorageManager) -> None:
    mock = MockIex(router(snapshot("dam")))

    with pytest.raises(ValueError, match="at most 31 days"):
        _run(conn, settings, storage, mock, start=date(2026, 10, 1), end=date(2026, 11, 1))
    assert mock.requests == []


@pytest.mark.parametrize(
    "argv",
    [["dam", "--start-date", "2026-10-01", "--end-date", "2026-11-01"], ["rtm", "--date", "2099-01-01"],
     ["gdam", "--date", "2020-01-01"]],
)
def test_cli_rejects_invalid_ranges_with_exit_code_2(argv: list[str], settings: Settings) -> None:
    assert cli_main(argv, settings=settings) == 2
