"""End-to-end NPP tests against the real ``pi`` schema with a mocked API.

Every test runs inside one outer transaction that is rolled back (nothing
persists). Assertions only see rows created inside that transaction, so
committed production rows in the table do not affect them. No live API calls.
"""

from __future__ import annotations

import copy
from collections.abc import Iterator
from datetime import date, datetime, timedelta, timezone
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
from src.pipelines.npp.runner import RunSummary, run_npp
from scripts.ingest_npp import main as cli_main
from tests.support.npp import DAY, DEMAND_ITEMS, GENERATION_ITEMS, MockNpp, to_bytes

pytestmark = pytest.mark.integration

IST = timezone(timedelta(hours=5, minutes=30))
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
        connection.info["npp_baseline_id"] = connection.execute(
            text("SELECT coalesce(max(id), 0) FROM pi.npp_demand_generation")).scalar_one()
        try:
            yield connection
        finally:
            outer.rollback()


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(raw_data_dir=tmp_path / "raw", log_to_file=False, npp_base_url="https://npp.test/dashBoard")


@pytest.fixture
def storage(tmp_path: Path) -> RawStorageManager:
    return RawStorageManager(LocalFileSystemBackend(tmp_path / "raw"))


def _run(conn: Connection, settings: Settings, storage: RawStorageManager, mock: MockNpp, kind: str = "demand",
         start: date = DAY, end: date | None = None) -> RunSummary:
    client = HttpClient(settings=settings, transport=httpx.MockTransport(mock), sleep=lambda _: None)
    return run_npp(kind, start_date=start, end_date=end, settings=settings, http_client=client,  # type: ignore[arg-type]
                   database=SavepointDatabase(conn), raw_storage=storage, today=TODAY)


def _rows(conn: Connection) -> list[Any]:
    """Rows created in this test's transaction."""
    return conn.execute(text("SELECT * FROM pi.npp_demand_generation WHERE id > :baseline "
                             "ORDER BY observed_at, name_of_data, id"),
                        {"baseline": conn.info["npp_baseline_id"]}).all()


def _issues(conn: Connection, run_id: int) -> list[tuple[str, str]]:
    return [tuple(r) for r in conn.execute(text(
        "SELECT issue_code, severity FROM pi.data_quality_issue WHERE ingestion_run_id = :r ORDER BY 1"),
        {"r": run_id})]


def _only(summary: RunSummary):
    assert len(summary.outcomes) == 1
    return summary.outcomes[0].result


# ------------------------------------------------------------ happy path


def test_demand_ingestion_end_to_end(conn: Connection, settings: Settings, storage: RawStorageManager) -> None:
    content = to_bytes(DEMAND_ITEMS)
    mock = MockNpp(content)

    summary = _run(conn, settings, storage, mock)

    result = _only(summary)
    assert summary.ok and result.status is RunStatus.SUCCESS
    assert (result.records_read, result.records_inserted, result.records_rejected) == (3, 3, 0)
    assert str(mock.requests[0].url) == "https://npp.test/dashBoard/demandmet1chartdata?date=2026-11-10"

    rows = _rows(conn)
    assert [(r.name_of_data, r.value_of_data, r.unit_code, r.state_code, r.region_code) for r in rows] == \
        [("DEMAND MET", Decimal("153196.0000"), "MW", None, None),
         ("DEMAND MET", Decimal("152797.0000"), "MW", None, None),
         ("DEMAND MET", Decimal("152869.0000"), "MW", None, None)]
    assert rows[0].observed_at == datetime(2026, 11, 10, 0, 1, 33, tzinfo=IST)
    assert rows[0].source_updated_on_ms == 1794249093000
    assert {r.ingestion_run_id for r in rows} == {result.ingestion_run_id}

    run = conn.execute(text("SELECT r.run_status, r.records_inserted, r.parameters, d.dataset_code "
                            "FROM pi.ingestion_run r JOIN pi.source_dataset d USING (dataset_id) "
                            "WHERE r.ingestion_run_id = :r"), {"r": result.ingestion_run_id}).one()
    assert (run.run_status, run.records_inserted, run.dataset_code) == ("SUCCESS", 3, "NPP_DEMAND")
    assert run.parameters["date"] == "2026-11-10"

    doc = conn.execute(text("SELECT * FROM pi.source_document WHERE document_id = :d"),
                       {"d": rows[0].document_id}).one()
    assert doc.checksum_sha256 == sha256_bytes(content)
    assert doc.source_report_date == DAY and doc.raw_payload is None
    assert doc.source_document_url.endswith("demandmet1chartdata?date=2026-11-10")
    key = result.raw_storage_keys[0]
    assert key.startswith("npp/npp_demand/2026-11-10/") and storage.read_bytes(key) == content


def test_generation_ingestion_end_to_end(conn: Connection, settings: Settings, storage: RawStorageManager) -> None:
    mock = MockNpp(to_bytes(GENERATION_ITEMS))

    result = _only(_run(conn, settings, storage, mock, kind="generation"))

    assert result.status is RunStatus.SUCCESS and result.records_inserted == 12
    assert "demandmet2chartdata" in str(mock.requests[0].url)
    names = {r.name_of_data for r in _rows(conn)}
    assert names == {"THERMAL GENERATION", "HYDRO GENERATION", "GAS GENERATION", "NUCLEAR GENERATION",
                     "SOLAR GENERATION", "WIND GENERATION"}


# ---------------------------------------------------- duplicate handling


def test_identical_response_is_not_reloaded(conn: Connection, settings: Settings, storage: RawStorageManager) -> None:
    _run(conn, settings, storage, MockNpp(to_bytes(DEMAND_ITEMS)))
    second = _only(_run(conn, settings, storage, MockNpp(to_bytes(DEMAND_ITEMS))))

    assert second.status is RunStatus.SUCCESS
    assert second.duplicate_documents == 1 and second.records_inserted == 0
    assert len(_rows(conn)) == 3


def test_growing_intraday_response_inserts_only_new_snapshots(
    conn: Connection, settings: Settings, storage: RawStorageManager
) -> None:
    _run(conn, settings, storage, MockNpp(to_bytes(DEMAND_ITEMS[:2])))
    second = _only(_run(conn, settings, storage, MockNpp(to_bytes(DEMAND_ITEMS))))

    # NULL state/region rows match null-safely, so the first two are recognised.
    assert (second.records_read, second.records_inserted, second.records_unchanged) == (3, 1, 2)
    rows = _rows(conn)
    assert len(rows) == 3
    assert len({(r.name_of_data, r.observed_at) for r in rows}) == 3


def test_changed_value_creates_a_new_version(conn: Connection, settings: Settings, storage: RawStorageManager) -> None:
    first = _only(_run(conn, settings, storage, MockNpp(to_bytes(DEMAND_ITEMS))))
    revised = copy.deepcopy(DEMAND_ITEMS)
    revised[0]["value_of_data"] = 153200

    second = _only(_run(conn, settings, storage, MockNpp(to_bytes(revised))))

    assert (second.records_inserted, second.records_unchanged) == (1, 2)
    history = [(r.value_of_data, r.ingestion_run_id) for r in _rows(conn) if r.source_updated_on_ms == 1794249093000]
    assert history == [(Decimal("153196.0000"), first.ingestion_run_id),
                       (Decimal("153200.0000"), second.ingestion_run_id)]


def test_all_india_rows_do_not_collide_with_regional_rows(
    conn: Connection, settings: Settings, storage: RawStorageManager
) -> None:
    conn.execute(text("INSERT INTO pi.region (region_code, region_name) VALUES ('TST_R', 'Test region (rolled back)')"))
    run_id = conn.execute(text(
        "INSERT INTO pi.ingestion_run (dataset_id, started_at, run_status) SELECT dataset_id, now(), 'SUCCESS' "
        "FROM pi.source_dataset WHERE dataset_code = 'NPP_DEMAND' RETURNING ingestion_run_id")).scalar_one()
    conn.execute(text(
        "INSERT INTO pi.npp_demand_generation (region_code, ingestion_run_id, observed_at, name_of_data, "
        "value_of_data, unit_code, source_updated_on_ms) VALUES ('TST_R', :r, :t, 'DEMAND MET', 1, 'MW', 1)"),
        {"r": run_id, "t": datetime(2026, 11, 10, 0, 1, 33, tzinfo=IST)})

    result = _only(_run(conn, settings, storage, MockNpp(to_bytes(DEMAND_ITEMS))))

    assert result.records_inserted == 3 and result.records_unchanged == 0
    assert len([r for r in _rows(conn) if r.region_code is None]) == 3


# ------------------------------------------------- validation / failures


def test_unknown_series_and_bad_values_are_rejected_not_loaded(
    conn: Connection, settings: Settings, storage: RawStorageManager
) -> None:
    items = copy.deepcopy(DEMAND_ITEMS)
    items.append({"updated_on": 1794249453000, "name_of_data": "RENEWABLE GENERATION", "value_of_data": 5})
    items[1]["value_of_data"] = -1
    items[2]["value_of_data"] = "n/a"

    result = _only(_run(conn, settings, storage, MockNpp(to_bytes(items))))

    assert result.status is RunStatus.PARTIAL
    assert (result.records_read, result.records_inserted, result.records_rejected) == (4, 1, 3)
    assert _issues(conn, result.ingestion_run_id) == [("OUT_OF_RANGE", "ERROR"), ("PARSE_ERROR", "ERROR"),
                                                      ("UNKNOWN_SERIES", "ERROR")]
    assert [r.value_of_data for r in _rows(conn)] == [Decimal("153196.0000")]
    assert any(key.startswith("npp/npp_demand-rejected/2026-11-10/") for key in storage.list_keys("npp"))


@pytest.mark.parametrize(
    ("responder", "error_fragment"),
    [
        (to_bytes([]), "EmptyResponseError"),
        (b"<html>Service unavailable</html>", "InvalidResponseError"),
        (to_bytes([{"updated_on": 1, "value": 2}]), "InvalidResponseError"),
        (lambda request: httpx.Response(404, text="Not Found"), "NppApiError"),
    ],
)
def test_failures_are_classified_and_load_nothing(
    conn: Connection, settings: Settings, storage: RawStorageManager, responder: Any, error_fragment: str
) -> None:
    summary = _run(conn, settings, storage, MockNpp(responder))

    outcome = summary.outcomes[0]
    assert outcome.status is RunStatus.FAILED and error_fragment in outcome.error
    assert _rows(conn) == []
    status = conn.execute(text("SELECT run_status FROM pi.ingestion_run WHERE ingestion_run_id = :r"),
                          {"r": outcome.result.ingestion_run_id}).scalar_one()
    assert status == "FAILED"


def test_raw_response_is_kept_even_when_it_is_invalid(
    conn: Connection, settings: Settings, storage: RawStorageManager
) -> None:
    _run(conn, settings, storage, MockNpp(b"<html>changed</html>"))

    keys = storage.list_keys("npp", "npp_demand")
    assert len(keys) == 1 and storage.read_bytes(keys[0]) == b"<html>changed</html>"


def test_each_day_is_a_separate_run(conn: Connection, settings: Settings, storage: RawStorageManager) -> None:
    def by_date(request: httpx.Request) -> httpx.Response:
        if request.url.params["date"] == "2026-11-11":
            return httpx.Response(200, content=to_bytes([]))
        return httpx.Response(200, content=to_bytes(DEMAND_ITEMS))

    summary = _run(conn, settings, storage, MockNpp(by_date), start=date(2026, 11, 10), end=date(2026, 11, 11))

    assert [(o.day, o.status) for o in summary.outcomes] == [(date(2026, 11, 10), RunStatus.SUCCESS),
                                                             (date(2026, 11, 11), RunStatus.FAILED)]
    assert not summary.ok and len(_rows(conn)) == 3
    assert len({o.result.ingestion_run_id for o in summary.outcomes}) == 2


def test_date_guards_run_before_any_request(conn: Connection, settings: Settings, storage: RawStorageManager) -> None:
    mock = MockNpp(to_bytes(DEMAND_ITEMS))

    with pytest.raises(ValueError, match="at most 31 days"):
        _run(conn, settings, storage, mock, start=date(2026, 10, 1), end=date(2026, 11, 1))
    assert mock.requests == []


# -------------------------------------------------------------------- CLI


@pytest.mark.parametrize(
    "argv",
    [
        ["demand", "--start-date", "2026-10-01", "--end-date", "2026-11-01"],
        ["generation", "--date", "2099-01-01"],
        ["demand", "--date", "2025-01-01"],
    ],
)
def test_cli_rejects_invalid_ranges_with_exit_code_2(argv: list[str], settings: Settings) -> None:
    assert cli_main(argv, settings=settings) == 2


def test_cli_requires_a_complete_range() -> None:
    with pytest.raises(SystemExit) as exc:
        cli_main(["demand", "--start-date", "2026-11-01"])
    assert exc.value.code == 2
