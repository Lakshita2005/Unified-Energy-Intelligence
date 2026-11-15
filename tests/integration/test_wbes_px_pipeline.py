"""WBES PX Dashboard end to end: mocked HTTP, real schema, every test rolled back.

No live WBES calls (decision W2). Synthetic values have three decimals so they
can never coincide with rows already present in the database.
"""

from __future__ import annotations

import copy
import json
import time
from collections.abc import Iterator
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.engine import Connection

from config.settings import Settings
from src.common.checksums import sha256_bytes
from src.common.database import DatabaseManager, SavepointDatabase
from src.common.exceptions import ConfigurationError
from src.common.http_client import HttpClient
from src.common.raw_storage import LocalFileSystemBackend, RawStorageManager, StorageError
from src.ingestion.models import RunStatus
from src.pipelines.wbes import api
from src.pipelines.wbes.errors import WbesError, WbesSessionLocked
from src.pipelines.wbes.lock import wbes_session_lock
from src.pipelines.wbes.runner import PxRunSummary, run_wbes_px
from src.pipelines.wbes.session import SessionTokens, TokenStore
from tests.support.wbes import (
    API,
    PORTAL,
    UTILITY,
    MockWbes,
    entity_items,
    envelope,
    make_jwt,
    px_record,
    region_items,
    series,
)

pytestmark = pytest.mark.integration

IST = ZoneInfo("Asia/Kolkata")
D = date(2026, 11, 10)
TODAY = date(2026, 11, 13)
REGION, LINK, ENTITY = "wbes_px_region_schedule", "wbes_px_link_schedule", "wbes_px_entity_schedule"
TABLES = (REGION, LINK, ENTITY)


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
        for table in TABLES:
            connection.info[table] = connection.execute(text(f"SELECT coalesce(max(id), 0) FROM pi.{table}")).scalar()
        try:
            yield connection
        finally:
            outer.rollback()


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    # Most tests pin the CreatedOn time zone; test_unverified_created_on_timezone covers the default.
    return Settings(raw_data_dir=tmp_path / "raw", log_to_file=False, wbes_api_base_url=API, wbes_portal_url=PORTAL,
                    http_max_retries=0, wbes_request_pause_seconds=0, wbes_created_on_naive_timezone="Asia/Kolkata")


@pytest.fixture
def storage(tmp_path: Path) -> RawStorageManager:
    return RawStorageManager(LocalFileSystemBackend(tmp_path / "raw"))


@pytest.fixture
def mock() -> MockWbes:
    return MockWbes()


@pytest.fixture
def store(tmp_path: Path, mock: MockWbes) -> TokenStore:
    token_store = TokenStore(tmp_path / "session" / "session.json")
    token_store.save(SessionTokens(mock.access, mock.refresh, UTILITY, datetime(2026, 11, 13, 9, 0, tzinfo=timezone.utc)))
    return token_store


def _run(conn: Connection, settings: Settings, storage: RawStorageManager, mock: MockWbes, store: TokenStore,
         manager: DatabaseManager, start: date = D, **kwargs: Any) -> PxRunSummary:
    client = HttpClient(settings=settings, transport=httpx.MockTransport(mock), sleep=lambda _: None)
    kwargs.setdefault("exchanges", ["IEX"])
    kwargs.setdefault("markets", ["DAM"])
    return run_wbes_px(start_date=start, settings=settings, http_client=client, database=SavepointDatabase(conn),
                       raw_storage=storage, token_store=store, lock_engine=manager.engine,
                       today=kwargs.pop("today", TODAY),
                       sleep=lambda _: None, **kwargs)


def _rows(conn: Connection, table: str) -> list[Any]:
    return conn.execute(text(f"SELECT * FROM pi.{table} WHERE id > :b ORDER BY id"), {"b": conn.info[table]}).all()


def _issues(conn: Connection, run_id: int) -> list[tuple[str, str]]:
    return [tuple(r) for r in conn.execute(text(
        "SELECT issue_code, severity FROM pi.data_quality_issue WHERE ingestion_run_id = :r ORDER BY 1, 2"),
        {"r": run_id})]


def _doc(conn: Connection, document_id: int) -> Any:
    return conn.execute(text("SELECT s.dataset_code, d.checksum_sha256, d.ingestion_run_id, d.raw_storage_uri, "
                             "d.published_at FROM pi.source_document d JOIN pi.source_dataset s USING (dataset_id) "
                             "WHERE d.document_id = :d"), {"d": document_id}).one()


def _raw_files(settings: Settings, partition: str) -> list[Path]:
    return sorted(p for p in (Path(settings.raw_data_dir) / "wbes" / "wbes_px" / partition).rglob("*.json")
                  if not p.name.endswith(".meta.json"))


# ----------------------------------------------------------------- end to end

def test_one_response_feeds_three_tables(conn, settings, storage, mock, store, manager) -> None:
    mock.set(D, "IEX", "DAM", envelope([px_record(D, 4)]))

    summary = _run(conn, settings, storage, mock, store, manager)

    assert summary.ok and len({p.result.ingestion_run_id for p in summary.parts.values()}) == 3
    assert {k: p.result.records_inserted for k, p in summary.parts.items()} == {"region": 576, "link": 192,
                                                                              "entity": 288}
    assert sum(1 for _, path in mock.calls if path.endswith("GetPowerExchangeByDetails")) == 1
    responses, documents = _raw_files(settings, "px-response"), _raw_files(settings, "px")
    assert len(responses) == len(documents) == 1
    original = responses[0].read_bytes()
    assert json.loads(original) == envelope([px_record(D, 4)])                  # stored exactly as received
    raw = documents[0].read_bytes()                                             # request-bound document
    bound = json.loads(raw)
    assert bound["response_text"].encode("utf-8") == original and bound["response_sha256"] == sha256_bytes(original)
    assert (bound["request"]["power_exchange"], bound["request"]["market"], bound["request"]["body"]["RevisionNo"]) \
        == ("IEX", "DAM", -1)
    created = datetime(2026, 11, 9, 14, 5, 31, tzinfo=IST)
    for table, dataset in ((REGION, "WBES_PX_REGION"), (LINK, "WBES_PX_LINK"), (ENTITY, "WBES_PX_ENTITY")):
        rows = _rows(conn, table)
        doc = _doc(conn, rows[0].document_id)
        run_id = summary.parts[{"WBES_PX_REGION": "region", "WBES_PX_LINK": "link",
                                "WBES_PX_ENTITY": "entity"}[dataset]].result.ingestion_run_id
        assert (doc.dataset_code, doc.checksum_sha256, doc.ingestion_run_id) == (dataset, sha256_bytes(raw), run_id)
        assert doc.published_at == created
        assert {r.ingestion_run_id for r in rows} == {run_id} and {r.document_id for r in rows} == {rows[0].document_id}
        assert {(r.revision_no, r.power_ex_id, r.availability_basis, r.source_published_at, r.source_available_at)
                for r in rows} == {(4, "PXID-SYN-1", "SOURCE_CREATED_ON", created, created)}
        assert _issues(conn, run_id) == []
    region = {(r.wbes_region_id, r.time_block): r for r in _rows(conn, REGION)}
    assert region[(1, 1)].injection_value == Decimal("100.111") and region[(1, 96)].injection_value == Decimal("149.226")
    assert {r.region_code for r in region.values()} == {None}                    # mapping unverified; NLDC NULL
    assert (region[(6, 1)].power_exchange, region[(6, 1)].market, region[(6, 1)].power_exchange_type_id,
            region[(6, 1)].market_type_id) == ("IEX", "DAM", 1, 11)
    entities = _rows(conn, ENTITY)
    assert sum(1 for r in entities if r.drawal_value == 0) == 192                 # published zeros are stored
    assert {r.entity_unit_acronym for r in entities if r.entity_acronym == "TEST_DISCOM_B"} == {None}
    links = _rows(conn, LINK)
    assert links[0].net_amount_value == Decimal("-12.345") and links[0].link_number == 1
    meta = json.loads(Path(str(documents[0]) + ".meta.json").read_text())
    with pytest.raises(StorageError, match="write-once"):                       # raw data is write-once
        storage.save_bytes(b"overwrite", source="wbes", dataset="wbes_px", extension="json", partition="px",
                           ingested_at=datetime.fromisoformat(meta["ingested_at"]))
    assert documents[0].read_bytes() == raw
    for path in responses + documents:                                          # checksums verifiable from metadata
        assert json.loads(Path(str(path) + ".meta.json").read_text())["sha256"] == sha256_bytes(path.read_bytes())


def test_discovers_every_exchange_and_market(conn, settings, storage, mock, store, manager) -> None:
    mock.set(D, "PXI", "RTM", envelope([px_record(D, 2)]))
    summary = _run(conn, settings, storage, mock, store, manager, exchanges=None, markets=None)
    assert summary.ok and len(summary.responses) == 12                          # 3 exchanges x 4 markets
    assert sum(1 for _, path in mock.calls if path.endswith("GetPowerExchangeByDetails")) == 12
    region = summary.parts["region"].result
    assert (region.documents_processed, region.records_inserted) == (12, 576)
    assert _issues(conn, region.ingestion_run_id).count(("NOT_PUBLISHED", "INFO")) == 11
    assert {(r.power_exchange, r.market) for r in _rows(conn, REGION)} == {("PXI", "RTM")}


def test_requested_names_not_listed_are_reported(conn, settings, storage, mock, store, manager) -> None:
    summary = _run(conn, settings, storage, mock, store, manager, exchanges=["IEX", "NOPE"], markets=["DAM"])
    assert summary.not_listed == [f"{D} exchange NOPE"] and len(summary.responses) == 1


# -------------------------------------------------------------- revisions

def _changed(record: dict[str, Any], *, revision: int, power_ex_id: str, created_on: str,
             region_block: int | None = None, link_block: int | None = None) -> dict[str, Any]:
    out = copy.deepcopy(record)
    out.update(RevisionNo=revision, PowerExId=power_ex_id, CreatedOn=created_on)
    if region_block is not None:
        out["AllRegionPowerExJsonData"]["RegionWiseJsonData"][1]["Injection"][region_block - 1] = 777.777
    if link_block is not None:
        out["AllLinkPowerExJsonData"]["LinkWiseJsonData"][0]["NetAmount"][link_block - 1] = 333.333
    return out


def test_lineage_only_revision_creates_no_versions_value_change_does(conn, settings, storage, mock, store,
                                                                     manager) -> None:
    rev1 = px_record(D, 1, created_on="2026-11-09T13:00:00")
    rev2 = _changed(rev1, revision=2, power_ex_id="PXID-SYN-2", created_on="2026-11-09T15:30:00", region_block=10)
    mock.set(D, "IEX", "DAM", envelope([rev2, rev1]))        # out of order on purpose

    summary = _run(conn, settings, storage, mock, store, manager)

    assert {k: p.result.records_inserted for k, p in summary.parts.items()} == {"region": 577, "link": 192,
                                                                              "entity": 288}
    changed = [r for r in _rows(conn, REGION) if (r.wbes_region_id, r.time_block) == (2, 10)]
    assert [(r.revision_no, r.injection_value, r.power_ex_id, r.source_available_at.astimezone(IST).time().isoformat())
            for r in changed] == [(1, Decimal("204.875"), "PXID-SYN-1", "13:00:00"),
                                  (2, Decimal("777.777"), "PXID-SYN-2", "15:30:00")]
    assert {r.revision_no for r in _rows(conn, LINK)} == {1}                  # lineage-only change: no new rows
    assert {r.revision_no for r in _rows(conn, ENTITY)} == {1}


def test_rerun_later_revisions_and_reprocess(conn, settings, storage, mock, store, manager) -> None:
    rev1 = px_record(D, 1, created_on="2026-11-09T13:00:00")
    rev2 = _changed(rev1, revision=2, power_ex_id="PXID-SYN-2", created_on="2026-11-09T15:30:00")   # lineage only
    mock.set(D, "IEX", "DAM", envelope([rev1, rev2]))
    first = _run(conn, settings, storage, mock, store, manager)
    assert first.parts["link"].result.records_inserted == 192

    again = _run(conn, settings, storage, mock, store, manager)              # identical response
    assert all(p.result.records_inserted == 0 and p.result.duplicate_documents == 1 for p in again.parts.values())

    rev3 = _changed(rev2, revision=3, power_ex_id="PXID-SYN-3", created_on="2026-11-09T18:00:00", link_block=5)
    mock.set(D, "IEX", "DAM", envelope([rev1, rev2, rev3]))
    later = _run(conn, settings, storage, mock, store, manager)
    link = later.parts["link"].result
    assert (link.records_inserted, link.records_unchanged) == (1, 192 * 3 - 1)
    assert later.parts["region"].result.records_inserted == 0
    assert ("REVISION_ALREADY_STORED", "INFO") in _issues(conn, link.ingestion_run_id)
    block5 = [r for r in _rows(conn, LINK) if (r.link_name, r.time_block) == ("TEST LINK ER-NR", 5)]
    assert [(r.revision_no, r.net_amount_value) for r in block5] == [(1, Decimal("-11.901")), (3, Decimal("333.333"))]

    counts = {t: len(_rows(conn, t)) for t in TABLES}
    reprocessed = _run(conn, settings, storage, mock, store, manager, reprocess=True)
    assert all(p.result.records_inserted == 0 for p in reprocessed.parts.values())
    assert {t: len(_rows(conn, t)) for t in TABLES} == counts


# ----------------------------------------------------------- data quality

def test_malformed_arrays_unknown_region_and_duplicate_entity(conn, settings, storage, mock, store, manager) -> None:
    regions = region_items()
    regions[0]["Injection"] = regions[0]["Injection"][:95]                    # malformed: rejected, not padded
    regions.append({"Region": 9, "Injection": series(9.999), "Drawal": series(8.888)})
    twins = [{"EntityAcronym": "TEST_TWIN", "EntityUnitAcronym": "U", "EntityPxCode": "P", "EntityRegionId": 1,
              "Injection": series(1.111), "Drawal": series(2.222)}] * 2
    mock.set(D, "IEX", "DAM", envelope([px_record(D, 1, regions=regions, entities=twins)]))
    summary = _run(conn, settings, storage, mock, store, manager)
    region, entity = summary.parts["region"].result, summary.parts["entity"].result
    assert region.status is RunStatus.PARTIAL and (region.records_inserted, region.records_rejected) == (576, 1)
    assert {"UNKNOWN_REGION", "PARSE_ERROR"} <= {c for c, _ in _issues(conn, region.ingestion_run_id)}
    assert 1 not in {r.wbes_region_id for r in _rows(conn, REGION)}
    assert {r.wbes_region_id for r in _rows(conn, REGION)} == {2, 3, 4, 5, 6, 9}
    assert entity.status is RunStatus.PARTIAL and (entity.records_inserted, entity.records_rejected) == (0, 192)
    assert ("DUPLICATE_KEY_IN_REVISION", "ERROR") in _issues(conn, entity.ingestion_run_id)
    assert summary.parts["link"].status is RunStatus.SUCCESS


def test_unsuccessful_response_fails_every_part(conn, settings, storage, mock, store, manager) -> None:
    mock.set(D, "IEX", "DAM", envelope([], code="WBES-500", message="Internal error"))
    summary = _run(conn, settings, storage, mock, store, manager)
    assert all(p.status is RunStatus.FAILED and "WBES-500" in p.error for p in summary.parts.values())
    assert _rows(conn, REGION) == []
    assert len(_raw_files(settings, "list-exchanges")) == len(_raw_files(settings, "list-markets")) == 1
    assert len(_raw_files(settings, "px-response")) == 1 and _raw_files(settings, "px") == []


def test_missing_response_status_and_unparsed_created_on(conn, settings, storage, mock, store, manager) -> None:
    mock.set(D, "IEX", "DAM", envelope([px_record(D, 1, created_on="sometime")], code=None))
    summary = _run(conn, settings, storage, mock, store, manager)
    assert summary.ok
    issues = _issues(conn, summary.parts["link"].result.ingestion_run_id)
    assert ("RESPONSE_STATUS_MISSING", "WARNING") in issues and ("CREATED_ON_UNPARSED", "WARNING") in issues
    rows = _rows(conn, LINK)
    assert {(r.availability_basis, r.source_published_at) for r in rows} == {("RETRIEVED_AT", None)}
    assert all(r.source_available_at == r.retrieved_at for r in rows)


# ------------------------------------------------------------- session and lock

def test_no_session_records_failed_runs_without_calling_wbes(conn, settings, storage, mock, tmp_path, manager) -> None:
    summary = _run(conn, settings, storage, mock, TokenStore(tmp_path / "nothing.json"), manager)
    assert summary.login_required and mock.calls == []
    assert all(p.status is RunStatus.FAILED and "ingest_wbes login" in p.error for p in summary.parts.values())


def test_refused_refresh_is_login_required_and_never_leaks_tokens(conn, settings, storage, mock, store,
                                                                  manager) -> None:
    mock.access = make_jwt(time.time() - 5)
    store.save(SessionTokens(mock.access, mock.refresh, UTILITY, datetime(2026, 11, 13, 9, 0, tzinfo=timezone.utc)))
    mock.refusals = 1
    summary = _run(conn, settings, storage, mock, store, manager)
    assert summary.login_required
    stored_text = " ".join(r[0] for r in conn.execute(text(
        "SELECT issue_description FROM pi.data_quality_issue WHERE ingestion_run_id = ANY(:r)"),
        {"r": [p.result.ingestion_run_id for p in summary.parts.values()]}))
    for secret in (mock.refresh, mock.access, "leak-check"):
        assert secret not in stored_text and all(secret not in (p.error or "") for p in summary.parts.values())


def test_rotated_tokens_are_saved_and_never_reach_raw_storage(conn, settings, storage, mock, store, manager) -> None:
    mock.access = make_jwt(time.time() + 30)                                  # refreshed before the first call
    store.save(SessionTokens(mock.access, mock.refresh, UTILITY, datetime(2026, 11, 13, 9, 0, tzinfo=timezone.utc)))
    mock.set(D, "IEX", "DAM", envelope([px_record(D, 1)]))
    assert _run(conn, settings, storage, mock, store, manager).ok
    assert mock.refreshes == 1 and store.load().refresh_token == mock.refresh == "refresh-1"
    for path in Path(settings.raw_data_dir).rglob("*"):
        if path.is_file():
            content = path.read_text(encoding="utf-8")
            assert "refresh-" not in content and "Bearer" not in content and mock.access not in content


def test_advisory_lock_blocks_a_second_process(conn, settings, storage, mock, store, manager) -> None:
    with wbes_session_lock(manager.engine, UTILITY):
        with pytest.raises(WbesSessionLocked):
            _run(conn, settings, storage, mock, store, manager)
        with pytest.raises(WbesSessionLocked):
            with wbes_session_lock(manager.engine, UTILITY.lower()):
                pass
    mock.set(D, "IEX", "DAM", envelope([px_record(D, 1)]))
    assert _run(conn, settings, storage, mock, store, manager).ok                # released again


# ------------------------------------------------- review fixes I-1 .. I-4, M-1

def test_item_withdrawn_in_a_later_run_is_reported(conn, settings, storage, mock, store, manager) -> None:
    mock.set(D, "IEX", "DAM", envelope([px_record(D, 1)]))
    assert _run(conn, settings, storage, mock, store, manager).ok
    mock.set(D, "IEX", "DAM", envelope([px_record(D, 2, entities=entity_items()[:2], power_ex_id="PXID-SYN-2")]))
    later = _run(conn, settings, storage, mock, store, manager)
    entity = later.parts["entity"].result
    assert entity.records_inserted == 0                                          # values unchanged, nothing invented
    issues = conn.execute(text("SELECT issue_code, severity, issue_description FROM pi.data_quality_issue "
                               "WHERE ingestion_run_id = :r"), {"r": entity.ingestion_run_id}).all()
    absent = [r for r in issues if r.issue_code == "STORED_ITEM_ABSENT"]
    assert len(absent) == 1 and absent[0].severity == "WARNING" and "TEST_GEN_A', 'U2'" in absent[0].issue_description
    assert ("STORED_ITEM_ABSENT", "WARNING") not in _issues(conn, later.parts["region"].result.ingestion_run_id)
    mock.set(D, "IEX", "DAM", envelope([px_record(D, 3, entities=[], power_ex_id="PXID-SYN-3")]))
    emptied = _run(conn, settings, storage, mock, store, manager).parts["entity"].result
    assert "3 stored entity item(s)" in conn.execute(text(
        "SELECT issue_description FROM pi.data_quality_issue WHERE ingestion_run_id = :r AND "
        "issue_code = 'STORED_ITEM_ABSENT'"), {"r": emptied.ingestion_run_id}).scalar()


def test_older_revision_after_newer_is_skipped(conn, settings, storage, mock, store, manager) -> None:
    rev2 = px_record(D, 2, regions=region_items(scale=2.0), created_on="2026-11-09T16:00:00")
    mock.set(D, "IEX", "DAM", envelope([rev2]))
    assert _run(conn, settings, storage, mock, store, manager).ok
    mock.set(D, "IEX", "DAM", envelope([px_record(D, 1, created_on="2026-11-09T13:00:00")]))   # older, other values
    late = _run(conn, settings, storage, mock, store, manager)
    assert all(p.result.records_inserted == 0 for p in late.parts.values())
    region_issues = _issues(conn, late.parts["region"].result.ingestion_run_id)
    assert ("REVISION_ALREADY_STORED", "INFO") in region_issues and ("STORED_ITEM_ABSENT", "WARNING") not in region_issues
    assert {r.revision_no for r in _rows(conn, REGION)} == {2}


def test_reprocess_is_offline_verifies_checksums_and_recovers_rejected_items(conn, settings, storage, mock, store,
                                                                           tmp_path, manager,
                                                                           monkeypatch) -> None:
    twins = [{"EntityAcronym": "TEST_TWIN", "EntityUnitAcronym": "U", "EntityPxCode": "P", "EntityRegionId": 1,
              "Injection": series(1.111), "Drawal": series(2.222)}] * 2
    mock.set(D, "IEX", "DAM", envelope([px_record(D, 1, entities=entity_items() + twins)]))
    first = _run(conn, settings, storage, mock, store, manager)
    assert (first.parts["entity"].result.records_inserted, first.parts["entity"].result.records_rejected) == (288, 192)
    calls = len(mock.calls)

    # A rule change (stand-in for a parser fix) now accepts the twins; reprocess re-reads raw storage only.
    monkeypatch.setattr("src.pipelines.wbes.pipeline._duplicate_key", lambda record: [])
    offline = _run(conn, settings, storage, mock, TokenStore(tmp_path / "no-session.json"), manager, reprocess=True)
    assert len(mock.calls) == calls and offline.fetch_error is None              # no WBES call, no session needed
    entity = offline.parts["entity"].result
    assert entity.records_inserted == 96                                         # the twin item, loaded once
    assert ("PREVIOUSLY_MISSING_ITEMS_LOADED", "INFO") in _issues(conn, entity.ingestion_run_id)
    assert offline.parts["region"].result.records_inserted == 0                  # stored items are not reloaded

    document = _raw_files(settings, "px")[0]
    document.write_bytes(document.read_bytes().replace(b"TEST_TWIN", b"TEST_TWIX"))   # tamper with the raw file
    with pytest.raises(WbesError, match="SHA-256 does not match"):
        _run(conn, settings, storage, mock, store, manager, reprocess=True)
    with pytest.raises(ValueError, match="no stored WBES PX documents"):
        _run(conn, settings, storage, mock, store, manager, start=date(2026, 11, 1), reprocess=True)


def test_day_not_yet_complete_skips_short_series_without_failing(conn, settings, storage, mock, store,
                                                                manager) -> None:
    today = api.today_ist()
    regions = region_items()
    regions[0]["Injection"], regions[0]["Drawal"] = regions[0]["Injection"][:30], regions[0]["Drawal"][:30]
    mock.set(today, "IEX", "RTM", envelope([px_record(today, 1, regions=regions)]))
    summary = _run(conn, settings, storage, mock, store, manager, start=today, markets=["RTM"], today=today)
    region = summary.parts["region"].result
    assert region.status is RunStatus.SUCCESS and (region.records_inserted, region.records_rejected) == (480, 0)
    assert ("INCOMPLETE_SERIES", "INFO") in _issues(conn, region.ingestion_run_id)


def test_unverified_created_on_timezone_uses_the_later_reading(conn, settings, storage, mock, store,
                                                               manager) -> None:
    default = settings.model_copy(update={"wbes_created_on_naive_timezone": "unverified"})
    mock.set(D, "IEX", "DAM", envelope([px_record(D, 1, created_on="2026-11-09T14:05:31")]))
    summary = _run(conn, default, storage, mock, store, manager)
    rows = _rows(conn, LINK)
    assert {(r.availability_basis, r.source_available_at) for r in rows} == {
        ("SOURCE_CREATED_ON", datetime(2026, 11, 9, 14, 5, 31, tzinfo=timezone.utc))}    # 19:35 IST, not 14:05 IST
    assert ("CREATED_ON_TIMEZONE_UNVERIFIED", "INFO") in _issues(conn, summary.parts["link"].result.ingestion_run_id)


def test_rejects_are_written_once_across_revision_batches(conn, settings, storage, mock, store, manager,
                                                          monkeypatch) -> None:
    from src.ingestion.models import Severity
    from src.loaders.table_loader import LoadIssue, TableLoader

    original_load = TableLoader.load

    def load_with_error(self, connection, items):                               # one load-stage error per batch
        result = original_load(self, connection, items)
        result.issues.append(LoadIssue(f"synthetic-{len(items)}", "TEST_LOAD_ERROR", "synthetic load error",
                                       Severity.ERROR, {"synthetic": True}))
        return result
    monkeypatch.setattr(TableLoader, "load", load_with_error)
    rev1 = px_record(D, 1, created_on="2026-11-09T13:00:00")
    rev2 = px_record(D, 2, created_on="2026-11-09T15:00:00", power_ex_id="PXID-SYN-2")
    mock.set(D, "IEX", "DAM", envelope([rev1, rev2]))                            # two revision batches
    summary = _run(conn, settings, storage, mock, store, manager)
    assert all(p.status is RunStatus.PARTIAL and p.error is None for p in summary.parts.values())
    for dataset in ("wbes_px_region", "wbes_px_link", "wbes_px_entity"):
        rejects = [p for p in (Path(settings.raw_data_dir) / "wbes" / f"{dataset}-rejected").rglob("*.json")
                   if not p.name.endswith(".meta.json")]
        assert len(rejects) == 1
        assert sum(1 for r in json.loads(rejects[0].read_text())["rejected"] if r["stage"] == "load") == 2
