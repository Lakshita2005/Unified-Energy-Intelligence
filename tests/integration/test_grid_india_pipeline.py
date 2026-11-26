"""End-to-end Grid-India Daily PSP tests (sections A, C, J) against the real ``pi`` schema with mocked HTTP.

Every test runs inside one outer transaction that is rolled back; nothing
persists. Assertions only see rows created inside that transaction.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.engine import Connection

from config.settings import Settings
from scripts.ingest_grid_india import main as cli_main
from src.common.checksums import sha256_bytes
from src.common.database import DatabaseManager, SavepointDatabase
from src.common.exceptions import ConfigurationError
from src.common.http_client import HttpClient
from src.common.raw_storage import LocalFileSystemBackend, RawStorageManager
from src.ingestion.models import RunStatus
from src.pipelines.grid_india.api import IST
from src.pipelines.grid_india.reference import seed_countries
from src.pipelines.grid_india.runner import PspRunSummary, run_grid_india_psp
from tests.support.grid_india import API, CDN, AreaRow, FileSpec, MockGridIndia, default_section_c, workbook_bytes

pytestmark = pytest.mark.integration

D = date(2026, 11, 10)
TODAY = date(2026, 11, 13)
TABLES = ("grid_daily_power_supply", "grid_state_power_supply", "grid_sourcewise_generation",
          "grid_generation_outage", "grid_regional_exchange", "grid_interregional_line_exchange",
          "grid_international_line_exchange", "grid_crossborder_market_schedule", "grid_15min_scada")


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
        seed_countries(connection, "pi")  # country reference rows, rolled back with the test
        try:
            yield connection
        finally:
            outer.rollback()


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(raw_data_dir=tmp_path / "raw", log_to_file=False, grid_india_api_base_url=API,
                    grid_india_cdn_base_url=CDN)


@pytest.fixture
def storage(tmp_path: Path) -> RawStorageManager:
    return RawStorageManager(LocalFileSystemBackend(tmp_path / "raw"))


def _run(conn: Connection, settings: Settings, storage: RawStorageManager, mock: MockGridIndia, start: date = D,
         end: date | None = None, **kwargs: Any) -> PspRunSummary:
    client = HttpClient(settings=settings, transport=httpx.MockTransport(mock), sleep=lambda _: None)
    return run_grid_india_psp(start_date=start, end_date=end, settings=settings, http_client=client,
                              database=SavepointDatabase(conn), raw_storage=storage, today=kwargs.pop("today", TODAY),
                              sleep=lambda _: None, **kwargs)


def _rows(conn: Connection, table: str, order: str = "id") -> list[Any]:
    return conn.execute(text(f"SELECT * FROM pi.{table} WHERE id > :b ORDER BY {order}"),
                        {"b": conn.info[table]}).all()


def _issues(conn: Connection, run_id: int) -> list[tuple[str, str]]:
    return [tuple(r) for r in conn.execute(text(
        "SELECT issue_code, severity FROM pi.data_quality_issue WHERE ingestion_run_id = :r ORDER BY 1, 2"),
        {"r": run_id})]


def _workbook_files(settings: Settings) -> list[Path]:
    return sorted((Path(settings.raw_data_dir) / "grid_india" / "grid_india_psp_workbook" / "xls").rglob("*.xls"))


# ------------------------------------------------------------------- happy path

def test_one_download_feeds_both_sections(conn: Connection, settings: Settings, storage: RawStorageManager) -> None:
    mock = MockGridIndia([FileSpec(D, workbook_bytes(D))])

    summary = _run(conn, settings, storage, mock)

    assert summary.ok and summary.workbooks == ["10.11.26_NLDC_PSP_247.xls"] and summary.missing == []
    assert mock.downloads == [f"{CDN}/files/grdw/2026/11/10.11.26_NLDC_PSP_247.xls"]  # downloaded once
    files = _workbook_files(settings)
    assert len(files) == 1  # stored once
    supply, states = summary.sections["supply"].result, summary.sections["states"].result
    assert supply.ingestion_run_id != states.ingestion_run_id  # one run per section
    assert (supply.records_inserted, states.records_inserted) == (6, 40)

    docs = conn.execute(text("SELECT d.ingestion_run_id, s.dataset_code, d.checksum_sha256, d.raw_storage_uri, "
                             "d.published_at, d.source_report_date FROM pi.source_document d JOIN pi.source_dataset s "
                             "USING (dataset_id) WHERE d.ingestion_run_id IN (:a, :b) ORDER BY 2"),
                        {"a": supply.ingestion_run_id, "b": states.ingestion_run_id}).all()
    assert [d.dataset_code for d in docs] == ["GRID_INDIA_PSP_DAILY", "GRID_INDIA_PSP_STATES"]
    assert docs[0].checksum_sha256 == docs[1].checksum_sha256 == sha256_bytes(files[0].read_bytes())
    assert docs[0].raw_storage_uri == docs[1].raw_storage_uri
    published = datetime(2026, 11, 11, 10, 40, tzinfo=IST)
    assert docs[0].published_at == published and docs[0].source_report_date == D

    supply_rows = {r.region_code: r for r in _rows(conn, "grid_daily_power_supply")}
    assert set(supply_rows) == {"NR", "WR", "SR", "ER", "NER", None}
    all_india = supply_rows[None]
    assert (all_india.energy_met_mu, all_india.max_demand_met_mw, all_india.max_demand_met_time) == (
        Decimal("4250.7500"), Decimal("212345.000"), time(9, 28))
    assert (all_india.availability_basis, all_india.source_published_at, all_india.source_available_at) == (
        "PUBLISHED", published, published)
    assert all_india.document_id == docs[0][0] or all_india.document_id is not None

    states_rows = {r.area_code: r for r in _rows(conn, "grid_state_power_supply")}
    dh = states_rows["DH"]
    assert (dh.area_type, dh.state_code, dh.area_name, dh.region_code, dh.energy_met_mu) == (
        "STATE", "DH", "DNHDDPDCL", "WR", Decimal("27.5000"))
    assert states_rows["DVC"].state_code is None and states_rows["JK_LA"].area_type == "SYSTEM"
    assert states_rows["BALCO"].area_type == "ENTITY"
    assert _issues(conn, supply.ingestion_run_id) == [] and _issues(conn, states.ingestion_run_id) == []


def test_reports_before_2025_05_12_get_estimated_availability(conn: Connection, settings: Settings,
                                                              storage: RawStorageManager) -> None:
    day = date(2024, 12, 15)
    bulk_reupload = datetime(2025, 4, 15, 9, 10)
    mock = MockGridIndia([FileSpec(day, workbook_bytes(day), bulk_reupload)])

    summary = _run(conn, settings, storage, mock, start=day)

    assert summary.ok
    row = next(r for r in _rows(conn, "grid_daily_power_supply") if r.region_code is None)
    assert row.availability_basis == "ESTIMATED_D_PLUS_2_0900" and row.source_published_at is None
    assert row.source_available_at == datetime(2024, 12, 17, 9, 0, tzinfo=IST)
    doc = conn.execute(text("SELECT published_at FROM pi.source_document WHERE document_id = :d"),
                       {"d": row.document_id}).scalar()
    assert doc is None  # the bulk re-upload time is not presented as the publication time
    meta_files = list((Path(settings.raw_data_dir) / "grid_india").rglob("*.xls.meta.json"))
    assert meta_files and "15-04-2025 09:10" in meta_files[0].read_text(encoding="utf-8")  # kept in raw metadata


def test_rerun_inserts_nothing_and_reprocess_finds_nothing_new(conn: Connection, settings: Settings,
                                                               storage: RawStorageManager) -> None:
    mock = MockGridIndia([FileSpec(D, workbook_bytes(D))])
    _run(conn, settings, storage, mock)

    again = _run(conn, settings, storage, mock)
    for section in again.sections.values():
        assert section.status is RunStatus.SUCCESS
        assert (section.result.duplicate_documents, section.result.records_inserted) == (1, 0)

    reprocessed = _run(conn, settings, storage, mock, reprocess=True)
    assert reprocessed.sections["supply"].result.records_unchanged == 6
    assert reprocessed.sections["states"].result.records_unchanged == 40
    assert all(s.result.records_inserted == 0 for s in reprocessed.sections.values())
    assert len(_rows(conn, "grid_daily_power_supply")) == 6 and len(_rows(conn, "grid_state_power_supply")) == 40


def test_same_day_revision_is_a_new_version(conn: Connection, settings: Settings, storage: RawStorageManager) -> None:
    original = FileSpec(D, workbook_bytes(D), datetime(2026, 11, 11, 10, 47), suffix="269")
    revised = FileSpec(D, workbook_bytes(D, energy_scale=1.01), datetime(2026, 11, 11, 16, 38), suffix="269_611")
    mock = MockGridIndia([revised, original])  # listing order does not matter

    summary = _run(conn, settings, storage, mock)

    supply = summary.sections["supply"].result
    assert summary.ok and supply.documents_processed == 2
    assert (supply.records_inserted, supply.records_unchanged) == (12, 0)  # every region's energy changed
    nr = [r for r in _rows(conn, "grid_daily_power_supply") if r.region_code == "NR"]
    assert [r.energy_met_mu for r in nr] == [Decimal("1100.5000"), Decimal("1111.5050")]
    assert [r.source_available_at.astimezone(IST).time() for r in nr] == [time(10, 47), time(16, 38)]
    states = summary.sections["states"].result
    assert (states.records_inserted, states.records_unchanged) == (40, 40)  # states unchanged in the revision


def test_report_listed_in_the_following_month_is_found(conn: Connection, settings: Settings,
                                                       storage: RawStorageManager) -> None:
    day = date(2026, 10, 31)
    mock = MockGridIndia([FileSpec(day, workbook_bytes(day), listed_month=date(2026, 11, 1))])
    assert _run(conn, settings, storage, mock, start=day).ok


# ------------------------------------------------------ quality and failures

def test_missing_section_fails_only_that_section(conn: Connection, settings: Settings,
                                                 storage: RawStorageManager) -> None:
    mock = MockGridIndia([FileSpec(D, workbook_bytes(D, omit_section="C"))])

    summary = _run(conn, settings, storage, mock)

    assert not summary.ok
    assert summary.sections["supply"].status is RunStatus.SUCCESS
    assert summary.sections["states"].status is RunStatus.FAILED
    assert "section C not found" in summary.sections["states"].error
    assert len(_rows(conn, "grid_daily_power_supply")) == 6 and _rows(conn, "grid_state_power_supply") == []
    status = conn.execute(text("SELECT run_status FROM pi.ingestion_run WHERE ingestion_run_id = :r"),
                          {"r": summary.sections["states"].result.ingestion_run_id}).scalar()
    assert status == "FAILED"


@pytest.mark.parametrize("break_it, error", [
    (lambda m: setattr(m, "listing_status", 500), "HTTP 500"),
    (lambda m: m.file_overrides.update({m.files[0].path: lambda r: httpx.Response(200, text="<!doctype html>x")}),
     "HTML page"),
])
def test_fetch_failure_fails_every_section(conn: Connection, settings: Settings, storage: RawStorageManager,
                                           break_it: Any, error: str) -> None:
    mock = MockGridIndia([FileSpec(D, workbook_bytes(D))])
    break_it(mock)

    summary = _run(conn, settings, storage, mock)

    assert summary.fetch_error and error in summary.fetch_error
    assert all(s.status is RunStatus.FAILED and error in (s.error or "") for s in summary.sections.values())
    assert _rows(conn, "grid_daily_power_supply") == [] and _rows(conn, "grid_state_power_supply") == []


def test_missing_days_are_recorded_in_each_section(conn: Connection, settings: Settings,
                                                   storage: RawStorageManager) -> None:
    mock = MockGridIndia([FileSpec(D, workbook_bytes(D))])

    summary = _run(conn, settings, storage, mock, start=date(2026, 11, 9), end=date(2026, 11, 12))

    assert summary.missing == ["2026-11-09", "2026-11-11", "2026-11-12"]
    for section in summary.sections.values():
        assert section.status is RunStatus.SUCCESS
        missing_day_issues = [i for i in _issues(conn, section.result.ingestion_run_id) if i[0] == "MISSING_DAY"]
        assert missing_day_issues == [("MISSING_DAY", "INFO"), ("MISSING_DAY", "WARNING"), ("MISSING_DAY", "WARNING")]


def test_unmapped_area_is_rejected_and_recorded(conn: Connection, settings: Settings,
                                                storage: RawStorageManager) -> None:
    rows = default_section_c() + [AreaRow("", "Mystery Captive Ltd")]
    mock = MockGridIndia([FileSpec(D, workbook_bytes(D, section_c=rows))])

    summary = _run(conn, settings, storage, mock)

    states = summary.sections["states"]
    assert states.status is RunStatus.PARTIAL and states.result.records_rejected == 1
    assert ("UNMAPPED_ENTITY", "ERROR") in _issues(conn, states.result.ingestion_run_id)
    assert summary.sections["supply"].status is RunStatus.SUCCESS and not summary.ok


def test_total_mismatches_are_warnings_and_published_totals_are_kept(conn: Connection, settings: Settings,
                                                                     storage: RawStorageManager) -> None:
    rows = default_section_c()
    bulk_wr = next(r for r in rows if r.name == "Bulk Consumer_WR ISTS")
    bulk_wr.energy = 31.0  # section J entities add up to 30.0
    workbook = workbook_bytes(D, section_c=rows, total_override={"Energy Met (MU)": 4300.0})
    summary = _run(conn, settings, storage, MockGridIndia([FileSpec(D, workbook)]))

    assert summary.ok
    supply_issues = conn.execute(text("SELECT issue_code, severity, issue_description FROM pi.data_quality_issue "
                                      "WHERE ingestion_run_id = :r"),
                                 {"r": summary.sections["supply"].result.ingestion_run_id}).all()
    assert [(i.issue_code, i.severity) for i in supply_issues] == [("TOTAL_MISMATCH", "WARNING")]
    assert "4250.7500" in supply_issues[0].issue_description and "4300.0000" in supply_issues[0].issue_description
    assert next(r for r in _rows(conn, "grid_daily_power_supply") if r.region_code is None).energy_met_mu == \
        Decimal("4300.0000")
    assert _issues(conn, summary.sections["states"].result.ingestion_run_id) == [("TOTAL_MISMATCH", "WARNING")]


def test_cli_rejects_bad_arguments(settings: Settings) -> None:
    assert cli_main(["psp", "--date", "2022-06-01"], settings=settings) == 2
    assert cli_main(["psp", "--date", "2026-11-10", "--sections", "nonsense"], settings=settings) == 2
    with pytest.raises(SystemExit):
        cli_main(["psp", "--start-date", "2026-11-01"], settings=settings)


def test_reprocessing_a_revised_day_adds_no_versions(conn: Connection, settings: Settings,
                                                     storage: RawStorageManager) -> None:
    original = FileSpec(D, workbook_bytes(D), datetime(2026, 11, 11, 10, 47), suffix="269")
    revised = FileSpec(D, workbook_bytes(D, energy_scale=1.01), datetime(2026, 11, 11, 16, 38), suffix="269_611")
    mock = MockGridIndia([original, revised])
    _run(conn, settings, storage, mock)
    before = len(_rows(conn, "grid_daily_power_supply"))

    reprocessed = _run(conn, settings, storage, mock, reprocess=True)

    supply = reprocessed.sections["supply"].result
    assert reprocessed.ok and (supply.records_inserted, supply.records_unchanged) == (0, 12)
    assert len(_rows(conn, "grid_daily_power_supply")) == before == 12
    assert ("REPROCESSED_DOCUMENT", "INFO") in _issues(conn, supply.ingestion_run_id)


# ------------------------------------------------------------------ section G

from tests.support.grid_india import SECTION_G  # noqa: E402

FUEL = "grid_sourcewise_generation"


def test_section_g_end_to_end_with_document_and_checksum_linkage(conn: Connection, settings: Settings,
                                                                  storage: RawStorageManager) -> None:
    mock = MockGridIndia([FileSpec(D, workbook_bytes(D))])

    summary = _run(conn, settings, storage, mock)

    fuel = summary.sections["fuel"]
    assert summary.ok and fuel.status is RunStatus.SUCCESS and fuel.result.records_inserted == 30
    assert len(mock.downloads) == 1 and len(_workbook_files(settings)) == 1  # shared download, one raw copy
    rows = _rows(conn, FUEL, "region_code, fuel_type")
    assert len(rows) == 30 and {r.region_code for r in rows} == {"NR", "WR", "SR", "ER", "NER"}
    coal_nr = next(r for r in rows if r.region_code == "NR" and r.fuel_type == "COAL")
    assert (coal_nr.fuel_label, coal_nr.gross_generation_mu, coal_nr.report_date) == ("Coal", Decimal("712.4200"), D)
    assert {r.fuel_label for r in rows if r.fuel_type == "GAS_NAPHTHA_DIESEL"} == {"Gas, Naptha & Diesel"}
    published = datetime(2026, 11, 11, 10, 40, tzinfo=IST)
    assert {(r.availability_basis, r.source_published_at, r.source_available_at) for r in rows} == {
        ("PUBLISHED", published, published)}
    assert {r.ingestion_run_id for r in rows} == {fuel.result.ingestion_run_id}
    doc = conn.execute(text("SELECT d.document_id, s.dataset_code, d.checksum_sha256, d.ingestion_run_id "
                            "FROM pi.source_document d JOIN pi.source_dataset s USING (dataset_id) "
                            "WHERE d.document_id = :d"), {"d": coal_nr.document_id}).one()
    assert doc.dataset_code == "GRID_INDIA_GENERATION" and doc.ingestion_run_id == fuel.result.ingestion_run_id
    assert doc.checksum_sha256 == sha256_bytes(_workbook_files(settings)[0].read_bytes())
    assert _issues(conn, fuel.result.ingestion_run_id) == []


def test_section_g_estimated_and_published_availability_around_cutoff(conn: Connection, settings: Settings,
                                                                      storage: RawStorageManager) -> None:
    before, after = date(2025, 5, 11), date(2025, 5, 12)
    mock = MockGridIndia([FileSpec(before, workbook_bytes(before), datetime(2025, 4, 15, 9, 10)),
                          FileSpec(after, workbook_bytes(after), datetime(2025, 5, 13, 17, 12))])

    summary = _run(conn, settings, storage, mock, start=before, end=after)

    assert summary.ok
    by_day = {(r.report_date, r.availability_basis, r.source_published_at, r.source_available_at)
              for r in _rows(conn, FUEL)}
    assert by_day == {(before, "ESTIMATED_D_PLUS_2_0900", None, datetime(2025, 5, 13, 9, 0, tzinfo=IST)),
                      (after, "PUBLISHED", datetime(2025, 5, 13, 17, 12, tzinfo=IST),
                       datetime(2025, 5, 13, 17, 12, tzinfo=IST))}


def test_missing_section_g_fails_only_the_fuel_section(conn: Connection, settings: Settings,
                                                       storage: RawStorageManager) -> None:
    summary = _run(conn, settings, storage, MockGridIndia([FileSpec(D, workbook_bytes(D, omit_section="G"))]))

    assert not summary.ok
    assert summary.sections["fuel"].status is RunStatus.FAILED and "section G not found" in summary.sections["fuel"].error
    assert summary.sections["supply"].status is RunStatus.SUCCESS
    assert summary.sections["states"].status is RunStatus.SUCCESS
    assert _rows(conn, FUEL) == [] and len(_rows(conn, "grid_daily_power_supply")) == 6


def test_unknown_fuel_label_is_rejected_and_recorded(conn: Connection, settings: Settings,
                                                     storage: RawStorageManager) -> None:
    fuels = SECTION_G + [("Hydrogen", [1.0, 1.0, 1.0, 1.0, 1.0])]
    summary = _run(conn, settings, storage, MockGridIndia([FileSpec(D, workbook_bytes(D, section_g=fuels))]))

    fuel = summary.sections["fuel"]
    assert fuel.status is RunStatus.PARTIAL and (fuel.result.records_inserted, fuel.result.records_rejected) == (30, 5)
    assert _issues(conn, fuel.result.ingestion_run_id) == [("UNMAPPED_FUEL", "ERROR")] * 5
    assert not any(r.fuel_label == "Hydrogen" for r in _rows(conn, FUEL))


def test_section_g_total_mismatches_are_warnings(conn: Connection, settings: Settings,
                                                 storage: RawStorageManager) -> None:
    workbook = workbook_bytes(D, g_all_india_override={"Coal": 3489.0}, g_total_override={"WR": 1743.72})
    summary = _run(conn, settings, storage, MockGridIndia([FileSpec(D, workbook)]))

    fuel = summary.sections["fuel"]
    assert fuel.status is RunStatus.SUCCESS and fuel.result.records_inserted == 30
    descriptions = conn.execute(text("SELECT issue_description FROM pi.data_quality_issue WHERE ingestion_run_id = :r "
                                     "AND issue_code = 'TOTAL_MISMATCH' AND severity = 'WARNING'"),
                                {"r": fuel.result.ingestion_run_id}).scalars().all()
    assert len(descriptions) == 3 and any("3489.0000" in d for d in descriptions)


def test_section_g_rerun_revision_and_reprocess(conn: Connection, settings: Settings,
                                                storage: RawStorageManager) -> None:
    original = FileSpec(D, workbook_bytes(D), datetime(2026, 11, 11, 10, 47), suffix="269")
    revised = FileSpec(D, workbook_bytes(D, coal_scale=1.01), datetime(2026, 11, 11, 16, 38), suffix="269_611")
    mock = MockGridIndia([original, revised])

    first = _run(conn, settings, storage, mock).sections["fuel"].result
    assert (first.records_inserted, first.records_unchanged) == (35, 25)  # 30 + 5 revised coal values
    coal_nr = [r for r in _rows(conn, FUEL) if r.region_code == "NR" and r.fuel_type == "COAL"]
    assert [r.gross_generation_mu for r in coal_nr] == [Decimal("712.4200"), Decimal("719.5440")]
    assert [r.source_available_at.astimezone(IST).time() for r in coal_nr] == [time(10, 47), time(16, 38)]

    rerun = _run(conn, settings, storage, mock).sections["fuel"].result
    assert (rerun.duplicate_documents, rerun.records_inserted) == (2, 0)

    reprocessed = _run(conn, settings, storage, mock, reprocess=True).sections["fuel"].result
    assert (reprocessed.records_inserted, reprocessed.records_unchanged) == (0, 60)
    assert len(_rows(conn, FUEL)) == 35


# ------------------------------------------------------------------ section F

from tests.support.grid_india import SECTION_F  # noqa: E402

OUTAGE = "grid_generation_outage"


def test_section_f_end_to_end_with_document_and_checksum_linkage(conn: Connection, settings: Settings,
                                                                  storage: RawStorageManager) -> None:
    mock = MockGridIndia([FileSpec(D, workbook_bytes(D))])

    summary = _run(conn, settings, storage, mock)

    outage = summary.sections["outage"]
    assert summary.ok and outage.status is RunStatus.SUCCESS and outage.result.records_inserted == 10
    assert len(mock.downloads) == 1 and len(_workbook_files(settings)) == 1
    rows = _rows(conn, OUTAGE, "region_code, sector")
    assert len(rows) == 10 and {r.region_code for r in rows} == {"NR", "WR", "SR", "ER", "NER"}
    wr = next(r for r in rows if r.region_code == "WR" and r.sector == "CENTRAL")
    assert (wr.outage_capacity_mw, wr.plant_id, wr.unit_id, wr.outage_reason) == (Decimal("9636.613"), None, None, None)
    published = datetime(2026, 11, 11, 10, 40, tzinfo=IST)
    assert {(r.availability_basis, r.source_published_at, r.source_available_at) for r in rows} == {
        ("PUBLISHED", published, published)}
    doc = conn.execute(text("SELECT s.dataset_code, d.checksum_sha256, d.ingestion_run_id FROM pi.source_document d "
                            "JOIN pi.source_dataset s USING (dataset_id) WHERE d.document_id = :d"),
                       {"d": wr.document_id}).one()
    assert doc.dataset_code == "GRID_INDIA_PSP_OUTAGE" and doc.ingestion_run_id == outage.result.ingestion_run_id
    assert doc.checksum_sha256 == sha256_bytes(_workbook_files(settings)[0].read_bytes())
    assert _issues(conn, outage.result.ingestion_run_id) == []


def test_section_f_availability_around_cutoff(conn: Connection, settings: Settings,
                                              storage: RawStorageManager) -> None:
    before, after = date(2025, 5, 11), date(2025, 5, 12)
    mock = MockGridIndia([FileSpec(before, workbook_bytes(before), datetime(2025, 4, 15, 9, 10)),
                          FileSpec(after, workbook_bytes(after), datetime(2025, 5, 13, 17, 12))])
    assert _run(conn, settings, storage, mock, start=before, end=after).ok
    assert {(r.report_date, r.availability_basis, r.source_published_at, r.source_available_at)
            for r in _rows(conn, OUTAGE)} == {
        (before, "ESTIMATED_D_PLUS_2_0900", None, datetime(2025, 5, 13, 9, 0, tzinfo=IST)),
        (after, "PUBLISHED", datetime(2025, 5, 13, 17, 12, tzinfo=IST), datetime(2025, 5, 13, 17, 12, tzinfo=IST))}


def test_missing_section_f_fails_only_the_outage_section(conn: Connection, settings: Settings,
                                                         storage: RawStorageManager) -> None:
    summary = _run(conn, settings, storage, MockGridIndia([FileSpec(D, workbook_bytes(D, omit_section="F"))]))
    assert summary.sections["outage"].status is RunStatus.FAILED
    assert "section F not found" in summary.sections["outage"].error
    assert all(summary.sections[s].status is RunStatus.SUCCESS for s in ("supply", "states", "fuel"))
    assert _rows(conn, OUTAGE) == []


def test_unknown_sector_is_rejected_and_total_mismatch_warned(conn: Connection, settings: Settings,
                                                              storage: RawStorageManager) -> None:
    sectors = SECTION_F + [("Private Sector", [1.5, 1.5, 1.5, 1.5, 1.5])]
    summary = _run(conn, settings, storage, MockGridIndia([FileSpec(D, workbook_bytes(D, section_f=sectors))]))
    outage = summary.sections["outage"]
    assert outage.status is RunStatus.PARTIAL
    assert (outage.result.records_inserted, outage.result.records_rejected) == (10, 5)
    assert _issues(conn, outage.result.ingestion_run_id) == [("UNMAPPED_SECTOR", "ERROR")] * 5

    day = date(2026, 11, 9)
    workbook = workbook_bytes(day, f_total_override={"ER": 7000.0})
    second = _run(conn, settings, storage, MockGridIndia([FileSpec(day, workbook)]), start=day).sections["outage"]
    assert second.status is RunStatus.SUCCESS
    assert _issues(conn, second.result.ingestion_run_id) == [("TOTAL_MISMATCH", "WARNING")]  # ER only


def test_section_f_rerun_revision_and_reprocess(conn: Connection, settings: Settings,
                                                storage: RawStorageManager) -> None:
    revised_sectors = [("Central Sector", [3927.041, 9036.613, 7368.017, 4030.019, 434.523]),  # WR revised
                       SECTION_F[1]]
    original = FileSpec(D, workbook_bytes(D), datetime(2026, 11, 11, 10, 47), suffix="269")
    revised = FileSpec(D, workbook_bytes(D, section_f=revised_sectors), datetime(2026, 11, 11, 16, 38),
                       suffix="269_611")
    mock = MockGridIndia([original, revised])

    first = _run(conn, settings, storage, mock).sections["outage"].result
    assert (first.records_inserted, first.records_unchanged) == (11, 9)
    wr = [r for r in _rows(conn, OUTAGE) if r.region_code == "WR" and r.sector == "CENTRAL"]
    assert [r.outage_capacity_mw for r in wr] == [Decimal("9636.613"), Decimal("9036.613")]
    assert [r.source_available_at.astimezone(IST).time() for r in wr] == [time(10, 47), time(16, 38)]

    rerun = _run(conn, settings, storage, mock).sections["outage"].result
    assert (rerun.duplicate_documents, rerun.records_inserted) == (2, 0)
    reprocessed = _run(conn, settings, storage, mock, reprocess=True).sections["outage"].result
    assert (reprocessed.records_inserted, reprocessed.records_unchanged) == (0, 20)
    assert len(_rows(conn, OUTAGE)) == 11


# ------------------------------------------------------------------ section E

from tests.support.grid_india import SECTION_E  # noqa: E402

EXCHANGE = "grid_regional_exchange"


def test_section_e_end_to_end_with_document_and_checksum_linkage(conn: Connection, settings: Settings,
                                                                  storage: RawStorageManager) -> None:
    mock = MockGridIndia([FileSpec(D, workbook_bytes(D))])

    summary = _run(conn, settings, storage, mock)

    exchange = summary.sections["exchange"]
    assert summary.ok and exchange.status is RunStatus.SUCCESS and exchange.result.records_inserted == 5
    assert len(mock.downloads) == 1 and len(_workbook_files(settings)) == 1
    rows = {r.region_code: r for r in _rows(conn, EXCHANGE)}
    assert set(rows) == {"NR", "WR", "SR", "ER", "NER"}
    er = rows["ER"]
    assert (er.scheduled_exchange_mu, er.actual_exchange_mu, er.od_ud_mu) == (
        Decimal("-162.4010"), Decimal("-184.5930"), Decimal("-22.2000"))
    assert er.exchange_direction is None and er.sign_convention == "Import(+ve)/Export(-ve); OD(+)/UD(-)"
    assert rows["NR"].scheduled_exchange_mu == Decimal("121.4830")  # import positive, as published
    published = datetime(2026, 11, 11, 10, 40, tzinfo=IST)
    assert {(r.availability_basis, r.source_published_at, r.source_available_at) for r in rows.values()} == {
        ("PUBLISHED", published, published)}
    doc = conn.execute(text("SELECT s.dataset_code, d.checksum_sha256, d.ingestion_run_id FROM pi.source_document d "
                            "JOIN pi.source_dataset s USING (dataset_id) WHERE d.document_id = :d"),
                       {"d": er.document_id}).one()
    assert doc.dataset_code == "GRID_INDIA_EXCHANGE" and doc.ingestion_run_id == exchange.result.ingestion_run_id
    assert doc.checksum_sha256 == sha256_bytes(_workbook_files(settings)[0].read_bytes())
    assert _issues(conn, exchange.result.ingestion_run_id) == []


def test_section_e_availability_around_cutoff(conn: Connection, settings: Settings,
                                              storage: RawStorageManager) -> None:
    before, after = date(2025, 5, 11), date(2025, 5, 12)
    mock = MockGridIndia([FileSpec(before, workbook_bytes(before), datetime(2025, 4, 15, 9, 10)),
                          FileSpec(after, workbook_bytes(after), datetime(2025, 5, 13, 17, 12))])
    assert _run(conn, settings, storage, mock, start=before, end=after).ok
    assert {(r.report_date, r.availability_basis, r.source_published_at, r.source_available_at)
            for r in _rows(conn, EXCHANGE)} == {
        (before, "ESTIMATED_D_PLUS_2_0900", None, datetime(2025, 5, 13, 9, 0, tzinfo=IST)),
        (after, "PUBLISHED", datetime(2025, 5, 13, 17, 12, tzinfo=IST), datetime(2025, 5, 13, 17, 12, tzinfo=IST))}


def test_missing_section_e_fails_only_the_exchange_section(conn: Connection, settings: Settings,
                                                           storage: RawStorageManager) -> None:
    summary = _run(conn, settings, storage, MockGridIndia([FileSpec(D, workbook_bytes(D, omit_section="E"))]))
    assert summary.sections["exchange"].status is RunStatus.FAILED
    assert "section E not found" in summary.sections["exchange"].error
    assert all(summary.sections[s].status is RunStatus.SUCCESS for s in ("supply", "states", "fuel", "outage"))
    assert _rows(conn, EXCHANGE) == []


def test_section_e_invalid_value_unknown_row_and_discrepancies(conn: Connection, settings: Settings,
                                                              storage: RawStorageManager) -> None:
    rows = [SECTION_E[0], ("Actual(MU)", [125.307, -235.941, 275.003, "n/a", 2.817]),
            ("O/D/U/D(MU)", [3.82, 1.55, 0.86, -22.2, -9.0]), ("Net Transfer(MU)", [1, 1, 1, 1, 1])]
    summary = _run(conn, settings, storage, MockGridIndia([FileSpec(D, workbook_bytes(D, section_e=rows))]))
    exchange = summary.sections["exchange"]
    assert exchange.status is RunStatus.PARTIAL
    assert (exchange.result.records_inserted, exchange.result.records_rejected) == (4, 1)
    issues = _issues(conn, exchange.result.ingestion_run_id)
    assert ("PARSE_ERROR", "ERROR") in issues and ("UNKNOWN_LABEL", "WARNING") in issues
    assert ("OD_UD_MISMATCH", "WARNING") in issues  # NER: published -9.0 vs Actual - Schedule

    day = date(2026, 11, 9)
    second = _run(conn, settings, storage,
                  MockGridIndia([FileSpec(day, workbook_bytes(day, e_total_override={"Schedule(MU)": 5.0}))]),
                  start=day).sections["exchange"]
    assert second.status is RunStatus.SUCCESS
    # the TOTAL mismatch, and the TOTAL O/D/U/D no longer equal to Actual - Schedule
    assert _issues(conn, second.result.ingestion_run_id) == [("OD_UD_MISMATCH", "WARNING"), ("TOTAL_MISMATCH", "WARNING")]


def test_section_e_rerun_revision_and_reprocess(conn: Connection, settings: Settings,
                                                storage: RawStorageManager) -> None:
    revised_rows = [SECTION_E[0], ("Actual(MU)", [125.307, -235.941, 275.003, -190.593, 2.817]),
                    ("O/D/U/D(MU)", [3.82, 1.55, 0.86, -28.19, -1.46])]  # ER revised
    original = FileSpec(D, workbook_bytes(D), datetime(2026, 11, 11, 10, 47), suffix="269")
    revised = FileSpec(D, workbook_bytes(D, section_e=revised_rows), datetime(2026, 11, 11, 16, 38), suffix="269_611")
    mock = MockGridIndia([original, revised])

    first = _run(conn, settings, storage, mock).sections["exchange"].result
    assert (first.records_inserted, first.records_unchanged) == (6, 4)
    er = [r for r in _rows(conn, EXCHANGE) if r.region_code == "ER"]
    assert [(r.actual_exchange_mu, r.od_ud_mu) for r in er] == [(Decimal("-184.5930"), Decimal("-22.2000")),
                                                                 (Decimal("-190.5930"), Decimal("-28.1900"))]
    assert [r.source_available_at.astimezone(IST).time() for r in er] == [time(10, 47), time(16, 38)]

    rerun = _run(conn, settings, storage, mock).sections["exchange"].result
    assert (rerun.duplicate_documents, rerun.records_inserted) == (2, 0)
    reprocessed = _run(conn, settings, storage, mock, reprocess=True).sections["exchange"].result
    assert (reprocessed.records_inserted, reprocessed.records_unchanged) == (0, 10)
    assert len(_rows(conn, EXCHANGE)) == 6


# ------------------------------------------------------------------ IR-Line

from tests.support.grid_india import INTL_ROWS, IR_BLOCKS  # noqa: E402

IR = "grid_interregional_line_exchange"
INTL = "grid_international_line_exchange"


def _doc(conn: Connection, document_id: int) -> Any:
    return conn.execute(text("SELECT s.dataset_code, d.checksum_sha256, d.ingestion_run_id FROM pi.source_document d "
                             "JOIN pi.source_dataset s USING (dataset_id) WHERE d.document_id = :d"),
                        {"d": document_id}).one()


def test_ir_lines_end_to_end(conn: Connection, settings: Settings, storage: RawStorageManager) -> None:
    mock = MockGridIndia([FileSpec(D, workbook_bytes(D))])

    summary = _run(conn, settings, storage, mock)

    ir, intl = summary.sections["ir_lines"], summary.sections["intl_lines"]
    assert summary.ok and ir.status is RunStatus.SUCCESS and intl.status is RunStatus.SUCCESS
    assert ir.result.ingestion_run_id != intl.result.ingestion_run_id
    assert (ir.result.records_inserted, intl.result.records_inserted) == (6, 9)
    assert len(mock.downloads) == 1 and len(_workbook_files(settings)) == 1
    checksum = sha256_bytes(_workbook_files(settings)[0].read_bytes())
    published = datetime(2026, 11, 11, 10, 40, tzinfo=IST)

    lines = {r.line_name: r for r in _rows(conn, IR)}
    gaya = lines["TEST GAYA-VARANASI"]
    assert (gaya.from_region_code, gaya.to_region_code, gaya.voltage_level, gaya.circuits, gaya.import_mu,
            gaya.export_mu, gaya.net_exchange_mu) == ("ER", "NR", "765 kV", "2", Decimal("0.0130"),
                                                      Decimal("17.2170"), Decimal("-17.2040"))
    assert gaya.direction_convention == "Import=(+ve) /Export =(-ve) for NET (MU)" and lines["TEST PUSAULI B/B"].circuits is None
    doc = _doc(conn, gaya.document_id)
    assert (doc.dataset_code, doc.checksum_sha256, doc.ingestion_run_id) == (
        "GRID_INDIA_PSP_IR_LINES", checksum, ir.result.ingestion_run_id)
    assert {(r.availability_basis, r.source_published_at) for r in lines.values()} == {("PUBLISHED", published)}
    assert _issues(conn, ir.result.ingestion_run_id) == [("SUBTOTAL_EXCLUDES_LINE", "INFO")]

    intl_rows = {r.line_name: r for r in _rows(conn, INTL)}
    godda = intl_rows["400kV GODDA_TPS-RAHANPUR (B'DESH) D/C"]
    assert (godda.country_code, godda.region_code, godda.region_label, godda.energy_exchange_mu, godda.direction,
            godda.sign_convention) == ("BGD", "ER", "ER (Isolated from Indian Grid)", Decimal("-20.7210"), None,
                                       "Import(+ve)/Export(-ve)")
    assert {r.country_code for r in intl_rows.values()} == {"BTN", "NPL", "BGD"}
    doc = _doc(conn, godda.document_id)
    assert (doc.dataset_code, doc.checksum_sha256, doc.ingestion_run_id) == (
        "GRID_INDIA_PSP_INTL_LINES", checksum, intl.result.ingestion_run_id)
    assert _issues(conn, intl.result.ingestion_run_id) == [("MW_NOT_REPORTED", "INFO")] * 2


def test_ir_lines_availability_around_cutoff(conn: Connection, settings: Settings,
                                             storage: RawStorageManager) -> None:
    before, after = date(2025, 5, 11), date(2025, 5, 12)
    mock = MockGridIndia([FileSpec(before, workbook_bytes(before), datetime(2025, 4, 15, 9, 10)),
                          FileSpec(after, workbook_bytes(after), datetime(2025, 5, 13, 17, 12))])
    assert _run(conn, settings, storage, mock, start=before, end=after).ok
    expected = {(before, "ESTIMATED_D_PLUS_2_0900", None, datetime(2025, 5, 13, 9, 0, tzinfo=IST)),
                (after, "PUBLISHED", datetime(2025, 5, 13, 17, 12, tzinfo=IST), datetime(2025, 5, 13, 17, 12, tzinfo=IST))}
    for table in (IR, INTL):
        assert {(r.report_date, r.availability_basis, r.source_published_at, r.source_available_at)
                for r in _rows(conn, table)} == expected


def test_missing_ir_line_sheet_fails_only_the_ir_sections(conn: Connection, settings: Settings,
                                                          storage: RawStorageManager) -> None:
    summary = _run(conn, settings, storage, MockGridIndia([FileSpec(D, workbook_bytes(D, omit_section="IR"))]))
    for name in ("ir_lines", "intl_lines"):
        assert summary.sections[name].status is RunStatus.FAILED and "IR-Line" in summary.sections[name].error
    assert all(summary.sections[s].status is RunStatus.SUCCESS for s in ("supply", "states", "fuel", "outage", "exchange"))
    assert _rows(conn, IR) == [] and _rows(conn, INTL) == []


def test_unknown_international_line_and_label_conflict_are_rejected(conn: Connection, settings: Settings,
                                                                    storage: RawStorageManager) -> None:
    rows = list(INTL_ROWS)
    rows[2] = ("", "NER", "132kV NEW BORDER LINE", -1.0, 0.0, -1.0, -0.5)
    rows[4] = ("BHUTAN", "NR", "132kV MAHENDRANAGAR-TANAKPUR(NHPC)", 68.761, 0.0, 61.013, 1.471)
    summary = _run(conn, settings, storage, MockGridIndia([FileSpec(D, workbook_bytes(D, intl_rows=rows))]))
    intl = summary.sections["intl_lines"]
    assert intl.status is RunStatus.PARTIAL and (intl.result.records_inserted, intl.result.records_rejected) == (7, 2)
    issues = _issues(conn, intl.result.ingestion_run_id)
    assert ("UNMAPPED_COUNTRY", "ERROR") in issues and ("COUNTRY_LABEL_CONFLICT", "ERROR") in issues
    assert not any(r.line_name == "132kV NEW BORDER LINE" for r in _rows(conn, INTL))


def test_ir_reconciliation_warnings(conn: Connection, settings: Settings, storage: RawStorageManager) -> None:
    blocks = [IR_BLOCKS[0], IR_BLOCKS[1],
              ("WR", "NR", [("765 kV", "TEST GWALIOR-AGRA", "2", 1200.031, 0.0, 25.537, 0.0, 20.0)], ())]  # NET off
    workbook = workbook_bytes(D, ir_blocks=blocks, ir_subtotal_override={"ER-NR": (1.0, 2.0, -1.0)},
                              section_d_override={"Nepal": 99.0})
    summary = _run(conn, settings, storage, MockGridIndia([FileSpec(D, workbook)]))
    ir, intl = summary.sections["ir_lines"], summary.sections["intl_lines"]
    assert ir.status is RunStatus.SUCCESS and intl.status is RunStatus.SUCCESS
    ir_issues = _issues(conn, ir.result.ingestion_run_id)
    assert ("NET_MISMATCH", "WARNING") in ir_issues and ("TOTAL_MISMATCH", "WARNING") in ir_issues
    assert ("SECTION_D_MISMATCH", "WARNING") in _issues(conn, intl.result.ingestion_run_id)
    gwalior = next(r for r in _rows(conn, IR) if r.line_name == "TEST GWALIOR-AGRA")
    assert gwalior.net_exchange_mu == Decimal("20.0000")  # published value kept


def test_runs_require_country_reference_outside_dry_runs(conn: Connection, settings: Settings,
                                                         storage: RawStorageManager,
                                                         monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("src.pipelines.grid_india.runner.missing_countries", lambda *a, **k: ["BTN"])
    with pytest.raises(ConfigurationError, match="Country reference rows missing"):
        _run(conn, settings, storage, MockGridIndia([FileSpec(D, workbook_bytes(D))]))
    # Sections that do not need countries still run.
    assert _run(conn, settings, storage, MockGridIndia([FileSpec(D, workbook_bytes(D))]), sections=["ir_lines"]).ok


def test_ir_lines_rerun_revision_and_reprocess(conn: Connection, settings: Settings,
                                               storage: RawStorageManager) -> None:
    revised_blocks = [("ER", "NR", [("765 kV", "TEST GAYA-VARANASI", "2", 357.013, 1339.017, 0.013, 19.217),
                                    IR_BLOCKS[0][2][1]], ()), IR_BLOCKS[1], IR_BLOCKS[2]]
    revised_intl = list(INTL_ROWS)
    revised_intl[5] = ("", "ER", "400kV DHALKEBAR-MUZAFFARPUR 1&2", 606.013, 476.017, 532.917, 13.793)
    original = FileSpec(D, workbook_bytes(D), datetime(2026, 11, 11, 10, 47), suffix="269")
    revised = FileSpec(D, workbook_bytes(D, ir_blocks=revised_blocks, intl_rows=revised_intl,
                                         section_d_override={"Nepal": 15.481}),
                       datetime(2026, 11, 11, 16, 38), suffix="269_611")
    mock = MockGridIndia([original, revised])

    first = _run(conn, settings, storage, mock)
    ir, intl = first.sections["ir_lines"].result, first.sections["intl_lines"].result
    assert (ir.records_inserted, ir.records_unchanged) == (7, 5)
    assert (intl.records_inserted, intl.records_unchanged) == (10, 8)
    gaya = [r for r in _rows(conn, IR) if r.line_name == "TEST GAYA-VARANASI"]
    assert [(r.export_mu, r.source_available_at.astimezone(IST).time()) for r in gaya] == [
        (Decimal("17.2170"), time(10, 47)), (Decimal("19.2170"), time(16, 38))]

    rerun = _run(conn, settings, storage, mock)
    assert all(rerun.sections[s].result.records_inserted == 0 for s in ("ir_lines", "intl_lines"))
    reprocessed = _run(conn, settings, storage, mock, reprocess=True)
    assert (reprocessed.sections["ir_lines"].result.records_inserted,
            reprocessed.sections["intl_lines"].result.records_inserted) == (0, 0)
    assert (len(_rows(conn, IR)), len(_rows(conn, INTL))) == (7, 10)


def test_reference_cli_seeds_countries_idempotently(settings: Settings, conn: Connection) -> None:
    from src.pipelines.grid_india.reference import missing_countries, seed_countries as seed
    assert missing_countries(conn, "pi") == []          # seeded by the fixture (rolled back)
    again = seed(conn, "pi")
    assert again.inserted == [] and sorted(again.unchanged) == ["BGD", "BTN", "MMR", "NPL"]


# --------------------------------------------------------------- CrossBorder

CB = "grid_crossborder_market_schedule"


def _cb_row(rows: list[Any], direction: str, country: str, contract: str, exchange: str | None = None) -> Any:
    return next(r for r in rows if (r.direction, r.country_code, r.contract_type, r.power_exchange)
                == (direction, country, contract, exchange))


def test_crossborder_end_to_end(conn: Connection, settings: Settings, storage: RawStorageManager) -> None:
    mock = MockGridIndia([FileSpec(D, workbook_bytes(D))])

    summary = _run(conn, settings, storage, mock, sections=["crossborder"])

    section = summary.sections["crossborder"]
    assert summary.ok and section.status is RunStatus.SUCCESS
    assert (section.result.records_read, section.result.records_inserted, section.result.records_rejected) == (96, 96, 0)
    rows = _rows(conn, CB)
    assert len(rows) == 96 and {r.direction for r in rows} == {"EXPORT", "IMPORT", "NET"}
    assert {r.country_code for r in rows} == {"BTN", "NPL", "BGD", "MMR"}
    gna = _cb_row(rows, "EXPORT", "BGD", "GNA")
    assert (gna.scheduled_energy_mu, gna.power_exchange, gna.sign_convention, gna.interconnector_id) == (
        Decimal("24.4100"), None, "Export From India (in MU)", None)
    net = _cb_row(rows, "NET", "BGD", "GNA")
    assert (net.scheduled_energy_mu, net.sign_convention) == (
        Decimal("-24.4060"), "Net from India(in MU) -ve : Export / +ve : Import")
    assert _cb_row(rows, "IMPORT", "NPL", "COLLECTIVE_RTM", "IEX").scheduled_energy_mu == Decimal("10.5300")
    assert all(r.scheduled_energy_mu > 0 for r in rows if r.direction != "NET")
    checksum = sha256_bytes(_workbook_files(settings)[0].read_bytes())
    doc = _doc(conn, gna.document_id)
    assert (doc.dataset_code, doc.checksum_sha256, doc.ingestion_run_id) == (
        "GRID_INDIA_PSP_CROSSBORDER", checksum, section.result.ingestion_run_id)
    assert {r.ingestion_run_id for r in rows} == {section.result.ingestion_run_id}
    assert {(r.availability_basis, r.source_published_at) for r in rows} == {
        ("PUBLISHED", datetime(2026, 11, 11, 10, 40, tzinfo=IST))}
    assert _issues(conn, section.result.ingestion_run_id) == []


def test_crossborder_pre_gna_layout_loads_its_own_contract_codes(conn: Connection, settings: Settings,
                                                                 storage: RawStorageManager) -> None:
    day = date(2023, 8, 1)
    mock = MockGridIndia([FileSpec(day, workbook_bytes(day, cb={"layout": "PRE_GNA"}), datetime(2025, 4, 15, 9, 9))])
    summary = _run(conn, settings, storage, mock, start=day, sections=["crossborder"])
    assert summary.ok
    rows = _rows(conn, CB)
    assert {r.contract_type for r in rows} == {"ISGS_LTA_MTOA_PPA", "STOA_BILATERAL", "COLLECTIVE_IDAM", "COLLECTIVE_RTM"}
    assert {(r.availability_basis, r.source_published_at) for r in rows} == {("ESTIMATED_D_PLUS_2_0900", None)}


def test_crossborder_availability_around_cutoff(conn: Connection, settings: Settings,
                                                storage: RawStorageManager) -> None:
    before, after = date(2025, 5, 11), date(2025, 5, 12)
    mock = MockGridIndia([FileSpec(before, workbook_bytes(before), datetime(2025, 4, 15, 9, 10)),
                          FileSpec(after, workbook_bytes(after), datetime(2025, 5, 13, 17, 12))])
    assert _run(conn, settings, storage, mock, start=before, end=after, sections=["crossborder"]).ok
    assert {(r.report_date, r.availability_basis, r.source_published_at, r.source_available_at)
            for r in _rows(conn, CB)} == {
        (before, "ESTIMATED_D_PLUS_2_0900", None, datetime(2025, 5, 13, 9, 0, tzinfo=IST)),
        (after, "PUBLISHED", datetime(2025, 5, 13, 17, 12, tzinfo=IST), datetime(2025, 5, 13, 17, 12, tzinfo=IST))}


def test_crossborder_not_yet_published_is_info_and_missing_sheet_fails(conn: Connection, settings: Settings,
                                                                     storage: RawStorageManager) -> None:
    early = date(2023, 7, 5)
    summary = _run(conn, settings, storage, MockGridIndia([FileSpec(early, workbook_bytes(early))]), start=early,
                   sections=["crossborder"])
    section = summary.sections["crossborder"]
    assert section.status is RunStatus.SUCCESS and section.result.records_inserted == 0
    assert _issues(conn, section.result.ingestion_run_id) == [("SECTION_NOT_PUBLISHED", "INFO")]

    summary = _run(conn, settings, storage, MockGridIndia([FileSpec(D, workbook_bytes(D, omit_section="CB"))]))
    assert summary.sections["crossborder"].status is RunStatus.FAILED
    assert "CrossBorder" in summary.sections["crossborder"].error
    assert all(summary.sections[s].status is RunStatus.SUCCESS for s in summary.sections if s != "crossborder")
    assert _rows(conn, CB) == []


def test_crossborder_unknown_country_rejected_with_warnings(conn: Connection, settings: Settings,
                                                            storage: RawStorageManager) -> None:
    workbook = workbook_bytes(D, cb={"labels": ("Bhutan", "Nepal", "Bangladesh", "Sri Lanka")})
    summary = _run(conn, settings, storage, MockGridIndia([FileSpec(D, workbook)]), sections=["crossborder"])
    section = summary.sections["crossborder"]
    assert section.status is RunStatus.PARTIAL
    assert (section.result.records_inserted, section.result.records_rejected) == (72, 24)
    issues = _issues(conn, section.result.ingestion_run_id)
    assert issues.count(("UNMAPPED_COUNTRY", "ERROR")) == 24 and issues.count(("MISSING_COUNTRY", "WARNING")) == 3
    assert "MMR" not in {r.country_code for r in _rows(conn, CB)}


def test_crossborder_reconciliation_warnings_keep_published_values(conn: Connection, settings: Settings,
                                                                   storage: RawStorageManager) -> None:
    workbook = workbook_bytes(D, cb={"net_override": {("Nepal", 7): -9.003, ("Bhutan", 1): 0.833},
                                     "total_override": {("EXPORT", "Bhutan"): 9.99}})
    summary = _run(conn, settings, storage, MockGridIndia([FileSpec(D, workbook)]), sections=["crossborder"])
    section = summary.sections["crossborder"]
    assert section.status is RunStatus.SUCCESS and section.result.records_inserted == 96
    issues = _issues(conn, section.result.ingestion_run_id)
    assert set(issues) == {("NET_MISMATCH", "WARNING"), ("TOTAL_MISMATCH", "WARNING")}
    assert _cb_row(_rows(conn, CB), "NET", "NPL", "COLLECTIVE_RTM", "IEX").scheduled_energy_mu == Decimal("-9.0030")
    assert _cb_row(_rows(conn, CB), "NET", "BTN", "GNA").scheduled_energy_mu == Decimal("0.8330")  # 0.024: no issue
    descriptions = [r[0] for r in conn.execute(text(
        "SELECT issue_description FROM pi.data_quality_issue WHERE ingestion_run_id = :r AND issue_code = 'NET_MISMATCH'"),
        {"r": section.result.ingestion_run_id})]
    assert not any("BHUTAN GNA" in d for d in descriptions)


def test_crossborder_rerun_revision_and_reprocess(conn: Connection, settings: Settings,
                                                  storage: RawStorageManager) -> None:
    from tests.support.grid_india import CB_ROWS
    revised_rows = {block: {country: list(values) for country, values in rows.items()} for block, rows in CB_ROWS.items()}
    revised_rows["EXPORT"]["Bangladesh"][0] = 25.13   # GNA export revised -> Export and Net rows change
    original = FileSpec(D, workbook_bytes(D), datetime(2026, 11, 11, 10, 47), suffix="269")
    revised = FileSpec(D, workbook_bytes(D, cb={"rows": revised_rows}), datetime(2026, 11, 11, 16, 38), suffix="269_611")
    mock = MockGridIndia([original, revised])

    first = _run(conn, settings, storage, mock, sections=["crossborder"]).sections["crossborder"].result
    assert (first.records_inserted, first.records_unchanged) == (98, 94)
    versions = [(r.direction, r.scheduled_energy_mu, r.source_available_at.astimezone(IST).time())
                for r in _rows(conn, CB) if (r.country_code, r.contract_type) == ("BGD", "GNA")
                and r.direction != "IMPORT"]
    assert versions == [("EXPORT", Decimal("24.4100"), time(10, 47)), ("NET", Decimal("-24.4060"), time(10, 47)),
                        ("EXPORT", Decimal("25.1300"), time(16, 38)), ("NET", Decimal("-25.1260"), time(16, 38))]
    assert _run(conn, settings, storage, mock, sections=["crossborder"]).sections["crossborder"].result.records_inserted == 0
    reprocessed = _run(conn, settings, storage, mock, sections=["crossborder"], reprocess=True)
    assert reprocessed.sections["crossborder"].result.records_inserted == 0
    assert len(_rows(conn, CB)) == 98


def test_crossborder_requires_country_reference_outside_dry_runs(conn: Connection, settings: Settings,
                                                                 storage: RawStorageManager,
                                                                 monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("src.pipelines.grid_india.runner.missing_countries", lambda *a, **k: ["MMR"])
    with pytest.raises(ConfigurationError, match="Country reference rows missing"):
        _run(conn, settings, storage, MockGridIndia([FileSpec(D, workbook_bytes(D))]), sections=["crossborder"])


# ---------------------------------------------------------------- TimeSeries

TS = "grid_15min_scada"


def _ts_run(conn: Connection, settings: Settings, storage: RawStorageManager, day: date, workbook: bytes,
            created: datetime | None = None, **kwargs: Any) -> Any:
    summary = _run(conn, settings, storage, MockGridIndia([FileSpec(day, workbook, created)]), start=day,
                   sections=["timeseries"], **kwargs)
    return summary.sections["timeseries"]


def test_timeseries_end_to_end(conn: Connection, settings: Settings, storage: RawStorageManager) -> None:
    section = _ts_run(conn, settings, storage, D, workbook_bytes(D))
    assert section.status is RunStatus.SUCCESS and section.result.records_inserted == 96
    rows = _rows(conn, TS, order="interval_start")
    assert len(rows) == 96 and {r.region_code for r in rows} == {None}
    assert rows[0].interval_start == datetime(2026, 11, 10, 0, 0, tzinfo=IST)
    assert rows[-1].interval_start == datetime(2026, 11, 10, 23, 45, tzinfo=IST)
    noon = next(r for r in rows if r.interval_start == datetime(2026, 11, 10, 12, 30, tzinfo=IST))
    assert (noon.solar_generation_mw, noon.storage_demand_mw, noon.net_transnational_exchange_mw) == (
        Decimal("40000.000"), Decimal("0.000"), Decimal("-50.000"))
    doc = _doc(conn, noon.document_id)
    assert (doc.dataset_code, doc.checksum_sha256, doc.ingestion_run_id) == (
        "GRID_INDIA_SCADA", sha256_bytes(_workbook_files(settings)[0].read_bytes()), section.result.ingestion_run_id)
    assert {(r.availability_basis, r.source_published_at) for r in rows} == {
        ("PUBLISHED", datetime(2026, 11, 11, 10, 40, tzinfo=IST))}
    assert _issues(conn, section.result.ingestion_run_id) == []


def test_timeseries_older_layouts_leave_unpublished_storage_null(conn: Connection, settings: Settings,
                                                                 storage: RawStorageManager) -> None:
    old, mid = date(2025, 1, 10), date(2026, 5, 10)
    assert _ts_run(conn, settings, storage, old, workbook_bytes(old), datetime(2025, 4, 15, 9, 9)).status \
        is RunStatus.SUCCESS
    assert _ts_run(conn, settings, storage, mid, workbook_bytes(mid), datetime(2026, 5, 11, 10, 5)).status \
        is RunStatus.SUCCESS
    rows = _rows(conn, TS, order="interval_start")
    by_day = {d: [r for r in rows if r.interval_start.astimezone(IST).date() == d] for d in (old, mid)}
    assert all(r.storage_generation_mw is None and r.storage_demand_mw is None for r in by_day[old])
    assert all(r.storage_generation_mw is not None and r.storage_demand_mw is None for r in by_day[mid])
    assert {(r.availability_basis, r.source_published_at) for r in by_day[old]} == {("ESTIMATED_D_PLUS_2_0900", None)}
    assert {r.source_available_at for r in by_day[old]} == {datetime(2025, 1, 12, 9, 0, tzinfo=IST)}


def test_timeseries_absent_empty_and_missing_sheet(conn: Connection, settings: Settings,
                                                   storage: RawStorageManager) -> None:
    early = date(2024, 11, 3)
    section = _ts_run(conn, settings, storage, early, workbook_bytes(early))
    assert section.status is RunStatus.SUCCESS and section.result.records_inserted == 0
    assert _issues(conn, section.result.ingestion_run_id) == [("SECTION_NOT_PUBLISHED", "INFO")]

    empty = date(2025, 7, 15)
    section = _ts_run(conn, settings, storage, empty, workbook_bytes(empty, ts={"empty": True}))
    assert section.status is RunStatus.SUCCESS and section.result.records_inserted == 0
    assert _issues(conn, section.result.ingestion_run_id) == [("NO_INTERVAL_DATA", "WARNING")]

    summary = _run(conn, settings, storage, MockGridIndia([FileSpec(D, workbook_bytes(D, omit_section="TS"))]))
    assert summary.sections["timeseries"].status is RunStatus.FAILED
    assert all(summary.sections[s].status is RunStatus.SUCCESS for s in summary.sections if s != "timeseries")
    assert _rows(conn, TS) == []


def test_timeseries_warnings_keep_published_values(conn: Connection, settings: Settings,
                                                   storage: RawStorageManager) -> None:
    overrides = {(40, "TOTAL GENERATION"): 1.0, (41, "NET DEMAND MET"): 5.0, (42, "FREQUENCY"): 0.0,
                 (44, "DEMAND MET"): 300000.0}
    section = _ts_run(conn, settings, storage, D, workbook_bytes(D, ts={"value_override": overrides,
                                                                         "drop_marks": (59,)}))
    assert section.status is RunStatus.SUCCESS and section.result.records_inserted == 95
    issues = _issues(conn, section.result.ingestion_run_id)
    for expected in [("MISSING_INTERVAL", "WARNING"), ("TOTAL_MISMATCH", "WARNING"), ("NET_DEMAND_MISMATCH", "WARNING"),
                     ("TELEMETRY_SUSPECT", "WARNING"), ("MAX_DEMAND_ABOVE_SECTION_A", "WARNING")]:
        assert expected in issues
    rows = {r.interval_start.astimezone(IST).strftime("%H:%M"): r for r in _rows(conn, TS)}
    assert "14:45" not in rows                                            # never filled
    assert rows["10:30"].grid_frequency_hz == Decimal("0.0000")           # stored as published (T3)
    assert rows["11:00"].demand_met_mw == Decimal("300000.000")


def test_timeseries_total_excluding_storage_is_info(conn: Connection, settings: Settings,
                                                    storage: RawStorageManager) -> None:
    day = date(2026, 5, 10)
    section = _ts_run(conn, settings, storage, day, workbook_bytes(day, ts={"total_excludes_storage": True}),
                      datetime(2026, 5, 11, 10, 5))
    assert section.status is RunStatus.SUCCESS and section.result.records_inserted == 96
    assert _issues(conn, section.result.ingestion_run_id) == [("TOTAL_EXCLUDES_STORAGE", "INFO")]


def test_timeseries_duplicate_mark_rejected(conn: Connection, settings: Settings, storage: RawStorageManager) -> None:
    section = _ts_run(conn, settings, storage, D, workbook_bytes(D, ts={"duplicate_mark": 10}))
    assert section.status is RunStatus.PARTIAL
    assert (section.result.records_inserted, section.result.records_rejected) == (95, 2)
    assert _issues(conn, section.result.ingestion_run_id).count(("DUPLICATE_INTERVAL", "ERROR")) == 2


def test_timeseries_availability_around_cutoff(conn: Connection, settings: Settings,
                                               storage: RawStorageManager) -> None:
    before, after = date(2025, 5, 11), date(2025, 5, 12)
    mock = MockGridIndia([FileSpec(before, workbook_bytes(before), datetime(2025, 4, 15, 9, 10)),
                          FileSpec(after, workbook_bytes(after), datetime(2025, 5, 13, 17, 12))])
    assert _run(conn, settings, storage, mock, start=before, end=after, sections=["timeseries"]).ok
    assert {(r.interval_start.astimezone(IST).date(), r.availability_basis, r.source_published_at, r.source_available_at)
            for r in _rows(conn, TS)} == {
        (before, "ESTIMATED_D_PLUS_2_0900", None, datetime(2025, 5, 13, 9, 0, tzinfo=IST)),
        (after, "PUBLISHED", datetime(2025, 5, 13, 17, 12, tzinfo=IST), datetime(2025, 5, 13, 17, 12, tzinfo=IST))}


def test_timeseries_rerun_revision_and_reprocess(conn: Connection, settings: Settings,
                                                 storage: RawStorageManager) -> None:
    original = FileSpec(D, workbook_bytes(D), datetime(2026, 11, 11, 10, 47), suffix="269")
    revised = FileSpec(D, workbook_bytes(D, ts={"value_override": {(72, "WIND"): 9999.0}}),
                       datetime(2026, 11, 11, 16, 38), suffix="269_611")
    mock = MockGridIndia([original, revised])
    first = _run(conn, settings, storage, mock, sections=["timeseries"]).sections["timeseries"].result
    assert (first.records_inserted, first.records_unchanged) == (97, 95)
    at_18 = [r for r in _rows(conn, TS) if r.interval_start == datetime(2026, 11, 10, 18, 0, tzinfo=IST)]
    assert [(r.wind_generation_mw, r.source_available_at.astimezone(IST).time()) for r in at_18] == [
        (Decimal("5720.000"), time(10, 47)), (Decimal("9999.000"), time(16, 38))]
    assert _run(conn, settings, storage, mock, sections=["timeseries"]).sections["timeseries"].result.records_inserted == 0
    again = _run(conn, settings, storage, mock, sections=["timeseries"], reprocess=True).sections["timeseries"].result
    assert again.records_inserted == 0 and len(_rows(conn, TS)) == 97


# ------------------------------------------------------------------- ACE (migration 0010_grid_state_ace)

def test_ace_layout_is_stored_in_the_ace_columns_and_od_ud_layout_leaves_them_null(
        conn: Connection, settings: Settings, storage: RawStorageManager) -> None:
    ace_day = date(2026, 11, 11)
    mock = MockGridIndia([FileSpec(D, workbook_bytes(D)), FileSpec(ace_day, workbook_bytes(ace_day, peak_layout="ace"))])

    summary = _run(conn, settings, storage, mock, start=D, end=ace_day, sections=["states"])

    assert summary.ok
    rows = {(r.report_date, r.area_code): r for r in _rows(conn, "grid_state_power_supply")}
    od_ud, ace = rows[(D, "DH")], rows[(ace_day, "DH")]
    assert (od_ud.max_od_mw, od_ud.max_ud_mw, od_ud.max_ace_pos_mw, od_ud.max_ace_neg_mw) == (
        Decimal("65.000"), Decimal("-108.000"), None, None)                    # before 18-Nov: OD/UD, ACE NULL
    assert (ace.max_od_mw, ace.max_ud_mw, ace.max_ace_pos_mw, ace.max_ace_neg_mw) == (
        None, None, Decimal("65.000"), Decimal("-108.000"))                    # ACE layout: signs as published
    assert all(r.max_ace_pos_mw is not None for (day, _), r in rows.items() if day == ace_day)
    run_ids = {s.result.ingestion_run_id for s in summary.sections.values()}
    codes = {c for run in run_ids for c, _sev in _issues(conn, run)}
    assert "ACE_NOT_STORED" not in codes and "ACE_LAYOUT" in codes
    again = _run(conn, settings, storage, mock, start=D, end=ace_day, sections=["states"])
    assert again.sections["states"].result.records_inserted == 0               # re-run: nothing duplicated
