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
from src.pipelines.grid_india.runner import PspRunSummary, run_grid_india_psp
from tests.support.grid_india import API, CDN, AreaRow, FileSpec, MockGridIndia, default_section_c, workbook_bytes

pytestmark = pytest.mark.integration

D = date(2026, 11, 10)
TODAY = date(2026, 11, 13)
TABLES = ("grid_daily_power_supply", "grid_state_power_supply", "grid_sourcewise_generation",
          "grid_generation_outage")


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
        assert _issues(conn, section.result.ingestion_run_id) == [
            ("MISSING_DAY", "INFO"), ("MISSING_DAY", "WARNING"), ("MISSING_DAY", "WARNING")]


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
