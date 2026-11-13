"""End-to-end CEA tests against the real ``pi`` schema with mocked HTTP.

Every test runs inside one outer transaction that is rolled back (nothing
persists, including the synthetic reference rows seeded by the fixture).
Assertions only see rows created inside that transaction. No live requests.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.engine import Connection

from config.settings import Settings
from scripts.ingest_cea import main as cli_main
from src.common.checksums import sha256_bytes
from src.common.database import DatabaseManager, SavepointDatabase
from src.common.exceptions import ConfigurationError
from src.common.http_client import HttpClient
from src.common.raw_storage import LocalFileSystemBackend, RawStorageManager
from src.ingestion.models import RunStatus
from src.pipelines.cea import api
from src.pipelines.cea.cells import read_grid
from src.pipelines.cea.parse_dgr import read_dgr2
from src.pipelines.cea.reference import EntityMap, missing_geography, seed_reference
from src.pipelines.cea.runner import RunSummary, run_cea_daily, run_cea_psp, run_reference
from tests.support.cea import (
    DAY,
    ENERGY_UUID,
    MONTH,
    NPP,
    PEAK_UUID,
    SITE,
    TODAY,
    CoalSpec,
    MockCea,
    OutageSpec,
    coal_rows,
    daily_url,
    default_coal,
    default_outages,
    dgr2_rows,
    dgr10_rows,
    listing_html,
    listing_url,
    pdf_bytes,
    pdf_url,
    psp_lines,
    xls_bytes,
    xls_response,
)

pytestmark = pytest.mark.integration

TABLES = ("cea_coal_stock_daily", "cea_dgr_unit_outage", "cea_dgr_plant_daily", "cea_power_supply_position")
ENERGY_TITLE = "Power Supply Position - Energy"


@pytest.fixture(scope="module")
def manager() -> Iterator[DatabaseManager]:
    try:
        db = DatabaseManager.from_settings(Settings())
    except ConfigurationError as exc:
        pytest.skip(f"PostgreSQL not configured: {exc}")
    yield db
    db.dispose()


def _baselines(connection: Connection) -> None:
    for table in TABLES:
        connection.info[f"{table}_baseline"] = connection.execute(
            text(f"SELECT coalesce(max(id), 0) FROM pi.{table}")).scalar_one()


@pytest.fixture
def bare_conn(manager: DatabaseManager) -> Iterator[Connection]:
    """A rolled-back transaction without seeded reference rows."""
    with manager.engine.connect() as connection:
        outer = connection.begin()
        _baselines(connection)
        try:
            yield connection
        finally:
            outer.rollback()


@pytest.fixture
def conn(bare_conn: Connection) -> Connection:
    seed_reference(bare_conn, "pi", read_dgr2(read_grid(xls_bytes(dgr2_rows())), expected_date=DAY))
    return bare_conn


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(raw_data_dir=tmp_path / "raw", log_to_file=False, cea_npp_reports_base_url=NPP,
                    cea_site_base_url=SITE)


@pytest.fixture
def storage(tmp_path: Path) -> RawStorageManager:
    return RawStorageManager(LocalFileSystemBackend(tmp_path / "raw"))


def _client(settings: Settings, mock: MockCea) -> HttpClient:
    return HttpClient(settings=settings, transport=httpx.MockTransport(mock), sleep=lambda _: None)


def _daily(conn: Connection, settings: Settings, storage: RawStorageManager, mock: MockCea, kind: str = "coal",
           start: date = DAY, end: date | None = None, today: date = TODAY) -> RunSummary:
    return run_cea_daily(kind, start_date=start, end_date=end, settings=settings,  # type: ignore[arg-type]
                         http_client=_client(settings, mock), database=SavepointDatabase(conn), raw_storage=storage,
                         today=today, sleep=lambda _: None)


def _psp(conn: Connection, settings: Settings, storage: RawStorageManager, mock: MockCea, kind: str = "psp-energy",
         start: date = MONTH, end: date | None = None) -> RunSummary:
    return run_cea_psp(kind, start_month=start, end_month=end, settings=settings,  # type: ignore[arg-type]
                       http_client=_client(settings, mock), database=SavepointDatabase(conn), raw_storage=storage,
                       today=TODAY, sleep=lambda _: None)


def _rows(conn: Connection, table: str, order: str = "id") -> list[Any]:
    return conn.execute(text(f"SELECT * FROM pi.{table} WHERE id > :b ORDER BY {order}"),
                        {"b": conn.info[f"{table}_baseline"]}).all()


def _plant_id(conn: Connection, name: str) -> int:
    return conn.execute(text("SELECT plant_id FROM pi.plant WHERE plant_code = :c"), {"c": f"CEA:{name}"}).scalar_one()


def _issues(conn: Connection, run_id: int) -> list[tuple[str, str]]:
    return [tuple(r) for r in conn.execute(text(
        "SELECT issue_code, severity FROM pi.data_quality_issue WHERE ingestion_run_id = :r ORDER BY 1, 2"),
        {"r": run_id})]


def _energy_mock(pdf: bytes | None = None, entries: list[tuple[str, str]] | None = None) -> MockCea:
    entries = [("2026-08", ENERGY_UUID)] if entries is None else entries
    return MockCea({
        listing_url(10010): (200, listing_html(ENERGY_TITLE, entries), {"content-type": "text/html"}),
        pdf_url(ENERGY_UUID): (200, pdf if pdf is not None else pdf_bytes(psp_lines()),
                               {"content-type": "application/pdf"}),
    })


# --------------------------------------------------------------- reference


def test_reference_seeds_geography_plants_units_and_mappings(bare_conn: Connection, settings: Settings,
                                                             storage: RawStorageManager) -> None:
    mock = MockCea({daily_url("dgr2"): xls_response(dgr2_rows())})
    result, report_date = run_reference(report_date=DAY, settings=settings, http_client=_client(settings, mock),
                                        database=SavepointDatabase(bare_conn), raw_storage=storage)

    assert report_date == DAY
    assert result.inserted["plant"] == 3 and result.inserted["generating_unit"] == 4
    assert result.inserted["source_entity_mapping"] == 6  # 3 plants x 2 plant-keyed datasets
    assert result.inserted.get("region", 0) + result.unchanged.get("region", 0) == 5
    assert result.inserted.get("state", 0) + result.unchanged.get("state", 0) == 36
    assert result.skipped == ["TEST DRUK HPS (Bhutan import)"]
    assert missing_geography(bare_conn, "pi") == []
    plant = bare_conn.execute(text("SELECT plant_name, state_code, region_code, plant_type, fuel_type, "
                                   "installed_capacity_mw FROM pi.plant WHERE plant_code = 'CEA:TEST GAMMA TPS'")).one()
    assert tuple(plant) == ("TEST GAMMA TPS", "CG", "WR", "THERMAL", None, None)
    unit = bare_conn.execute(text("SELECT unit_name, unit_capacity_mw FROM pi.generating_unit u JOIN pi.plant p "
                                  "USING (plant_id) WHERE p.plant_code = 'CEA:TEST ALPHA TPS' AND unit_code = '2'")).one()
    assert tuple(unit) == ("TEST ALPHA TPS Unit 2", Decimal("250.000"))
    assert any((Path(settings.raw_data_dir) / "cea" / "cea_reference").rglob("*.xls"))  # source file kept

    again, _ = run_reference(report_date=DAY, settings=settings, http_client=_client(settings, mock),
                             database=SavepointDatabase(bare_conn), raw_storage=storage)
    assert again.inserted == {} and again.differing == []
    assert again.unchanged["plant"] == 3 and again.unchanged["source_entity_mapping"] == 6


def test_reference_reports_differing_rows_without_changing_them(conn: Connection, settings: Settings,
                                                                storage: RawStorageManager) -> None:
    conn.execute(text("UPDATE pi.plant SET plant_type = 'EDITED' WHERE plant_code = 'CEA:TEST BETA HPS'"))
    mock = MockCea({daily_url("dgr2"): xls_response(dgr2_rows())})
    result, _ = run_reference(report_date=DAY, settings=settings, http_client=_client(settings, mock),
                              database=SavepointDatabase(conn), raw_storage=storage)
    assert result.differing == ["plant:CEA:TEST BETA HPS"]
    assert conn.execute(text("SELECT plant_type FROM pi.plant WHERE plant_code = 'CEA:TEST BETA HPS'")).scalar() == "EDITED"


def test_reference_for_unpublished_date_fails(bare_conn: Connection, settings: Settings,
                                              storage: RawStorageManager) -> None:
    with pytest.raises(ConfigurationError, match="not published"):
        run_reference(report_date=DAY, settings=settings, http_client=_client(settings, MockCea()),
                      database=SavepointDatabase(bare_conn), raw_storage=storage)


# ---------------------------------------------------------------- coal


def test_coal_end_to_end(conn: Connection, settings: Settings, storage: RawStorageManager) -> None:
    mock = MockCea({daily_url("dailyCoal1"): xls_response(coal_rows())})

    summary = _daily(conn, settings, storage, mock)

    r = summary.result
    assert summary.ok and (r.documents_processed, r.records_read, r.records_inserted, r.records_rejected) == (1, 2, 2, 0)
    assert str(mock.requests[0].url) == f"{NPP}/daily/fuel/11-11-2026/dailyCoal1-2026-11-11.xls"
    rows = _rows(conn, "cea_coal_stock_daily", "plant_id")
    alpha = next(row for row in rows if row.plant_id == _plant_id(conn, "TEST ALPHA TPS"))
    assert (alpha.report_date, alpha.actual_stock_000_tonnes, alpha.daily_requirement_000_tonnes,
            alpha.stock_vs_normative_pct, alpha.critical_stock_flag) == (
        DAY, Decimal("88.700"), Decimal("10.284"), Decimal("39.00"), False)
    assert alpha.stock_days is None and alpha.actual_stock_tonnes is None
    assert alpha.ingestion_run_id == r.ingestion_run_id and alpha.document_id is not None
    document = conn.execute(text("SELECT document_name, checksum_sha256, published_at, source_report_date "
                                 "FROM pi.source_document WHERE document_id = :d"), {"d": alpha.document_id}).one()
    assert document.document_name == "cea_coal__2026-11-11.xls"
    assert document.published_at == datetime.fromisoformat("2026-11-12T10:01:04+00:00")  # Last-Modified
    assert document.source_report_date == DAY
    stored = Path(settings.raw_data_dir) / r.raw_storage_keys[0]
    assert sha256_bytes(stored.read_bytes()) == document.checksum_sha256 and "/coal/" in r.raw_storage_keys[0].replace("\\", "/")
    assert _issues(conn, r.ingestion_run_id) == []


def test_coal_rerun_is_duplicate_and_revision_is_versioned(conn: Connection, settings: Settings,
                                                           storage: RawStorageManager) -> None:
    mock = MockCea({daily_url("dailyCoal1"): xls_response(coal_rows())})
    _daily(conn, settings, storage, mock)

    again = _daily(conn, settings, storage, mock)
    assert again.ok and again.result.duplicate_documents == 1 and again.result.records_inserted == 0
    assert _issues(conn, again.result.ingestion_run_id) == [("DUPLICATE_DOCUMENT", "INFO")]

    revised = default_coal()
    revised[0].total, revised[0].indigenous = 95.0, 95.0
    mock.routes[daily_url("dailyCoal1")] = xls_response(coal_rows(plants=revised))
    third = _daily(conn, settings, storage, mock)
    assert (third.result.records_inserted, third.result.records_unchanged) == (1, 1)
    alpha = [row for row in _rows(conn, "cea_coal_stock_daily") if row.plant_id == _plant_id(conn, "TEST ALPHA TPS")]
    assert [row.actual_stock_000_tonnes for row in alpha] == [Decimal("88.700"), Decimal("95.000")]


def test_coal_unmapped_station_is_rejected_and_recorded(conn: Connection, settings: Settings,
                                                        storage: RawStorageManager) -> None:
    unknown = CoalSpec("Punjab", "PSPCL", "TEST UNKNOWN TPS", 100.0, "50%", 22.0, 1.0, 22.0, 5.0, 0.0, 5.0, "23%",
                       True, 0.0, 1.0)
    mock = MockCea({daily_url("dailyCoal1"): xls_response(coal_rows(plants=[*default_coal(), unknown]))})

    summary = _daily(conn, settings, storage, mock)

    assert summary.status is RunStatus.PARTIAL
    assert (summary.result.records_inserted, summary.result.records_rejected) == (2, 1)
    issue = conn.execute(text("SELECT issue_code, severity, issue_description FROM pi.data_quality_issue "
                              "WHERE ingestion_run_id = :r"), {"r": summary.result.ingestion_run_id}).one()
    assert issue.issue_code == "UNMAPPED_ENTITY" and issue.severity == "ERROR"
    assert "TEST UNKNOWN TPS" in issue.issue_description
    rejects = [k for k in summary.result.raw_storage_keys if "rejected" in k]
    saved = json.loads((Path(settings.raw_data_dir) / rejects[0]).read_text(encoding="utf-8"))
    assert saved["rejected"][0]["raw"]["station"] == "TEST UNKNOWN TPS"


def test_reprocess_loads_records_rejected_earlier_and_skips_the_rest(conn: Connection, settings: Settings,
                                                                     storage: RawStorageManager) -> None:
    unknown = CoalSpec("Punjab", "PSPCL", "TEST LATE TPS", 100.0, "50%", 22.0, 1.0, 22.0, 5.0, 0.0, 5.0, "23%",
                       True, 0.0, 1.0)
    mock = MockCea({daily_url("dailyCoal1"): xls_response(coal_rows(plants=[*default_coal(), unknown]))})
    first = _daily(conn, settings, storage, mock)
    assert (first.result.records_inserted, first.result.records_rejected) == (2, 1)

    # The station is added to the reference afterwards.
    plant_id = conn.execute(text("INSERT INTO pi.plant (plant_code, plant_name) VALUES ('CEA:TEST LATE TPS', "
                                 "'TEST LATE TPS') RETURNING plant_id")).scalar_one()
    conn.execute(text("INSERT INTO pi.source_entity_mapping (dataset_id, source_entity_name, plant_id) SELECT "
                      "dataset_id, 'TEST LATE TPS', :p FROM pi.source_dataset WHERE dataset_code = "
                      "'CEA_COAL_STOCK_DAILY'"), {"p": plant_id})

    plain = _daily(conn, settings, storage, mock)  # identical file: skipped as a duplicate document
    assert plain.result.duplicate_documents == 1 and plain.result.records_inserted == 0

    reprocessed = run_cea_daily("coal", start_date=DAY, settings=settings, http_client=_client(settings, mock),
                                database=SavepointDatabase(conn), raw_storage=storage, today=TODAY,
                                sleep=lambda _: None, reprocess=True)
    r = reprocessed.result
    assert reprocessed.ok and (r.records_inserted, r.records_unchanged, r.duplicate_documents) == (1, 2, 0)
    assert ("REPROCESSED_DOCUMENT", "INFO") in _issues(conn, r.ingestion_run_id)
    assert len(_rows(conn, "cea_coal_stock_daily")) == 3


def test_coal_missing_days_and_total_mismatch(conn: Connection, settings: Settings,
                                              storage: RawStorageManager) -> None:
    # 11-10 should have been published by 11-13 (WARNING); 11-12 may simply be late (INFO).
    older, recent = date(2026, 11, 10), date(2026, 11, 12)
    mock = MockCea({daily_url("dailyCoal1"): xls_response(coal_rows(grand_total=999.0))})

    summary = _daily(conn, settings, storage, mock, start=older, end=recent)

    assert summary.missing == [older.isoformat(), recent.isoformat()]
    assert summary.result.records_inserted == 2
    assert _issues(conn, summary.result.ingestion_run_id) == [
        ("INCONSISTENT_TOTAL", "WARNING"), ("MISSING_DAY", "INFO"), ("MISSING_DAY", "WARNING")]


def test_coal_inconsistent_row_total_warns_but_loads(conn: Connection, settings: Settings,
                                                     storage: RawStorageManager) -> None:
    plants = default_coal()
    plants[0].total = 90.0
    mock = MockCea({daily_url("dailyCoal1"): xls_response(coal_rows(plants=plants, grand_total=173.9))})
    summary = _daily(conn, settings, storage, mock)
    assert summary.ok and summary.result.records_inserted == 2
    assert _issues(conn, summary.result.ingestion_run_id) == [("INCONSISTENT_TOTAL", "WARNING")]


@pytest.mark.parametrize("response, error", [
    ((200, b"<!DOCTYPE html><html>login</html>", {"content-type": "text/html"}), "HTML page"),
    ((200, b"garbage", {"content-type": "application/vnd.ms-excel"}), "readable .xls"),
    ((403, b"forbidden", {}), "HTTP 403"),
])
def test_coal_structural_failures_fail_the_run(conn: Connection, settings: Settings, storage: RawStorageManager,
                                               response: tuple[int, bytes, dict[str, str]], error: str) -> None:
    summary = _daily(conn, settings, storage, MockCea({daily_url("dailyCoal1"): response}))
    assert summary.status is RunStatus.FAILED and error in (summary.error or "")
    assert _rows(conn, "cea_coal_stock_daily") == []


def test_server_errors_are_retried(conn: Connection, settings: Settings, storage: RawStorageManager) -> None:
    calls = {"n": 0}
    good = xls_bytes(coal_rows())

    def flaky(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(503) if calls["n"] == 1 else httpx.Response(200, content=good)

    summary = _daily(conn, settings, storage, MockCea({daily_url("dailyCoal1"): flaky}))
    assert summary.ok and calls["n"] == 2 and summary.result.records_inserted == 2


# -------------------------------------------------------------- outages


def test_outages_end_to_end_and_revision(conn: Connection, settings: Settings, storage: RawStorageManager) -> None:
    mock = MockCea({daily_url("dgr10"): xls_response(dgr10_rows())})

    summary = _daily(conn, settings, storage, mock, kind="outages")

    assert summary.ok and summary.result.records_inserted == 3
    rows = _rows(conn, "cea_dgr_unit_outage", "plant_id, unit_label")
    alpha_1 = next(row for row in rows if row.plant_id == _plant_id(conn, "TEST ALPHA TPS") and row.unit_label == "1")
    assert (alpha_1.forced_minor_mw, alpha_1.planned_mw, alpha_1.outage_capacity_mw) == (
        Decimal("210.000"), Decimal("0.000"), Decimal("210.000"))
    assert alpha_1.unit_id is not None and alpha_1.state_name == "Haryana"
    assert alpha_1.outage_start_at == datetime.fromisoformat("2026-11-08T09:58:00+05:30")
    assert alpha_1.outage_reason == "RESERVE SHUT DOWN / STANDBY UNIT"
    gamma = next(row for row in rows if row.plant_id == _plant_id(conn, "TEST GAMMA TPS"))
    assert gamma.forced_major_mw == Decimal("660.000")
    assert gamma.expected_sync_at == datetime.fromisoformat("2026-11-20T00:00:00+05:30")

    # The unit came back: same unit, 0 MW and a sync time -> a new version, the old one is kept.
    changed = dgr10_rows(outages=[
        OutageSpec("Haryana", "TEST ALPHA TPS", 1.0, start="08/11/2026 09:58", sync="11/11/2026 22:28"),
        *default_outages()[1:],
    ])
    mock.routes[daily_url("dgr10")] = xls_response(changed)
    again = _daily(conn, settings, storage, mock, kind="outages")
    assert again.result.records_inserted == 1
    versions = [row for row in _rows(conn, "cea_dgr_unit_outage")
                if row.plant_id == _plant_id(conn, "TEST ALPHA TPS") and row.unit_label == "1"]
    assert [v.outage_capacity_mw for v in versions] == [Decimal("210.000"), Decimal("0.000")]


def test_outage_above_unit_capacity_and_total_mismatch_warn(conn: Connection, settings: Settings,
                                                            storage: RawStorageManager) -> None:
    rows = dgr10_rows(outages=[OutageSpec("Haryana", "TEST ALPHA TPS", 1.0, forced_major=300.0)], total=100.0)
    summary = _daily(conn, settings, storage, MockCea({daily_url("dgr10"): xls_response(rows)}), kind="outages")
    assert summary.ok and summary.result.records_inserted == 1
    assert _issues(conn, summary.result.ingestion_run_id) == [("ABOVE_UNIT_CAPACITY", "WARNING"),
                                                              ("INCONSISTENT_TOTAL", "WARNING")]


# ----------------------------------------------------------- plant daily


def test_plant_daily_end_to_end(conn: Connection, settings: Settings, storage: RawStorageManager) -> None:
    mock = MockCea({daily_url("dgr2"): xls_response(dgr2_rows())})
    summary = _daily(conn, settings, storage, mock, kind="plant-daily")
    assert summary.ok and summary.result.records_inserted == 3  # Bhutan station skipped
    alpha = next(row for row in _rows(conn, "cea_dgr_plant_daily") if row.plant_id == _plant_id(conn, "TEST ALPHA TPS"))
    assert (alpha.coal_stock_days, alpha.monitored_capacity_mw, alpha.todays_actual_mu, alpha.ytd_actual_mu,
            alpha.capacity_under_outage_mw) == (Decimal("7.00"), Decimal("460.000"), Decimal("6.2500"),
                                                Decimal("1400.2500"), Decimal("210.000"))
    again = _daily(conn, settings, storage, mock, kind="plant-daily")
    assert again.result.duplicate_documents == 1 and again.result.records_inserted == 0


# ------------------------------------------------------------------- PSP


def test_psp_energy_end_to_end_and_rerun(conn: Connection, settings: Settings, storage: RawStorageManager) -> None:
    mock = _energy_mock()

    summary = _psp(conn, settings, storage, mock)

    r = summary.result
    assert summary.ok and (r.documents_processed, r.records_inserted, r.records_rejected) == (2, 26, 0)
    rows = {(row.area_code, row.period_scope): row for row in _rows(conn, "cea_power_supply_position")}
    hr = rows[("HR", "MONTH")]
    assert (hr.report_month, hr.measure, hr.unit, hr.requirement, hr.supplied, hr.not_supplied, hr.not_supplied_pct,
            hr.state_code, hr.region_code, hr.report_status) == (
        MONTH, "ENERGY", "MU", Decimal("8611.000"), Decimal("8600.000"), Decimal("11.000"), Decimal("0.10"),
        "HR", "NR", "Revised")
    assert rows[("ALL_INDIA", "FYTD")].requirement == Decimal("811111.000")
    assert rows[("AN", "MONTH")].footnote_flags == "(#) (**)"
    names = conn.execute(text("SELECT document_name FROM pi.source_document WHERE ingestion_run_id = :r ORDER BY 1"),
                         {"r": r.ingestion_run_id}).scalars().all()
    assert names == [f"cea_psp_energy__2026-08__{ENERGY_UUID}.pdf", "cea_psp_energy__listing__2026-08__2026-08.html"]

    again = _psp(conn, settings, storage, mock)
    assert again.ok and again.result.duplicate_documents == 2 and again.result.records_inserted == 0


def test_psp_revised_report_is_versioned(conn: Connection, settings: Settings, storage: RawStorageManager) -> None:
    _psp(conn, settings, storage, _energy_mock())
    rows = [("/ Haryana", "8,700 8,690 10 0.1 37,810 37,651 159 0.4"), (" / All India", "9 9 0 0.0 9 9 0 0.0")]
    summary = _psp(conn, settings, storage, _energy_mock(pdf_bytes(psp_lines(rows=rows))))
    assert summary.result.records_inserted == 4 and summary.result.records_unchanged == 0
    haryana = [row.requirement for row in _rows(conn, "cea_power_supply_position")
               if row.area_code == "HR" and row.period_scope == "MONTH"]
    assert haryana == [Decimal("8611.000"), Decimal("8700.000")]


def test_psp_wrong_file_fails_and_missing_month_is_recorded(conn: Connection, settings: Settings,
                                                            storage: RawStorageManager) -> None:
    wrong = _psp(conn, settings, storage, _energy_mock(pdf_bytes(psp_lines("PEAK"))))
    assert wrong.status is RunStatus.FAILED and "found a peak report" in (wrong.error or "")
    assert _rows(conn, "cea_power_supply_position") == []

    missing = _psp(conn, settings, storage, _energy_mock(), start=date(2026, 7, 1), end=MONTH)
    assert missing.missing == ["2026-07"] and missing.result.records_inserted == 26
    assert ("MISSING_MONTH", "INFO") in _issues(conn, missing.result.ingestion_run_id)


def test_psp_unknown_area_rejected(conn: Connection, settings: Settings, storage: RawStorageManager) -> None:
    rows = [("/ Atlantis", "1 1 0 0.0 2 2 0 0.0"), (" / All India", "9 9 0 0.0 9 9 0 0.0")]
    summary = _psp(conn, settings, storage, _energy_mock(pdf_bytes(psp_lines(rows=rows))))
    assert summary.status is RunStatus.PARTIAL and summary.result.records_rejected == 2
    assert _issues(conn, summary.result.ingestion_run_id) == [("UNMAPPED_ENTITY", "ERROR")] * 2


def test_psp_peak_uses_its_own_listing(conn: Connection, settings: Settings, storage: RawStorageManager) -> None:
    mock = MockCea({
        listing_url(10011): (200, listing_html("Power Supply Position - Peak", [("2026-08", PEAK_UUID)]), {}),
        pdf_url(PEAK_UUID): (200, pdf_bytes(psp_lines("PEAK")), {}),
    })
    summary = _psp(conn, settings, storage, mock, kind="psp-peak")
    assert summary.ok and summary.result.records_inserted == 26
    assert {row.unit for row in _rows(conn, "cea_power_supply_position")} == {"MW"}


# ------------------------------------------------- reference checks, guards


def test_runs_require_reference_rows_outside_dry_runs(bare_conn: Connection, settings: Settings,
                                                      storage: RawStorageManager, monkeypatch: pytest.MonkeyPatch) -> None:
    # Hide any real mappings so the check sees an empty reference.
    monkeypatch.setattr("src.pipelines.cea.runner.load_entities", lambda *a, **k: EntityMap({}, {}, {}))
    with pytest.raises(ConfigurationError, match="reference"):
        _daily(bare_conn, settings, storage, MockCea({daily_url("dailyCoal1"): xls_response(coal_rows())}))


def test_validation_guards(conn: Connection, settings: Settings, storage: RawStorageManager) -> None:
    with pytest.raises(ValueError, match="cannot be published yet"):
        _daily(conn, settings, storage, MockCea(), start=TODAY)
    with pytest.raises(ValueError, match="at most 31 days"):
        _daily(conn, settings, storage, MockCea(), start=date(2026, 10, 1), end=DAY)
    with pytest.raises(ValueError, match="supported from"):
        _psp(conn, settings, storage, MockCea(), start=date(2019, 6, 1))


def test_cli_rejects_bad_arguments(settings: Settings) -> None:
    assert cli_main(["coal", "--date", "2030-01-01"], settings=settings) == 2
    assert cli_main(["psp-energy", "--month", "2019-06"], settings=settings) == 2
    with pytest.raises(SystemExit):
        cli_main(["coal", "--start-date", "2026-11-01"], settings=settings)
    assert api.MAX_DAYS_PER_COMMAND == 31
