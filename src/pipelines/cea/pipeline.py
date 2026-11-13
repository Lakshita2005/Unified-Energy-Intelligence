"""CEA ingestion pipelines on :class:`~src.ingestion.base.BaseIngestionPipeline`.

| Pipeline | Report | Table | Natural key |
|---|---|---|---|
| :class:`CoalStockPipeline` | dailyCoal1 (.xls) | cea_coal_stock_daily | plant_id, report_date |
| :class:`UnitOutagePipeline` | DGR sub-report 10 (.xls) | cea_dgr_unit_outage | plant_id, unit_label, report_date |
| :class:`PlantDailyPipeline` | DGR sub-report 2 (.xls) | cea_dgr_plant_daily | plant_id, report_date |
| :class:`PspEnergyPipeline` / :class:`PspPeakPipeline` | monthly PDF | cea_power_supply_position | area_code, report_month, period_scope, measure |

All use :attr:`ConflictPolicy.VERSION`: an unchanged record is skipped, a
revised one is inserted as a new version (earlier rows are kept). Each file
is one raw document and one transaction. Names are resolved to ids with an
:class:`~src.pipelines.cea.reference.EntityMap` loaded before the run; a name
that is not mapped is rejected as ``UNMAPPED_ENTITY`` (recorded, not dropped).

Daily reports that are not published (HTTP 404) are collected in
:attr:`missing` and recorded by the runner as ``MISSING_DAY`` issues.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any, ClassVar

from src.ingestion.base import BaseIngestionPipeline, RunContext
from src.ingestion.catalog import DATASETS, SOURCES
from src.ingestion.models import (
    DataQualityIssue,
    DatasetDefinition,
    ParsedRecord,
    ParseResult,
    RawDocument,
    Severity,
    ValidationIssue,
)
from src.ingestion.validation import CompositeValidator, FunctionValidator, NumericRange, RequiredFields, Validator
from src.loaders.table_loader import ConflictPolicy
from src.pipelines.cea import api
from src.pipelines.cea.cells import read_grid
from src.pipelines.cea.errors import InvalidResponseError
from src.pipelines.cea.parse_coal import CoalReport, parse_coal_report
from src.pipelines.cea.parse_dgr import OutageReport, parse_outage_report, plant_daily_records, read_dgr2
from src.pipelines.cea.parse_psp import ListingEntry, parse_listing, parse_psp_report
from src.pipelines.cea.reference import EntityMap

PIPELINE_VERSION = "cea-1.0.0"
COAL_TOTAL_TOLERANCE = Decimal("0.15")   # '000 t: Total vs Indigenous + Import (published to 0.1)
GRAND_TOTAL_TOLERANCE = Decimal("0.5")   # '000 t: sum of plant rows vs Grand Total
PSP_TOLERANCE = Decimal("1")             # MU/MW: figures are rounded to whole units
OUTAGE_TOTAL_TOLERANCE = Decimal("0.5")  # MW


def _catalog_dataset(code: str) -> DatasetDefinition:
    return next(entry.definition for entry in DATASETS if entry.definition.code == code)


def _unmapped(field_name: str, what: str, name_key: str) -> FunctionValidator:
    def check(record: ParsedRecord) -> list[ValidationIssue]:
        if record.values.get(field_name) is not None:
            return []
        name = (record.raw or {}).get(name_key) if isinstance(record.raw, dict) else None
        return [ValidationIssue("UNMAPPED_ENTITY", f"{what} {name!r} has no reference mapping; add it with "
                                "'python -m scripts.ingest_cea reference' (for an older report, pass --date)",
                                Severity.ERROR, field_name)]
    return FunctionValidator(check)


def _non_negative(*fields: str, severity: Severity = Severity.ERROR) -> list[NumericRange]:
    return [NumericRange(f, minimum=0, severity=severity) for f in fields]


# ------------------------------------------------------------------- daily base

class _DailyReportPipeline(BaseIngestionPipeline):
    """One .xls per report date from the NPP mirror; 404 = not published."""

    source = SOURCES["CEA"]
    conflict_policy = ConflictPolicy.VERSION
    pipeline_version = PIPELINE_VERSION
    report: ClassVar[api.DailyReport]

    def __init__(self, *, entities: EntityMap | None = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.entities = entities or EntityMap({}, {}, {})
        self.missing: list[date] = []
        self._parsed: dict[int, Any] = {}

    def raw_storage_partition(self, document: RawDocument, context: RunContext) -> str | None:
        return self.report.kind

    def fetch(self, context: RunContext) -> list[RawDocument]:
        start = date.fromisoformat(context.parameters["start_date"])
        end = date.fromisoformat(context.parameters["end_date"])
        base = self.settings.cea_npp_reports_base_url
        documents, self.missing = [], []
        day = start
        while day <= end:
            if day > start:
                self._sleep(api.REQUEST_PAUSE_SECONDS)
            url = self.report.url(base, day)
            context.log.info("Requesting %s", url)
            fetched = api.fetch_file(self.http, url)
            if fetched is None:
                context.log.warning("%s report for %s is not published (HTTP 404)", self.report.kind, day)
                self.missing.append(day)
            else:
                api.require_binary(fetched, f"{self.report.kind} {day}")
                documents.append(RawDocument(
                    content=fetched.content, retrieved_at=fetched.retrieved_at, extension="xls",
                    content_type=fetched.content_type, published_at=fetched.last_modified, source_url=fetched.url,
                    document_name=f"cea_{self.report.kind}__{day.isoformat()}.xls", source_report_date=day))
            day += timedelta(days=1)
        return documents


# ------------------------------------------------------------------- coal stock

def _coal_total_check(record: ParsedRecord) -> list[ValidationIssue]:
    v = record.values
    parts = (v.get("indigenous_stock_000_tonnes"), v.get("imported_stock_000_tonnes"), v.get("actual_stock_000_tonnes"))
    if None in parts or abs(parts[0] + parts[1] - parts[2]) <= COAL_TOTAL_TOLERANCE:
        return []
    return [ValidationIssue("INCONSISTENT_TOTAL", f"Total stock {parts[2]} differs from indigenous {parts[0]} + "
                            f"import {parts[1]} ('000 t)", Severity.WARNING, "actual_stock_000_tonnes")]


class CoalStockPipeline(_DailyReportPipeline):
    """CEA Daily Coal Stock Report -> ``cea_coal_stock_daily``."""

    dataset = _catalog_dataset("CEA_COAL_STOCK_DAILY")
    target_table = "cea_coal_stock_daily"
    natural_key = ("plant_id", "report_date")
    report = api.COAL

    def parse(self, document: RawDocument, context: RunContext) -> ParseResult:
        report = parse_coal_report(read_grid(document.content), expected_date=document.source_report_date,
                                   plants=self.entities.plants)
        self._parsed[id(document)] = report
        return ParseResult(report.records, report.failures)

    def validator(self) -> Validator:
        return CompositeValidator(
            _unmapped("plant_id", "Station", "station"),
            RequiredFields("report_date", "actual_stock_000_tonnes"),
            *_non_negative("actual_stock_000_tonnes", "indigenous_stock_000_tonnes", "imported_stock_000_tonnes",
                           "daily_requirement_000_tonnes", "receipt_000_tonnes", "consumption_000_tonnes",
                           "stock_vs_normative_pct"),
            NumericRange("normative_stock_days", minimum=0, maximum=365),
            FunctionValidator(_coal_total_check),
        )

    def document_issues(self, document: RawDocument, parsed: ParseResult, context: RunContext) -> list[DataQualityIssue]:
        report: CoalReport | None = self._parsed.pop(id(document), None)
        if report is None:
            return []
        if report.grand_total_stock is None:
            return [DataQualityIssue(self.target_table, "TOTAL_NOT_FOUND", f"Grand Total row not read for "
                                     f"{report.report_date}; plant sum not cross-checked", Severity.WARNING,
                                     str(report.report_date))]
        difference = report.plant_stock_sum - report.grand_total_stock
        if abs(difference) <= GRAND_TOTAL_TOLERANCE or report.failures:
            return []
        return [DataQualityIssue(self.target_table, "INCONSISTENT_TOTAL",
                                 f"Sum of plant stocks {report.plant_stock_sum} differs from Grand Total "
                                 f"{report.grand_total_stock} ('000 t) on {report.report_date}", Severity.WARNING,
                                 str(report.report_date))]


# -------------------------------------------------------------------- outages

class UnitOutagePipeline(_DailyReportPipeline):
    """DGR sub-report 10 (coal, lignite and nuclear units under maintenance) -> ``cea_dgr_unit_outage``."""

    dataset = _catalog_dataset("CEA_DGR_DAILY")
    target_table = "cea_dgr_unit_outage"
    natural_key = ("plant_id", "unit_label", "report_date")
    report = api.DGR10

    def parse(self, document: RawDocument, context: RunContext) -> ParseResult:
        report = parse_outage_report(read_grid(document.content), expected_date=document.source_report_date,
                                     plants=self.entities.plants, units=self.entities.units)
        self._parsed[id(document)] = report
        return ParseResult(report.records, report.failures)

    def _capacity_check(self, record: ParsedRecord) -> list[ValidationIssue]:
        unit_id, total = record.values.get("unit_id"), record.values.get("outage_capacity_mw")
        capacity = self.entities.unit_capacity.get(unit_id) if unit_id is not None else None
        if capacity is None or total is None or total <= capacity:
            return []
        return [ValidationIssue("ABOVE_UNIT_CAPACITY", f"Outage {total} MW is above the unit capacity {capacity} MW "
                                "recorded in generating_unit", Severity.WARNING, "outage_capacity_mw")]

    @staticmethod
    def _start_check(record: ParsedRecord) -> list[ValidationIssue]:
        start, day = record.values.get("outage_start_at"), record.values.get("report_date")
        if start is None or day is None or start <= datetime.combine(day + timedelta(days=1), time(), api.IST):
            return []
        return [ValidationIssue("FUTURE_OUTAGE_START", f"Outage start {start} is after the report date {day}",
                                Severity.WARNING, "outage_start_at")]

    def validator(self) -> Validator:
        return CompositeValidator(
            _unmapped("plant_id", "Power station", "station"),
            RequiredFields("report_date", "unit_label", "outage_capacity_mw"),
            *_non_negative("planned_mw", "forced_major_mw", "forced_minor_mw", "other_mw", "outage_capacity_mw"),
            FunctionValidator(self._capacity_check),
            FunctionValidator(self._start_check),
        )

    def document_issues(self, document: RawDocument, parsed: ParseResult, context: RunContext) -> list[DataQualityIssue]:
        report: OutageReport | None = self._parsed.pop(id(document), None)
        if report is None or report.published_total_mw is None or report.failures:
            return []
        total = sum((r.values["outage_capacity_mw"] for r in report.records), Decimal(0))
        if abs(total - report.published_total_mw) <= OUTAGE_TOTAL_TOLERANCE:
            return []
        return [DataQualityIssue(self.target_table, "INCONSISTENT_TOTAL",
                                 f"Sum of unit outages {total} MW differs from the report Total "
                                 f"{report.published_total_mw} MW on {report.report_date}", Severity.WARNING,
                                 str(report.report_date))]


# ---------------------------------------------------------------- plant daily

class PlantDailyPipeline(_DailyReportPipeline):
    """DGR sub-report 2 station rows -> ``cea_dgr_plant_daily`` (incl. coal stock in days)."""

    dataset = _catalog_dataset("CEA_DGR_DAILY")
    target_table = "cea_dgr_plant_daily"
    natural_key = ("plant_id", "report_date")
    report = api.DGR2

    def parse(self, document: RawDocument, context: RunContext) -> ParseResult:
        report = read_dgr2(read_grid(document.content), expected_date=document.source_report_date)
        records, failures = plant_daily_records(report, plants=self.entities.plants)
        return ParseResult(records, failures)

    def validator(self) -> Validator:
        return CompositeValidator(
            _unmapped("plant_id", "Power station", "station"),
            RequiredFields("report_date", "monitored_capacity_mw"),
            *_non_negative("monitored_capacity_mw", "capacity_under_outage_mw", "coal_stock_days"),
            *_non_negative("todays_program_mu", "todays_actual_mu", "ytd_program_mu", "ytd_actual_mu",
                           severity=Severity.WARNING),
        )


# --------------------------------------------------------- power supply position

def _psp_checks(record: ParsedRecord) -> list[ValidationIssue]:
    v = record.values
    requirement, supplied, short = v.get("requirement"), v.get("supplied"), v.get("not_supplied")
    if None in (requirement, supplied, short):
        return []
    issues = []
    if supplied > requirement:
        issues.append(ValidationIssue("SUPPLY_ABOVE_REQUIREMENT", f"Supplied {supplied} exceeds requirement "
                                      f"{requirement}", Severity.WARNING, "supplied"))
    if abs(requirement - supplied - short) > PSP_TOLERANCE:
        issues.append(ValidationIssue("INCONSISTENT_TOTAL", f"Requirement {requirement} - supplied {supplied} "
                                      f"differs from not supplied {short}", Severity.WARNING, "not_supplied"))
    return issues


class _PspPipeline(BaseIngestionPipeline):
    """Monthly report PDFs found through the cea.nic.in listing page (stored as a document for audit)."""

    source = SOURCES["CEA"]
    target_table = "cea_power_supply_position"
    natural_key = ("area_code", "report_month", "period_scope", "measure")
    conflict_policy = ConflictPolicy.VERSION
    pipeline_version = PIPELINE_VERSION
    psp: ClassVar[api.PspReport]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.missing: list[date] = []
        self.listing: list[ListingEntry] = []

    @property
    def _prefix(self) -> str:
        return f"cea_psp_{self.psp.measure.lower()}"

    def raw_storage_partition(self, document: RawDocument, context: RunContext) -> str | None:
        return "listing" if (document.document_name or "").endswith(".html") else self.psp.measure.lower()

    def fetch(self, context: RunContext) -> list[RawDocument]:
        start = date.fromisoformat(context.parameters["start_month"])
        end = date.fromisoformat(context.parameters["end_month"])
        site = self.settings.cea_site_base_url
        listing_url = self.psp.listing_url(site)
        context.log.info("Requesting listing %s", listing_url)
        listing = api.fetch_file(self.http, listing_url, not_found_ok=False)
        assert listing is not None
        self.listing = parse_listing(listing.content, listing_title=self.psp.listing_title)
        documents = [RawDocument(
            content=listing.content, retrieved_at=listing.retrieved_at, extension="html",
            content_type=listing.content_type, source_url=listing.url,
            document_name=f"{self._prefix}__listing__{start:%Y-%m}__{end:%Y-%m}.html")]
        self.missing = []
        for month in api.months(start, end):
            entries = [e for e in self.listing if e.month == month]
            if not entries:
                context.log.warning("No %s report listed for %s", self.psp.measure.lower(), f"{month:%Y-%m}")
                self.missing.append(month)
                continue
            if len(entries) > 1:
                # One file per month is expected; loading two would make two versions in one run.
                raise InvalidResponseError(f"{len(entries)} {self.psp.listing_title!r} entries listed for "
                                           f"{month:%Y-%m}: {[e.uuid for e in entries]}")
            for entry in entries:
                self._sleep(api.REQUEST_PAUSE_SECONDS)
                url = api.pdf_url(site, entry.uuid)
                context.log.info("Requesting %s (%s)", url, f"{month:%Y-%m}")
                fetched = api.fetch_file(self.http, url, not_found_ok=False)
                assert fetched is not None
                api.require_binary(fetched, f"{self.psp.listing_title} {month:%Y-%m}")
                documents.append(RawDocument(
                    content=fetched.content, retrieved_at=fetched.retrieved_at, extension="pdf",
                    content_type=fetched.content_type, published_at=fetched.last_modified, source_url=url,
                    document_name=f"{self._prefix}__{month:%Y-%m}__{entry.uuid}.pdf", source_report_date=month))
        return documents

    def parse(self, document: RawDocument, context: RunContext) -> ParseResult:
        if document.extension == "html":
            return ParseResult()  # the listing is stored for audit; it holds no records
        report = parse_psp_report(document.content, measure=self.psp.measure,
                                  expected_month=document.source_report_date)
        return ParseResult(report.records, report.failures)

    def validator(self) -> Validator:
        return CompositeValidator(
            _unmapped("area_code", "Area", "name"),
            RequiredFields("report_month", "period_scope", "measure", "requirement", "supplied"),
            *_non_negative("requirement", "supplied", "not_supplied", "not_supplied_pct"),
            NumericRange("not_supplied_pct", maximum=100),
            FunctionValidator(_psp_checks),
        )


class PspEnergyPipeline(_PspPipeline):
    dataset = _catalog_dataset("CEA_PSP_ENERGY_MONTHLY")
    psp = api.PSP_ENERGY


class PspPeakPipeline(_PspPipeline):
    dataset = _catalog_dataset("CEA_PSP_PEAK_MONTHLY")
    psp = api.PSP_PEAK
