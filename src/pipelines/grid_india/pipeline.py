"""Grid-India Daily PSP section pipelines (decision G2: one workbook, several tables).

The runner downloads each workbook version **once** and stores it **once**
(write-once raw file with SHA-256). Each destination table then has its own
pipeline and its own ingestion run, which:

* receives the already-downloaded documents (no second request);
* reuses the stored raw object instead of writing another copy (``_store_raw``);
* registers the workbook as a ``source_document`` of its own dataset (documents
  are de-duplicated per dataset), parses only its section(s), validates and loads.

So a failure in one section is recorded as that section's FAILED/PARTIAL run
and never marks the other sections (or the workbook) as successful.

| Pipeline | Section(s) | Table | Natural key (VERSION) |
|---|---|---|---|
| :class:`SupplyPipeline` | A | grid_daily_power_supply | region_code (NULL = All India), report_date |
| :class:`StatesPipeline` | C + J | grid_state_power_supply | area_code, report_date |
| :class:`FuelPipeline` | G | grid_sourcewise_generation | region_code, fuel_type, report_date |
| :class:`OutagePipeline` | F | grid_generation_outage | region_code, sector, report_date |

Every record carries ``source_published_at`` / ``source_available_at`` /
``availability_basis`` (decision G3; see :func:`api.availability`).
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, ClassVar

from src.common.raw_storage import StoredObject
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
from src.pipelines.grid_india import api
from src.pipelines.grid_india.entities import EXPECTED_SECTION_C, REGION_HEADINGS
from src.pipelines.grid_india.parse_psp import (
    parse_section_a,
    parse_section_f,
    parse_section_g,
    parse_states,
    read_workbook,
)

PIPELINE_VERSION = "grid-india-1.0.0"
MW_TOLERANCE = Decimal("1")      # published regional values are whole MW
MU_TOLERANCE = Decimal("0.05")   # published to 2-3 decimals


def _catalog_dataset(code: str) -> DatasetDefinition:
    return next(entry.definition for entry in DATASETS if entry.definition.code == code)


@dataclass(frozen=True)
class PreparedWorkbook:
    """A downloaded and stored workbook version, shared by all section pipelines."""

    entry: api.ListingEntry
    document: RawDocument
    stored: StoredObject
    availability: api.Availability


def _tolerance(field: str) -> Decimal:
    return MW_TOLERANCE if field.endswith("_mw") else MU_TOLERANCE


class _PspSectionPipeline(BaseIngestionPipeline):
    source = SOURCES["GRID_INDIA"]
    conflict_policy = ConflictPolicy.VERSION
    pipeline_version = PIPELINE_VERSION
    section_label: ClassVar[str]

    def __init__(self, *, workbooks: list[PreparedWorkbook] | None = None,
                 fetch_error: BaseException | None = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.workbooks = list(workbooks or [])
        self.fetch_error = fetch_error
        self._by_document = {id(w.document): w for w in self.workbooks}
        self._parsed: dict[int, Any] = {}

    # The runner already downloaded and stored the workbooks.
    def fetch(self, context: RunContext) -> list[RawDocument]:
        if self.fetch_error is not None:
            raise self.fetch_error
        return [w.document for w in self.workbooks]

    def _store_raw(self, document: RawDocument, context: RunContext) -> StoredObject:
        """Reuse the single stored copy of the workbook (it is never written twice)."""
        stored = self._by_document[id(document)].stored
        context.log.info("Using stored workbook %s (sha256=%s)", stored.key, stored.sha256)
        return stored

    def raw_storage_partition(self, document: RawDocument, context: RunContext) -> str | None:
        return "xls"

    def _load_document(self, connection: Any, document: RawDocument, context: RunContext, document_id: int,
                       accepted: list, *args: Any, **kwargs: Any) -> Any:
        """When reprocessing, skip records this same document already loaded.

        The VERSION comparison is against the *latest* stored row. Replaying an
        original workbook after its same-day revision would otherwise re-insert
        the original values as a "new" version (and then the revision again).
        A record already stored from this document (same natural key and
        ``document_id``) adds nothing, so it is counted as unchanged; records the
        document never loaded (e.g. rejected earlier) still go through.
        """
        if not self.reload_duplicate_documents:
            return super()._load_document(connection, document, context, document_id, accepted, *args, **kwargs)
        from sqlalchemy import text  # local: only needed when reprocessing

        columns = ", ".join(self.natural_key)
        stored = {tuple(row) for row in connection.execute(
            text(f"SELECT {columns} FROM {self.settings.db_schema}.{self.target_table} WHERE document_id = :d"),
            {"d": document_id})}
        fresh = [(record, issues) for record, issues in accepted
                 if tuple(record.values.get(c) for c in self.natural_key) not in stored]
        load = super()._load_document(connection, document, context, document_id, fresh, *args, **kwargs)
        load.unchanged += len(accepted) - len(fresh)
        return load

    def _with_availability(self, document: RawDocument, records: list[ParsedRecord]) -> list[ParsedRecord]:
        avail = self._by_document[id(document)].availability
        timing = {"source_published_at": avail.published_at, "source_available_at": avail.available_at,
                  "availability_basis": avail.basis}
        return [ParsedRecord(r.key, {**r.values, **timing}, r.raw) for r in records]

    def _availability_issues(self, document: RawDocument) -> list[DataQualityIssue]:
        w = self._by_document[id(document)]
        if w.availability.note is None:
            return []
        return [DataQualityIssue(self.target_table, "ESTIMATED_AVAILABILITY",
                                 f"{w.entry.file_name}: actual upload time not used ({w.availability.note}); "
                                 f"availability estimated as {w.availability.available_at.isoformat()}",
                                 Severity.WARNING, w.entry.file_name)]


# ------------------------------------------------------------------ section A

class SupplyPipeline(_PspSectionPipeline):
    """Section A -> ``grid_daily_power_supply`` (5 regions + All India)."""

    dataset = _catalog_dataset("GRID_INDIA_PSP_DAILY")
    target_table = "grid_daily_power_supply"
    natural_key = ("region_code", "report_date")
    nullable_key_columns = ("region_code",)
    section_label = "A"

    def parse(self, document: RawDocument, context: RunContext) -> ParseResult:
        workbook = read_workbook(document.content, expected_date=document.source_report_date)
        result = parse_section_a(workbook)
        self._parsed[id(document)] = result
        return ParseResult(self._with_availability(document, result.records), result.failures)

    def validator(self) -> Validator:
        return CompositeValidator(
            RequiredFields("report_date", "energy_met_mu", "max_demand_met_mw"),
            *(NumericRange(f, minimum=0) for f in (
                "evening_peak_demand_met_mw", "evening_peak_shortage_mw", "energy_met_mu", "hydro_gen_mu",
                "wind_gen_mu", "solar_gen_mu", "energy_shortage_mu", "max_demand_met_mw")),
        )

    def document_issues(self, document: RawDocument, parsed: ParseResult, context: RunContext) -> list[DataQualityIssue]:
        result = self._parsed.pop(id(document), None)
        issues = self._availability_issues(document)
        if result is None:
            return issues
        day = document.source_report_date
        for label in result.unknown_labels:
            issues.append(DataQualityIssue(self.target_table, "UNKNOWN_LABEL",
                                           f"Section A row {label!r} on {day} is not mapped to a column; not loaded",
                                           Severity.WARNING, f"{day}|{label}"))
        for field, (regional_sum, published) in result.totals.items():
            difference = regional_sum - published
            if abs(difference) > _tolerance(field):
                issues.append(DataQualityIssue(
                    self.target_table, "TOTAL_MISMATCH",
                    f"Section A {field} on {day}: sum of regions {regional_sum} vs published All India {published} "
                    f"(difference {difference}); both stored as published",
                    Severity.WARNING, f"{day}|{field}"))
        return issues


# -------------------------------------------------------------- sections C, J

def _unmapped_area(record: ParsedRecord) -> list[ValidationIssue]:
    if record.values.get("area_code") is not None:
        return []
    return [ValidationIssue("UNMAPPED_ENTITY", f"Area {record.values.get('area_name')!r} is not a known state, "
                            "system, bulk consumer or entity (src/pipelines/grid_india/entities.py)",
                            Severity.ERROR, "area_code")]


class StatesPipeline(_PspSectionPipeline):
    """Sections C and J -> ``grid_state_power_supply``."""

    dataset = _catalog_dataset("GRID_INDIA_PSP_STATES")
    target_table = "grid_state_power_supply"
    natural_key = ("area_code", "report_date")
    section_label = "C+J"

    def parse(self, document: RawDocument, context: RunContext) -> ParseResult:
        workbook = read_workbook(document.content, expected_date=document.source_report_date)
        result, has_j = parse_states(workbook)
        self._parsed[id(document)] = (result, has_j)
        return ParseResult(self._with_availability(document, result.records), result.failures)

    def validator(self) -> Validator:
        return CompositeValidator(
            FunctionValidator(_unmapped_area),
            RequiredFields("report_date", "area_name", "energy_met_mu"),
            *(NumericRange(f, minimum=0) for f in ("max_demand_met_mw", "shortage_at_max_demand_mw",
                                                    "energy_met_mu", "energy_shortage_mu")),
        )

    def document_issues(self, document: RawDocument, parsed: ParseResult, context: RunContext) -> list[DataQualityIssue]:
        stashed = self._parsed.pop(id(document), None)
        issues = self._availability_issues(document)
        if stashed is None:
            return issues
        result, has_j = stashed
        day = document.source_report_date
        present = {r.values["area_code"] for r in result.records}
        for code in sorted(EXPECTED_SECTION_C - present):
            issues.append(DataQualityIssue(self.target_table, "MISSING_AREA",
                                           f"{code} not found in section C on {day}", Severity.WARNING,
                                           f"{day}|{code}"))
        for mismatch in result.region_label_mismatches:
            issues.append(DataQualityIssue(self.target_table, "REGION_LABEL_MISMATCH", f"{day}: {mismatch}",
                                           Severity.WARNING, str(day)))
        if has_j:
            issues.extend(self._bulk_consumer_checks(result.records, day))
        return issues

    def _bulk_consumer_checks(self, records: list[ParsedRecord], day: Any) -> list[DataQualityIssue]:
        """'Bulk Consumer_<region> ISTS' (section C) should equal the sum of that region's section J entities."""
        bulk = {r.values["region_code"]: r.values for r in records if r.values.get("area_type") == "BULK_CONSUMER"}
        entities: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for r in records:
            if r.values.get("area_type") == "ENTITY" and r.raw.get("section") == "J":
                entities[r.values["region_code"]].append(r.values)
        issues = []
        for region in REGION_HEADINGS.values():
            if region not in bulk:
                continue
            for field in ("energy_met_mu", "drawal_schedule_mu"):
                published = bulk[region].get(field)
                if published is None:
                    continue
                parts = entities.get(region, [])
                total = sum((e.get(field) or Decimal(0) for e in parts), Decimal(0))
                # Each entity is published rounded (to 0.1 MU), so allow half a unit per part.
                if abs(total - published) > _tolerance(field) * max(1, len(parts)):
                    issues.append(DataQualityIssue(
                        self.target_table, "TOTAL_MISMATCH",
                        f"BULK_{region} {field} on {day}: published {published} vs sum of section J entities {total} "
                        f"(difference {total - published}); both stored as published",
                        Severity.WARNING, f"{day}|BULK_{region}|{field}"))
        return issues


# ------------------------------------------------------------------ section G

def _unmapped_fuel(record: ParsedRecord) -> list[ValidationIssue]:
    if record.values.get("fuel_type") is not None:
        return []
    return [ValidationIssue("UNMAPPED_FUEL", f"Fuel row {record.values.get('fuel_label')!r} is not a known Section G "
                            "fuel label (src/pipelines/grid_india/parse_psp.py FUEL_TYPES); not loaded",
                            Severity.ERROR, "fuel_type")]


class FuelPipeline(_PspSectionPipeline):
    """Section G -> ``grid_sourcewise_generation`` (region x fuel; All India and Total validated, not stored)."""

    dataset = _catalog_dataset("GRID_INDIA_GENERATION")
    target_table = "grid_sourcewise_generation"
    natural_key = ("region_code", "fuel_type", "report_date")
    section_label = "G"

    def parse(self, document: RawDocument, context: RunContext) -> ParseResult:
        workbook = read_workbook(document.content, expected_date=document.source_report_date)
        result = parse_section_g(workbook)
        self._parsed[id(document)] = result
        return ParseResult(self._with_availability(document, result.records), result.failures)

    def validator(self) -> Validator:
        return CompositeValidator(
            FunctionValidator(_unmapped_fuel),
            RequiredFields("region_code", "report_date", "fuel_label"),
            NumericRange("gross_generation_mu", minimum=0),
        )

    def document_issues(self, document: RawDocument, parsed: ParseResult, context: RunContext) -> list[DataQualityIssue]:
        result = self._parsed.pop(id(document), None)
        issues = self._availability_issues(document)
        if result is None:
            return issues
        day = document.source_report_date
        for fuel in result.missing_fuels:
            issues.append(DataQualityIssue(self.target_table, "MISSING_FUEL",
                                           f"Section G fuel {fuel} not found on {day}", Severity.WARNING,
                                           f"{day}|{fuel}"))
        for what, parts_sum, published, _ in result.totals:
            difference = parts_sum - published
            if abs(difference) > MU_TOLERANCE:
                issues.append(DataQualityIssue(
                    self.target_table, "TOTAL_MISMATCH",
                    f"Section G {what} on {day}: {parts_sum} vs published {published} (difference {difference}); "
                    "values stored as published", Severity.WARNING, f"{day}|{what}"))
        return issues


# ------------------------------------------------------------------ section F

def _unmapped_sector(record: ParsedRecord) -> list[ValidationIssue]:
    if record.values.get("sector") is not None:
        return []
    return [ValidationIssue("UNMAPPED_SECTOR", f"Outage row {record.raw.get('sector_label')!r} is not a known Section F "
                            "sector (src/pipelines/grid_india/parse_psp.py OUTAGE_SECTORS); not loaded",
                            Severity.ERROR, "sector")]


class OutagePipeline(_PspSectionPipeline):
    """Section F -> ``grid_generation_outage`` (region x sector; TOTAL column and Total row validated, not stored)."""

    dataset = _catalog_dataset("GRID_INDIA_PSP_OUTAGE")
    target_table = "grid_generation_outage"
    natural_key = ("region_code", "sector", "report_date")
    section_label = "F"

    def parse(self, document: RawDocument, context: RunContext) -> ParseResult:
        workbook = read_workbook(document.content, expected_date=document.source_report_date)
        result = parse_section_f(workbook)
        self._parsed[id(document)] = result
        return ParseResult(self._with_availability(document, result.records), result.failures)

    def validator(self) -> Validator:
        return CompositeValidator(
            FunctionValidator(_unmapped_sector),
            RequiredFields("region_code", "report_date"),
            NumericRange("outage_capacity_mw", minimum=0),
        )

    def document_issues(self, document: RawDocument, parsed: ParseResult, context: RunContext) -> list[DataQualityIssue]:
        result = self._parsed.pop(id(document), None)
        issues = self._availability_issues(document)
        if result is None:
            return issues
        day = document.source_report_date
        for sector in result.missing_sectors:
            issues.append(DataQualityIssue(self.target_table, "MISSING_SECTOR",
                                           f"Section F sector {sector} not found on {day}", Severity.WARNING,
                                           f"{day}|{sector}"))
        for what, parts_sum, published in result.totals:
            difference = parts_sum - published
            if abs(difference) > MW_TOLERANCE:
                issues.append(DataQualityIssue(
                    self.target_table, "TOTAL_MISMATCH",
                    f"Section F {what} on {day}: {parts_sum} vs published {published} (difference {difference}); "
                    "values stored as published", Severity.WARNING, f"{day}|{what}"))
        return issues


SECTION_PIPELINES: dict[str, type[_PspSectionPipeline]] = {
    "supply": SupplyPipeline, "states": StatesPipeline, "fuel": FuelPipeline, "outage": OutagePipeline,
}
