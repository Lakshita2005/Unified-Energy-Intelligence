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
| :class:`ExchangePipeline` | E | grid_regional_exchange | region_code, report_date |
| :class:`IrLinesPipeline` | IR-Line inter-regional | grid_interregional_line_exchange | from_region_code, to_region_code, line_name, report_date |
| :class:`IntlLinesPipeline` | IR-Line international | grid_international_line_exchange | country_code, line_name, report_date |
| :class:`CrossBorderPipeline` | CrossBorder | grid_crossborder_market_schedule | country_code, contract_type, power_exchange (NULL for GNA/bilateral), direction, report_date |
| :class:`TimeSeriesPipeline` | TimeSeries | grid_15min_scada | region_code (NULL = All India), interval_start |

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
from src.pipelines.grid_india.errors import InvalidWorkbookError
from src.pipelines.grid_india.parse_psp import (
    TIMESERIES_FIRST_PUBLISHED,
    parse_timeseries,
    CROSSBORDER_FIRST_PUBLISHED,
    parse_crossborder,
    parse_section_a,
    parse_section_e,
    parse_section_f,
    parse_intl_lines,
    parse_ir_lines,
    parse_section_g,
    parse_states,
    read_workbook,
)
from src.pipelines.grid_india.reference import line_country

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


# ------------------------------------------------------------------ section E

class ExchangePipeline(_PspSectionPipeline):
    """Section E -> ``grid_regional_exchange`` (signed Schedule / Actual / published O/D/U/D per region)."""

    dataset = _catalog_dataset("GRID_INDIA_EXCHANGE")
    target_table = "grid_regional_exchange"
    natural_key = ("region_code", "report_date")
    section_label = "E"

    def parse(self, document: RawDocument, context: RunContext) -> ParseResult:
        workbook = read_workbook(document.content, expected_date=document.source_report_date)
        result = parse_section_e(workbook)
        self._parsed[id(document)] = result
        return ParseResult(self._with_availability(document, result.records), result.failures)

    def validator(self) -> Validator:
        # Values are signed (import/over-drawal +ve, export/under-drawal -ve): no range limits.
        return RequiredFields("region_code", "report_date", "sign_convention", "scheduled_exchange_mu",
                              "actual_exchange_mu")

    def document_issues(self, document: RawDocument, parsed: ParseResult, context: RunContext) -> list[DataQualityIssue]:
        result = self._parsed.pop(id(document), None)
        issues = self._availability_issues(document)
        if result is None:
            return issues
        day = document.source_report_date
        for label in result.unknown_labels:
            issues.append(DataQualityIssue(self.target_table, "UNKNOWN_LABEL",
                                           f"Section E row {label!r} on {day} is not mapped to a column; not loaded",
                                           Severity.WARNING, f"{day}|{label}"))
        for column in result.unknown_columns:
            issues.append(DataQualityIssue(self.target_table, "UNKNOWN_COLUMN",
                                           f"Section E column {column!r} on {day} is not a known region; not loaded",
                                           Severity.WARNING, f"{day}|{column}"))
        for name in result.missing_fields:
            issues.append(DataQualityIssue(self.target_table, "MISSING_FIELD",
                                           f"Section E row for {name} not found on {day}; stored as NULL",
                                           Severity.WARNING, f"{day}|{name}"))
        for what, parts_sum, published in result.totals:
            if abs(parts_sum - published) > MU_TOLERANCE:
                issues.append(DataQualityIssue(
                    self.target_table, "TOTAL_MISMATCH",
                    f"Section E {what} on {day}: {parts_sum} vs published {published} (difference "
                    f"{parts_sum - published}); values stored as published", Severity.WARNING, f"{day}|{what}"))
        for region, computed, published in result.od_checks:
            if abs(computed - published) > MU_TOLERANCE:
                issues.append(DataQualityIssue(
                    self.target_table, "OD_UD_MISMATCH",
                    f"Section E {region} on {day}: published O/D/U/D {published} vs Actual - Schedule {computed} "
                    f"(difference {published - computed}); published value stored", Severity.WARNING,
                    f"{day}|{region}"))
        return issues


# ---------------------------------------------------------------- IR-Line

def _unmapped_ir_region(record: ParsedRecord) -> list[ValidationIssue]:
    if record.values.get("from_region_code") and record.values.get("to_region_code"):
        return []
    return [ValidationIssue("UNMAPPED_REGION", f"IR-Line block {record.raw.get('block')!r} does not name two known "
                            "regions (NR, WR, SR, ER, NER); not loaded", Severity.ERROR, "from_region_code")]


class IrLinesPipeline(_PspSectionPipeline):
    """IR-Line inter-regional part -> ``grid_interregional_line_exchange`` (one row per line)."""

    dataset = _catalog_dataset("GRID_INDIA_PSP_IR_LINES")
    target_table = "grid_interregional_line_exchange"
    natural_key = ("from_region_code", "to_region_code", "line_name", "report_date")
    section_label = "IR-Line (inter-regional)"

    def parse(self, document: RawDocument, context: RunContext) -> ParseResult:
        workbook = read_workbook(document.content, expected_date=document.source_report_date)
        result = parse_ir_lines(workbook)
        self._parsed[id(document)] = result
        return ParseResult(self._with_availability(document, result.records), result.failures)

    def validator(self) -> Validator:
        return CompositeValidator(
            FunctionValidator(_unmapped_ir_region),
            RequiredFields("report_date", "line_name", "net_exchange_mu", "direction_convention"),
            # Import and export are published as separate non-negative magnitudes.
            *(NumericRange(f, minimum=0) for f in ("max_import_mw", "max_export_mw", "import_mu", "export_mu")),
        )

    def document_issues(self, document: RawDocument, parsed: ParseResult, context: RunContext) -> list[DataQualityIssue]:
        result = self._parsed.pop(id(document), None)
        issues = self._availability_issues(document)
        if result is None:
            return issues
        day = document.source_report_date
        for row in result.unknown_rows:
            issues.append(DataQualityIssue(self.target_table, "UNKNOWN_ROW",
                                           f"IR-Line row on {day} not recognised; not loaded: {row}",
                                           Severity.WARNING, str(day)))
        for record in result.records:
            v = record.values
            if None in (v["import_mu"], v["export_mu"], v["net_exchange_mu"]):
                continue
            computed = v["import_mu"] - v["export_mu"]
            if abs(computed - v["net_exchange_mu"]) > MU_TOLERANCE:
                issues.append(DataQualityIssue(
                    self.target_table, "NET_MISMATCH",
                    f"IR-Line {v['line_name']} on {day}: published NET {v['net_exchange_mu']} vs Import - Export "
                    f"{computed}; published value stored", Severity.WARNING, record.key))
        for pair, total, published, excluded in result.subtotals:
            if all(abs(t - p) <= MU_TOLERANCE for t, p in zip(total, published)):
                continue
            if excluded:
                issues.append(DataQualityIssue(
                    self.target_table, "SUBTOTAL_EXCLUDES_LINE",
                    f"IR-Line {pair} subtotal on {day} equals the sum of its lines without {excluded!r}",
                    Severity.INFO, f"{day}|{pair}"))
            else:
                issues.append(DataQualityIssue(
                    self.target_table, "TOTAL_MISMATCH",
                    f"IR-Line {pair} subtotal on {day}: lines (import, export, net) {tuple(map(str, total))} vs "
                    f"published {tuple(map(str, published))}; values stored as published", Severity.WARNING,
                    f"{day}|{pair}"))
        return issues


def _intl_entity_checks(record: ParsedRecord) -> list[ValidationIssue]:
    v, raw = record.values, record.raw or {}
    if v.get("country_code") is None:
        return [ValidationIssue("UNMAPPED_COUNTRY", f"International line {v.get('line_name')!r} is not in the exact "
                                "line-name reference (src/pipelines/grid_india/reference.py); not loaded",
                                Severity.ERROR, "country_code")]
    issues = []
    if raw.get("country_label") and raw.get("country_label_code") != v["country_code"]:
        issues.append(ValidationIssue("COUNTRY_LABEL_CONFLICT", f"Published country label {raw['country_label']!r} "
                                      f"disagrees with the reference ({v['country_code']}) for {v['line_name']!r}; "
                                      "not loaded", Severity.ERROR, "country_code"))
    if v.get("region_code") is None:
        issues.append(ValidationIssue("UNMAPPED_REGION", f"Region cell {v.get('region_label')!r} does not start with "
                                      "a known region; not loaded", Severity.ERROR, "region_code"))
    return issues


class IntlLinesPipeline(_PspSectionPipeline):
    """IR-Line international part -> ``grid_international_line_exchange`` (one row per line)."""

    dataset = _catalog_dataset("GRID_INDIA_PSP_INTL_LINES")
    target_table = "grid_international_line_exchange"
    natural_key = ("country_code", "line_name", "report_date")
    section_label = "IR-Line (international)"

    def parse(self, document: RawDocument, context: RunContext) -> ParseResult:
        workbook = read_workbook(document.content, expected_date=document.source_report_date)
        result = parse_intl_lines(workbook, line_country=line_country)
        self._parsed[id(document)] = result
        return ParseResult(self._with_availability(document, result.records), result.failures)

    def validator(self) -> Validator:
        # Values are signed (import +ve, export -ve): no range limits.
        return CompositeValidator(
            FunctionValidator(_intl_entity_checks),
            RequiredFields("report_date", "line_name", "region_label", "energy_exchange_mu", "sign_convention"),
        )

    def document_issues(self, document: RawDocument, parsed: ParseResult, context: RunContext) -> list[DataQualityIssue]:
        result = self._parsed.pop(id(document), None)
        issues = self._availability_issues(document)
        if result is None:
            return issues
        day = document.source_report_date
        for label, total, published in result.section_d:
            if abs(total - published) > MU_TOLERANCE:
                issues.append(DataQualityIssue(
                    self.target_table, "SECTION_D_MISMATCH",
                    f"IR-Line international {label} on {day}: sum of lines {total} vs Section D Actual {published} "
                    f"(difference {total - published}); values stored as published", Severity.WARNING,
                    f"{day}|{label}"))
        for line in result.mw_not_reported:
            issues.append(DataQualityIssue(
                self.target_table, "MW_NOT_REPORTED",
                f"IR-Line international {line!r} on {day}: Max = Min = Avg = 0 MW with non-zero energy; stored as "
                "published", Severity.INFO, f"{day}|{line[:60]}"))
        return issues


# ---------------------------------------------------------------- CrossBorder

def _crossborder_checks(record: ParsedRecord) -> list[ValidationIssue]:
    v, raw = record.values, record.raw or {}
    issues = []
    if v.get("country_code") is None:
        issues.append(ValidationIssue("UNMAPPED_COUNTRY", f"CrossBorder country {raw.get('country_label')!r} is not a "
                                      "known exact label (src/pipelines/grid_india/reference.py CROSSBORDER_COUNTRIES); "
                                      "not loaded", Severity.ERROR, "country_code"))
    if raw.get("duplicate"):
        issues.append(ValidationIssue("DUPLICATE_ROW", f"CrossBorder {v.get('direction')} block lists "
                                      f"{raw.get('country_label')!r} more than once; none of its rows loaded",
                                      Severity.ERROR, "country_code"))
    value = v.get("scheduled_energy_mu")
    if v.get("direction") in ("EXPORT", "IMPORT") and value is not None and value < 0:
        issues.append(ValidationIssue("OUT_OF_RANGE", f"CrossBorder {v['direction']} value {value} is negative; "
                                      "Export and Import are published as non-negative magnitudes", Severity.ERROR,
                                      "scheduled_energy_mu"))
    return issues


class CrossBorderPipeline(_PspSectionPipeline):
    """CrossBorder -> ``grid_crossborder_market_schedule`` (scheduled MU per block x country x contract x exchange).

    Export and Import rows are non-negative; Net rows (direction 'NET', migration 0005) are signed as published.
    The row TOTAL column and the Total rows are validated, not stored (decision C4).
    """

    dataset = _catalog_dataset("GRID_INDIA_PSP_CROSSBORDER")
    target_table = "grid_crossborder_market_schedule"
    natural_key = ("country_code", "contract_type", "power_exchange", "direction", "report_date")
    nullable_key_columns = ("power_exchange",)
    section_label = "CrossBorder"

    def parse(self, document: RawDocument, context: RunContext) -> ParseResult:
        workbook = read_workbook(document.content, expected_date=document.source_report_date)
        result = parse_crossborder(workbook)
        self._parsed[id(document)] = result
        return ParseResult(self._with_availability(document, result.records), result.failures)

    def validator(self) -> Validator:
        return CompositeValidator(
            FunctionValidator(_crossborder_checks),
            RequiredFields("report_date", "contract_type", "direction", "sign_convention"),
        )

    def document_issues(self, document: RawDocument, parsed: ParseResult, context: RunContext) -> list[DataQualityIssue]:
        result = self._parsed.pop(id(document), None)
        issues = self._availability_issues(document)
        if result is None:
            return issues
        day = document.source_report_date
        if result.not_published:
            issues.append(DataQualityIssue(
                self.target_table, "SECTION_NOT_PUBLISHED",
                f"{document.document_name}: no CrossBorder sheet; it is published from {CROSSBORDER_FIRST_PUBLISHED} "
                f"(report date {day})", Severity.INFO, str(day)))
            return issues
        for what in result.missing_values:
            issues.append(DataQualityIssue(self.target_table, "MISSING_VALUE",
                                           f"CrossBorder cell {what} is blank; stored as NULL, not derived",
                                           Severity.WARNING, what))
        for what in result.missing_countries:
            issues.append(DataQualityIssue(self.target_table, "MISSING_COUNTRY",
                                           f"CrossBorder {what} not found on {day}", Severity.WARNING, f"{day}|{what}"))
        for row in result.unknown_rows:
            issues.append(DataQualityIssue(self.target_table, "UNKNOWN_ROW",
                                           f"CrossBorder on {day} not recognised; not loaded: {row}",
                                           Severity.WARNING, str(day)))
        for what, parts_sum, published in result.totals:
            if abs(parts_sum - published) > MU_TOLERANCE:
                issues.append(DataQualityIssue(
                    self.target_table, "TOTAL_MISMATCH",
                    f"CrossBorder {what} on {day}: sum {parts_sum} vs published {published} (difference "
                    f"{parts_sum - published}); values stored as published", Severity.WARNING, f"{day}|{what}"))
        for what, computed, published in result.net_checks:
            if abs(computed - published) > MU_TOLERANCE:
                issues.append(DataQualityIssue(
                    self.target_table, "NET_MISMATCH",
                    f"CrossBorder {what} on {day}: published Net {published} vs Import - Export {computed} "
                    f"(difference {published - computed}); published value stored", Severity.WARNING,
                    f"{day}|{what}"))
        return issues


# ----------------------------------------------------------------- TimeSeries

def _timeseries_checks(record: ParsedRecord) -> list[ValidationIssue]:
    if (record.raw or {}).get("duplicate"):
        return [ValidationIssue("DUPLICATE_INTERVAL", f"TimeSeries mark {record.key} appears more than once; none of "
                                "its rows loaded", Severity.ERROR, "interval_start")]
    return []


class TimeSeriesPipeline(_PspSectionPipeline):
    """TimeSeries -> ``grid_15min_scada`` (all-India, region_code NULL; instantaneous values at each 15-minute mark).

    Values are stored as published (decisions T3-T5, T8): telemetry-loss zeros are flagged, not replaced; storage
    columns are NULL before they were published; a published Total that leaves out storage is kept.
    """

    dataset = _catalog_dataset("GRID_INDIA_SCADA")
    target_table = "grid_15min_scada"
    natural_key = ("region_code", "interval_start")
    nullable_key_columns = ("region_code",)
    section_label = "TimeSeries"

    def parse(self, document: RawDocument, context: RunContext) -> ParseResult:
        workbook = read_workbook(document.content, expected_date=document.source_report_date)
        result = parse_timeseries(workbook)
        section_a_max = None
        if result.records:
            try:   # same-scope cross-check (T7); Section A problems belong to the supply run, not this one
                section_a_max = next((r.values["max_demand_met_mw"] for r in parse_section_a(workbook).records
                                      if r.values["region_code"] is None), None)
            except InvalidWorkbookError:
                section_a_max = None
        self._parsed[id(document)] = (result, section_a_max)
        return ParseResult(self._with_availability(document, result.records), result.failures)

    def validator(self) -> Validator:
        # Values are as published: generation can be slightly negative (e.g. solar at night); no range rejection.
        return CompositeValidator(FunctionValidator(_timeseries_checks), RequiredFields("interval_start"))

    def document_issues(self, document: RawDocument, parsed: ParseResult, context: RunContext) -> list[DataQualityIssue]:
        stashed = self._parsed.pop(id(document), None)
        issues = self._availability_issues(document)
        if stashed is None:
            return issues
        result, section_a_max = stashed
        day = document.source_report_date
        t = self.target_table
        if result.not_published:
            issues.append(DataQualityIssue(t, "SECTION_NOT_PUBLISHED", f"{document.document_name}: no TimeSeries sheet; "
                                           f"it is published from {TIMESERIES_FIRST_PUBLISHED} (report date {day})",
                                           Severity.INFO, str(day)))
            return issues
        if not result.records and not result.failures:
            issues.append(DataQualityIssue(t, "NO_INTERVAL_DATA", f"{document.document_name}: TimeSeries sheet for {day} "
                                           "has headers but no 15-minute rows; nothing loaded", Severity.WARNING, str(day)))
            return issues
        for mark in result.missing_marks:
            issues.append(DataQualityIssue(t, "MISSING_INTERVAL", f"TimeSeries {day} {mark} not published; not filled",
                                           Severity.WARNING, f"{day}|{mark}"))
        for what in result.missing_values:
            issues.append(DataQualityIssue(t, "MISSING_VALUE", f"TimeSeries cell {what} is blank; stored as NULL, not "
                                           "derived", Severity.WARNING, what))
        for row in result.unknown_rows:
            issues.append(DataQualityIssue(t, "UNKNOWN_ROW", f"TimeSeries on {day} not recognised; not loaded: {row}",
                                           Severity.WARNING, str(day)))
        for mark, published, expected in result.net_demand_mismatches:
            issues.append(DataQualityIssue(t, "NET_DEMAND_MISMATCH", f"TimeSeries {day} {mark}: published Net Demand Met "
                                           f"{published} vs Demand - Wind - Solar {expected}; published value stored",
                                           Severity.WARNING, f"{day}|{mark}"))
        for mark, published, parts in result.total_mismatches:
            issues.append(DataQualityIssue(t, "TOTAL_MISMATCH", f"TimeSeries {day} {mark}: published Total Generation "
                                           f"{published} vs sum of sources {parts} (difference {published - parts}); "
                                           "published value stored", Severity.WARNING, f"{day}|{mark}"))
        if result.total_excludes_storage:
            marks = result.total_excludes_storage
            issues.append(DataQualityIssue(t, "TOTAL_EXCLUDES_STORAGE", f"TimeSeries {day}: published Total Generation "
                                           f"equals the sum of sources without Storage (PSP&BESS) at {len(marks)} marks "
                                           f"({marks[0]}..{marks[-1]}); published values stored", Severity.INFO, str(day)))
        for mark, reason in result.telemetry_suspect:
            issues.append(DataQualityIssue(t, "TELEMETRY_SUSPECT", f"TimeSeries {day} {mark}: {reason}; stored as "
                                           "published", Severity.WARNING, f"{day}|{mark}"))
        if section_a_max is not None and result.max_demand_mw is not None \
                and result.max_demand_mw > section_a_max + MW_TOLERANCE:
            issues.append(DataQualityIssue(t, "MAX_DEMAND_ABOVE_SECTION_A", f"TimeSeries {day}: highest 15-minute demand "
                                           f"{result.max_demand_mw} MW exceeds Section A maximum demand met (1-minute "
                                           f"SCADA) {section_a_max} MW", Severity.WARNING, str(day)))
        return issues


SECTION_PIPELINES: dict[str, type[_PspSectionPipeline]] = {
    "supply": SupplyPipeline, "states": StatesPipeline, "fuel": FuelPipeline, "outage": OutagePipeline,
    "exchange": ExchangePipeline, "ir_lines": IrLinesPipeline, "intl_lines": IntlLinesPipeline,
    "crossborder": CrossBorderPipeline, "timeseries": TimeSeriesPipeline,
}
