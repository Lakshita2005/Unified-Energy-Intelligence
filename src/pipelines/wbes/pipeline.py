"""WBES PX Dashboard pipelines: one response, three tables (WBES_PX_REGION / _LINK / _ENTITY).

The runner fetches each ``GetPowerExchangeByDetails`` response (delivery date
x exchange x market) **once**, stores it **once** in raw storage (write-once,
SHA-256) and hands the same documents to three pipelines. Each pipeline has
its own dataset and ingestion run, registers the response as a
``source_document`` of its dataset, parses its part and loads it.

| Pipeline | Dataset | Table | Natural key |
|---|---|---|---|
| :class:`PxRegionPipeline` | WBES_PX_REGION | wbes_px_region_schedule | delivery_date, power_exchange, market, wbes_region_id, time_block |
| :class:`PxLinkPipeline` | WBES_PX_LINK | wbes_px_link_schedule | delivery_date, power_exchange, market, link_name, time_block |
| :class:`PxEntityPipeline` | WBES_PX_ENTITY | wbes_px_entity_schedule | delivery_date, power_exchange, market, entity_acronym, entity_unit_acronym, entity_px_code, time_block |

Revisions (decision W4): revision number, PowerExId, the WBES type ids and the
availability basis are lineage (``extra_lineage_columns``), so a new version is
stored only when a measured value changes. Older revisions are never deleted.

Loading one document (one response, possibly several revisions):

* revisions are loaded in ascending order, one revision per loader batch, inside
  the document's transaction; issues and rejected records are written once;
* a record is **skipped** if its revision is not newer than the latest revision
  already stored for that delivery date x exchange x market *and* its item already
  has stored rows (loading it would put an older value above a newer one). Items
  with no stored rows at all (e.g. rejected before a parser fix) are loaded, so
  ``--reprocess`` can recover them;
* after loading, the items stored for that delivery date x exchange x market are
  compared with the items published in the newest revision of the response; stored
  items it no longer publishes are reported (``STORED_ITEM_ABSENT`` WARNING), because
  their last stored values would otherwise silently remain the latest rows.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date
from typing import Any, ClassVar

from sqlalchemy import text

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
from src.ingestion.validation import CompositeValidator, FunctionValidator, RequiredFields, Validator
from src.loaders.table_loader import ConflictPolicy, LoadResult, RowToLoad
from src.pipelines.wbes.api import ListedItem
from src.pipelines.wbes.parse import PxContext, PxParseResult, parse_px_records

PIPELINE_VERSION = "wbes-px-1.1.0"
# Not data values: never make a row "changed" (W4). Measurement columns must never be listed here.
PX_LINEAGE_COLUMNS = frozenset({"revision_no", "power_ex_id", "power_exchange_type_id", "market_type_id",
                                "availability_basis"})


def _catalog_dataset(code: str) -> DatasetDefinition:
    return next(entry.definition for entry in DATASETS if entry.definition.code == code)


@dataclass(frozen=True)
class PreparedResponse:
    """One fetched (or re-read) PX response, shared by the three pipelines."""

    delivery_date: date
    exchange: ListedItem
    market: ListedItem
    records: list[dict[str, Any]]
    status_code: str | None
    document: RawDocument
    stored: StoredObject
    request_body: dict[str, Any] = field(default_factory=dict)


def _duplicate_key(record: ParsedRecord) -> list[ValidationIssue]:
    if (record.raw or {}).get("duplicate_key"):
        return [ValidationIssue("DUPLICATE_KEY_IN_REVISION", "The same natural key appears more than once in one "
                                "revision (key uniqueness is unverified, W7); none of those items loaded",
                                Severity.ERROR, "natural_key")]
    return []


class _PxPipeline(BaseIngestionPipeline):
    source = SOURCES["WBES"]
    conflict_policy = ConflictPolicy.VERSION
    pipeline_version = PIPELINE_VERSION
    extra_lineage_columns = PX_LINEAGE_COLUMNS
    part: ClassVar[str]
    measurement_columns: ClassVar[tuple[str, ...]]
    item_key_columns: ClassVar[tuple[str, ...]]      # natural key without date/exchange/market/block
    required_fields: ClassVar[tuple[str, ...]]

    def __init__(self, *, responses: list[PreparedResponse] | None = None,
                 fetch_error: BaseException | None = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        overlap = set(self.measurement_columns) & set(self.extra_lineage_columns)
        if overlap:
            raise TypeError(f"{type(self).__name__}: measurement columns listed as lineage: {sorted(overlap)}")
        self.responses = list(responses or [])
        self.fetch_error = fetch_error
        self._by_document = {id(r.document): r for r in self.responses}
        self._parsed: dict[int, PxParseResult] = {}

    # The runner already fetched (or re-read) and stored the responses.
    def fetch(self, context: RunContext) -> list[RawDocument]:
        if self.fetch_error is not None:
            raise self.fetch_error
        return [r.document for r in self.responses]

    def _store_raw(self, document: RawDocument, context: RunContext) -> StoredObject:
        stored = self._by_document[id(document)].stored
        context.log.info("Using stored WBES response %s (sha256=%s)", stored.key, stored.sha256)
        return stored

    def raw_storage_partition(self, document: RawDocument, context: RunContext) -> str | None:
        return "px"

    def parse(self, document: RawDocument, context: RunContext) -> ParseResult:
        response = self._by_document[id(document)]
        ctx = PxContext(response.delivery_date, response.exchange.name, response.exchange.item_id,
                        response.market.name, response.market.item_id, document.retrieved_at,
                        self.settings.wbes_created_on_naive_timezone)
        result = parse_px_records(response.records, ctx)
        self._parsed[id(document)] = result
        return ParseResult(result.records[self.part], result.failures[self.part])

    def validator(self) -> Validator:
        return CompositeValidator(FunctionValidator(_duplicate_key), RequiredFields(*self.required_fields))

    def document_issues(self, document: RawDocument, parsed: ParseResult, context: RunContext) -> list[DataQualityIssue]:
        response = self._by_document[id(document)]
        result = self._parsed.get(id(document))
        where = f"{response.delivery_date} {response.exchange.name} {response.market.name}"
        key = f"{response.delivery_date}|{response.exchange.name}|{response.market.name}"
        issues = []
        if response.status_code is None:
            issues.append(DataQualityIssue(self.target_table, "RESPONSE_STATUS_MISSING",
                                           f"PX {where}: response has no ResponseStatus; body used as returned",
                                           Severity.WARNING, key))
        if not response.records:
            issues.append(DataQualityIssue(self.target_table, "NOT_PUBLISHED",
                                           f"PX {where}: no records (not published yet); nothing loaded",
                                           Severity.INFO, key))
        if result is not None:
            for part, code, description, ikey in result.warnings:
                if part in ("all", self.part):
                    issues.append(DataQualityIssue(self.target_table, code, f"PX {where}: {description}",
                                                   Severity.WARNING, ikey))
            for part, code, description, ikey in result.info:
                if part in ("all", self.part):
                    issues.append(DataQualityIssue(self.target_table, code, f"PX {where}: {description}",
                                                   Severity.INFO, ikey))
        return issues

    # ------------------------------------------------------------------ loading
    def _item_key(self, values: dict[str, Any]) -> tuple:
        return tuple(values.get(c) for c in self.item_key_columns)

    def _load_document(self, connection: Any, document: RawDocument, context: RunContext, document_id: int,
                       accepted: list, issues: list[DataQualityIssue], rejected_records: list, stored: StoredObject,
                       result: Any) -> LoadResult:
        response = self._by_document[id(document)]
        parsed = self._parsed.get(id(document))
        table = f"{self.settings.db_schema}.{self.target_table}"
        scope = {"d": response.delivery_date, "p": response.exchange.name, "m": response.market.name}
        where = f"{response.delivery_date} {response.exchange.name} {response.market.name}"
        issue_key = f"{response.delivery_date}|{response.exchange.name}|{response.market.name}"
        stored_max = connection.execute(text(
            f"SELECT max(revision_no) FROM {table} WHERE delivery_date = :d AND power_exchange = :p AND market = :m"),
            scope).scalar()
        columns = ", ".join(self.item_key_columns)
        stored_items = {tuple(row) for row in connection.execute(text(
            f"SELECT DISTINCT {columns} FROM {table} WHERE delivery_date = :d AND power_exchange = :p AND market = :m"),
            scope)}

        groups: dict[int, list] = defaultdict(list)
        skipped: dict[int, int] = defaultdict(int)
        recovered = 0
        for record, record_issues in accepted:
            revision = record.values["revision_no"]
            older = stored_max is not None and revision <= stored_max
            if older and self._item_key(record.values) in stored_items:
                skipped[revision] += 1          # would put an older value above a newer stored one
                continue
            recovered += older
            groups[revision].append((record, record_issues))
        if skipped:
            issues.append(DataQualityIssue(
                self.target_table, "REVISION_ALREADY_STORED",
                f"PX {where}: revision(s) {sorted(skipped)} not newer than stored revision {stored_max}; "
                f"{sum(skipped.values())} record(s) of items already stored skipped", Severity.INFO, issue_key))
        if recovered:
            issues.append(DataQualityIssue(
                self.target_table, "PREVIOUSLY_MISSING_ITEMS_LOADED",
                f"PX {where}: {recovered} record(s) of revision(s) <= {stored_max} loaded for items that had no stored "
                "rows (e.g. rejected earlier)", Severity.INFO, issue_key))

        # I-1: stored items that the newest (new) revision of this response no longer publishes.
        if parsed is not None and parsed.published_keys[self.part]:
            newest = max(parsed.published_keys[self.part])
            if stored_max is None or newest > stored_max:
                after = stored_items | {self._item_key(r.values) for batch in groups.values() for r, _ in batch}
                absent = sorted(map(str, after - parsed.published_keys[self.part][newest]))
                if absent:
                    issues.append(DataQualityIssue(
                        self.target_table, "STORED_ITEM_ABSENT",
                        f"PX {where}: {len(absent)} stored {self.part} item(s) are not published in revision {newest}; "
                        f"their last stored values remain the latest rows: {absent[:10]}"
                        f"{' ...' if len(absent) > 10 else ''}", Severity.WARNING, issue_key))

        total = LoadResult()
        revisions = sorted(groups)
        for revision in revisions[:-1]:
            self._accumulate(total, self._load_batch(connection, document, context, document_id, groups[revision],
                                                     issues, rejected_records))
        last = groups[revisions[-1]] if revisions else []
        # The base writes all issues and one rejects file, once, with the last batch (M-1).
        self._accumulate(total, super()._load_document(connection, document, context, document_id, last, issues,
                                                       rejected_records, stored, result))
        total.unchanged += sum(skipped.values())
        return total

    def _load_batch(self, connection: Any, document: RawDocument, context: RunContext, document_id: int,
                    batch: list, issues: list[DataQualityIssue], rejected_records: list) -> LoadResult:
        """Load one revision batch; its issues and rejects are collected for the final write."""
        columns = self.loader.column_names(connection)
        rows = []
        for record, _ in batch:
            row = {**record.values, **self._lineage(columns, context.ingestion_run_id, document_id)}
            if "retrieved_at" in columns:
                row.setdefault("retrieved_at", document.retrieved_at)
            if "source_published_at" in columns and document.published_at is not None:
                row.setdefault("source_published_at", document.published_at)
            rows.append(RowToLoad(record.key, row, record.raw))
        load = self.loader.load(connection, rows)
        for load_issue in load.issues:
            issues.append(self._issue(load_issue.key, load_issue, load_issue.raw))
            if load_issue.severity is Severity.ERROR:
                rejected_records.append({"key": load_issue.key, "stage": "load", "raw": load_issue.raw,
                                         "issues": [{"code": load_issue.issue_code,
                                                     "description": load_issue.description}]})
        return load

    @staticmethod
    def _accumulate(total: LoadResult, load: LoadResult) -> None:
        total.inserted += load.inserted
        total.updated += load.updated
        total.unchanged += load.unchanged
        total.versioned += load.versioned
        total.issues.extend(load.issues)


_COMMON_REQUIRED = ("delivery_date", "power_exchange", "market", "time_block", "revision_no", "retrieved_at",
                    "source_available_at", "availability_basis")


class PxRegionPipeline(_PxPipeline):
    """Region-wise injection / drawal -> ``wbes_px_region_schedule``."""

    dataset = _catalog_dataset("WBES_PX_REGION")
    target_table = "wbes_px_region_schedule"
    natural_key = ("delivery_date", "power_exchange", "market", "wbes_region_id", "time_block")
    part = "region"
    measurement_columns = ("injection_value", "drawal_value")
    item_key_columns = ("wbes_region_id",)
    required_fields = (*_COMMON_REQUIRED, "wbes_region_id")


class PxLinkPipeline(_PxPipeline):
    """Link-wise net amount -> ``wbes_px_link_schedule``."""

    dataset = _catalog_dataset("WBES_PX_LINK")
    target_table = "wbes_px_link_schedule"
    natural_key = ("delivery_date", "power_exchange", "market", "link_name", "time_block")
    part = "link"
    measurement_columns = ("net_amount_value",)
    item_key_columns = ("link_name",)
    required_fields = (*_COMMON_REQUIRED, "link_name")


class PxEntityPipeline(_PxPipeline):
    """Entity-wise injection / drawal -> ``wbes_px_entity_schedule``."""

    dataset = _catalog_dataset("WBES_PX_ENTITY")
    target_table = "wbes_px_entity_schedule"
    natural_key = ("delivery_date", "power_exchange", "market", "entity_acronym", "entity_unit_acronym",
                   "entity_px_code", "time_block")
    nullable_key_columns = ("entity_unit_acronym", "entity_px_code")
    part = "entity"
    measurement_columns = ("injection_value", "drawal_value")
    item_key_columns = ("entity_acronym", "entity_unit_acronym", "entity_px_code")
    required_fields = (*_COMMON_REQUIRED, "entity_acronym")


PX_PIPELINES: dict[str, type[_PxPipeline]] = {"region": PxRegionPipeline, "link": PxLinkPipeline,
                                              "entity": PxEntityPipeline}
