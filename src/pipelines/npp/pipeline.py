"""NPP demand and generation ingestion pipelines.

Both reuse :class:`~src.ingestion.base.BaseIngestionPipeline`; they add only
the NPP request (``fetch``), response parsing (``parse``) and validation. One
:meth:`run` covers one dataset for one IST day; see
:mod:`src.pipelines.npp.runner` for date ranges and dry runs.

Duplicate handling uses the natural key ``(name_of_data, observed_at,
state_code, region_code)`` with :attr:`ConflictPolicy.VERSION`. The geography
columns are NULL (All-India) and are matched null-safely
(``IS NOT DISTINCT FROM``). Re-fetching a day loads only snapshots not seen
before; a snapshot whose value changed gets a new row tied to the new run.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import ClassVar

from src.ingestion.base import BaseIngestionPipeline, RunContext
from src.ingestion.catalog import DATASETS, SOURCES
from src.ingestion.models import DatasetDefinition, ParsedRecord, ParseResult, RawDocument, ValidationIssue
from src.ingestion.validation import CompositeValidator, FunctionValidator, NumericRange, RequiredFields, Validator
from src.loaders.table_loader import ConflictPolicy
from src.pipelines.npp import api
from src.pipelines.npp.parser import parse_response

MAX_MW = Decimal("500000")  # well above All-India peak demand (~250 GW in 2026)


def _catalog_dataset(code: str) -> DatasetDefinition:
    return next(entry.definition for entry in DATASETS if entry.definition.code == code)


def npp_validator(series: tuple[str, ...]) -> Validator:
    """Required fields, a plausible MW range, and only the expected series."""

    def known_series(record: ParsedRecord) -> list[ValidationIssue]:
        name = record.values.get("name_of_data")
        if name in series:
            return []
        return [ValidationIssue("UNKNOWN_SERIES", f"Series {name!r} is not one of {list(series)}; not loaded "
                                "(new or renamed series must be reviewed first)", field="name_of_data")]

    return CompositeValidator(
        RequiredFields("observed_at", "name_of_data", "value_of_data"),
        NumericRange("value_of_data", minimum=Decimal("0"), maximum=MAX_MW),
        FunctionValidator(known_series),
    )


class _NppPipeline(BaseIngestionPipeline):
    """Behaviour shared by the demand and generation pipelines."""

    source = SOURCES["NPP"]
    target_table = "npp_demand_generation"
    natural_key = ("name_of_data", "observed_at", "state_code", "region_code")
    nullable_key_columns = ("state_code", "region_code")
    conflict_policy = ConflictPolicy.VERSION
    pipeline_version = "npp-1.0.0"
    endpoint: ClassVar[api.NppEndpoint]

    def raw_storage_partition(self, document: RawDocument, context: RunContext) -> str | None:
        return str(context.parameters["date"])

    def fetch(self, context: RunContext) -> list[RawDocument]:
        day = date.fromisoformat(context.parameters["date"])
        url = api.endpoint_url(self.settings.npp_base_url, self.endpoint)
        context.log.info("Requesting %s for %s", url, day)
        response = api.call_api(self.http, url, api.request_params(day))
        return [RawDocument(
            content=response.content,
            retrieved_at=response.retrieved_at,
            extension="json",
            content_type=response.content_type,
            published_at=None,  # NPP does not state a publication time
            source_url=response.url,
            document_name=f"{self.dataset.code.lower()}_{day.isoformat()}.json",
            source_report_date=day,
        )]

    def parse(self, document: RawDocument, context: RunContext) -> ParseResult:
        return parse_response(document.content, requested_date=date.fromisoformat(context.parameters["date"]),
                              expected_series=self.endpoint.series)

    def validator(self) -> Validator:
        return npp_validator(self.endpoint.series)


class NppDemandPipeline(_NppPipeline):
    """All-India demand met (``demandmet1chartdata``)."""

    dataset = _catalog_dataset(api.DEMAND.dataset_code)
    endpoint = api.DEMAND


class NppGenerationPipeline(_NppPipeline):
    """All-India generation by source (``demandmet2chartdata``)."""

    dataset = _catalog_dataset(api.GENERATION.dataset_code)
    endpoint = api.GENERATION
