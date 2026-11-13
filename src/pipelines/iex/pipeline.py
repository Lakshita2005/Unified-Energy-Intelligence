"""IEX DAM, GDAM and RTM price ingestion pipelines.

All three reuse :class:`~src.ingestion.base.BaseIngestionPipeline`. One
:meth:`run` covers one market for a range of delivery days (at most 31) and
fetches:

* the market snapshot -> unconstrained MCP (``area_code = 'MKT'``);
* for DAM and GDAM, the price of each of the 13 bid areas (one document each).

Each response is a separate raw document and transaction. Duplicate handling
uses the natural key ``(market_code, area_code, delivery_date, time_block)``
with :attr:`ConflictPolicy.VERSION`: an unchanged price is skipped, a revised
price gets a new row tied to the new run (earlier rows are kept).
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal
from typing import ClassVar

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
from src.pipelines.iex import api
from src.pipelines.iex.parser import parse_area_prices, parse_snapshot

REQUEST_PAUSE_SECONDS = 0.5  # between consecutive requests of one run, to keep load on IEX low


def _catalog_dataset(code: str) -> DatasetDefinition:
    return next(entry.definition for entry in DATASETS if entry.definition.code == code)


def _above_cap(record: ParsedRecord) -> list[ValidationIssue]:
    price, day = record.values.get("price_inr_per_mwh"), record.values.get("delivery_date")
    cap = api.price_cap(day) if isinstance(day, date) else None
    # Prices above ABSOLUTE_MAX_PRICE are rejected by the range rule instead.
    if cap is not None and isinstance(price, Decimal) and cap < price <= api.ABSOLUTE_MAX_PRICE:
        return [ValidationIssue("ABOVE_PRICE_CAP", f"Price {price} Rs/MWh is above the {cap} Rs/MWh cap recorded "
                                f"for {day}; loaded, but the cap schedule may be out of date",
                                Severity.WARNING, "price_inr_per_mwh")]
    return []


def price_validator() -> Validator:
    """Required fields, prices within [floor, absolute maximum], and a warning above the cap in force."""
    return CompositeValidator(
        RequiredFields("market_code", "area_code", "delivery_date", "time_block", "price_inr_per_mwh"),
        NumericRange("price_inr_per_mwh", minimum=api.PRICE_FLOOR, maximum=api.ABSOLUTE_MAX_PRICE),
        NumericRange("time_block", minimum=1, maximum=api.BLOCKS_PER_DAY),
        FunctionValidator(_above_cap),
    )


def _document_name(market: api.Market, area: str, start: date, end: date) -> str:
    # "iex_dam__mkt__2026-11-10__2026-11-12.json"; parse() and raw storage read the area back.
    return f"{market.dataset_code.lower()}__{area.lower()}__{start.isoformat()}__{end.isoformat()}.json"


def _area_of(document: RawDocument) -> str:
    return (document.document_name or "").split("__")[1].upper()


class _IexPipeline(BaseIngestionPipeline):
    """Behaviour shared by the DAM, GDAM and RTM pipelines."""

    source = SOURCES["IEX"]
    target_table = "iex_market_price"
    natural_key = ("market_code", "area_code", "delivery_date", "time_block")
    conflict_policy = ConflictPolicy.VERSION
    pipeline_version = "iex-1.0.0"
    market: ClassVar[api.Market]

    def areas(self) -> list[str]:
        """``MKT`` (from the snapshot) plus the bid areas, if fetched for this market."""
        return [api.MKT_AREA, *(api.BID_AREAS if self.market.area_prices else ())]

    def raw_storage_partition(self, document: RawDocument, context: RunContext) -> str | None:
        return f"{_area_of(document).lower()}_{context.parameters['start_date']}_{context.parameters['end_date']}"

    def fetch(self, context: RunContext) -> list[RawDocument]:
        start, end = date.fromisoformat(context.parameters["start_date"]), date.fromisoformat(context.parameters["end_date"])
        base = self.settings.iex_api_base_url
        requests = [(api.MKT_AREA, api.snapshot_url(base, self.market), api.snapshot_params(start, end))]
        if self.market.area_prices:
            requests += [(area, api.area_price_url(base, self.market), api.area_price_params(start, end, area))
                         for area in api.BID_AREAS]
        documents = []
        for index, (area, url, params) in enumerate(requests):
            if index:
                self._sleep(REQUEST_PAUSE_SECONDS)
            context.log.info("Requesting %s %s for %s..%s", url, area, start, end)
            response = api.call_api(self.http, url, params)
            documents.append(RawDocument(
                content=response.content,
                retrieved_at=response.retrieved_at,
                extension="json",
                content_type=response.content_type,
                published_at=None,  # IEX states no publication time
                source_url=response.url,
                document_name=_document_name(self.market, area, start, end),
                source_report_date=start if start == end else None,
            ))
        return documents

    def parse(self, document: RawDocument, context: RunContext) -> ParseResult:
        start, end = date.fromisoformat(context.parameters["start_date"]), date.fromisoformat(context.parameters["end_date"])
        area = _area_of(document)
        if area == api.MKT_AREA:
            return parse_snapshot(document.content, market=self.market, start=start, end=end)
        return parse_area_prices(document.content, market=self.market, area=area, start=start, end=end)

    def validator(self) -> Validator:
        return price_validator()

    def document_issues(self, document: RawDocument, parsed: ParseResult, context: RunContext) -> list[DataQualityIssue]:
        """Flag delivery days without exactly 96 distinct blocks.

        A past day with missing blocks is a WARNING; today's RTM (still being
        cleared) and tomorrow's DAM/GDAM are INFO.
        """
        start, end = date.fromisoformat(context.parameters["start_date"]), date.fromisoformat(context.parameters["end_date"])
        today = date.fromisoformat(context.parameters["today"])
        area = _area_of(document)
        blocks: dict[date, set[int]] = defaultdict(set)
        for record in parsed.records:
            blocks[record.values["delivery_date"]].add(record.values["time_block"])
        issues = []
        day = start
        while day <= end:
            count = len(blocks.get(day, ()))
            if count != api.BLOCKS_PER_DAY:
                in_progress = day >= today
                code = "MISSING_DAY" if count == 0 else "INCOMPLETE_DAY"
                issues.append(DataQualityIssue(
                    self.target_table, code,
                    f"{self.market.market_code} {area} {day}: {count} of {api.BLOCKS_PER_DAY} blocks"
                    + (" (day not complete yet)" if in_progress else ""),
                    Severity.INFO if in_progress else Severity.WARNING,
                    f"{self.market.market_code}|{area}|{day}"))
            day += timedelta(days=1)
        return issues


class IexDamPipeline(_IexPipeline):
    """Day-Ahead Market: MCP and 13 bid-area prices."""

    dataset = _catalog_dataset(api.DAM.dataset_code)
    market = api.DAM


class IexGdamPipeline(_IexPipeline):
    """Green Day-Ahead Market: unconstrained MCP and 13 bid-area prices."""

    dataset = _catalog_dataset(api.GDAM.dataset_code)
    market = api.GDAM


class IexRtmPipeline(_IexPipeline):
    """Real-Time Market: MCP only (the area-price endpoint returns no RTM data)."""

    dataset = _catalog_dataset(api.RTM.dataset_code)
    market = api.RTM
