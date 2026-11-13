"""A mock pipeline used to exercise the ingestion framework. Test-only.

It reads JSON payloads supplied in memory (no external connection) and loads
them into ``pi.iex_market_price`` using a mock market and area.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Iterable
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from src.common.database import SavepointDatabase  # noqa: F401  (re-exported for tests)
from src.ingestion.base import BaseIngestionPipeline, RunContext
from src.ingestion.models import (
    DatasetDefinition,
    ParsedRecord,
    ParseFailure,
    ParseResult,
    RawDocument,
    Severity,
    SourceDefinition,
)
from src.ingestion.validation import CompositeValidator, NumericRange, RequiredFields, Validator

FIXTURE_PATH = Path(__file__).resolve().parent.parent / "fixtures" / "mock_market_prices.json"

MOCK_SOURCE = SourceDefinition("MOCK_EXCHANGE", "Mock Exchange (tests only)", "https://example.invalid/")
MOCK_DATASET = DatasetDefinition(
    "MOCK_DAM_PRICES", "Mock day-ahead prices (tests only)",
    endpoint_or_report="mock://dam-prices", native_granularity="15min", expected_frequency="daily",
)
MOCK_MARKET = "MOCK_DAM"
MOCK_AREA = "MOCK_A1"


class MockPricePipeline(BaseIngestionPipeline):
    """Loads mock day-ahead prices from in-memory JSON payloads."""

    source = MOCK_SOURCE
    dataset = MOCK_DATASET
    target_table = "iex_market_price"
    natural_key = ("market_code", "area_code", "delivery_date", "time_block")
    pipeline_version = "mock-1.0"

    def __init__(self, payloads: Iterable[bytes] | Callable[[], Iterable[bytes]], **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._payloads = payloads

    def fetch(self, context: RunContext) -> list[RawDocument]:
        payloads = self._payloads() if callable(self._payloads) else self._payloads
        documents = []
        for payload in payloads:
            header = json.loads(payload)
            documents.append(RawDocument(
                content=payload,
                retrieved_at=datetime.now(timezone.utc),
                extension="json",
                content_type="application/json",
                published_at=datetime.fromisoformat(header["published_at"]),
                source_url="mock://dam-prices",
                document_name=f"mock_dam_{header['delivery_date']}.json",
                source_report_date=date.fromisoformat(header["delivery_date"]),
            ))
        return documents

    def parse(self, document: RawDocument, context: RunContext) -> ParseResult:
        # parse_float=Decimal keeps prices exactly as published (no float rounding).
        data = json.loads(document.content, parse_float=Decimal)
        delivery_date = date.fromisoformat(data["delivery_date"])
        result = ParseResult()
        for item in data["prices"]:
            key = f"{data['market']}|{data['area']}|{data['delivery_date']}|block={item.get('block')}"
            price = item.get("price")
            if not isinstance(price, (int, Decimal)) or isinstance(price, bool):
                result.failures.append(ParseFailure(key, item, f"Price {price!r} is not a number"))
                continue
            result.records.append(ParsedRecord(
                key=key,
                values={
                    "market_code": data["market"],
                    "area_code": data["area"],
                    "delivery_date": delivery_date,
                    "time_block": item.get("block"),
                    "price_inr_per_mwh": price,
                },
                raw=item,
            ))
        return result

    def validator(self) -> Validator:
        return CompositeValidator(
            RequiredFields("market_code", "area_code", "delivery_date", "time_block", "price_inr_per_mwh"),
            NumericRange("time_block", minimum=1, maximum=96),
            NumericRange("price_inr_per_mwh", minimum=0),
            NumericRange("price_inr_per_mwh", maximum=10000, severity=Severity.WARNING),
        )


def load_fixture() -> bytes:
    return FIXTURE_PATH.read_bytes()


def modified_fixture(**changes: Any) -> bytes:
    """Return the fixture with top-level keys replaced (prices as given)."""
    data = json.loads(load_fixture(), parse_float=Decimal)
    data.update(changes)
    # json cannot emit Decimal as a number: write a marker, then unquote it.
    text = json.dumps(data, default=lambda value: f"__DECIMAL__{value}__")
    return re.sub(r'"__DECIMAL__(.*?)__"', r"\1", text).encode("utf-8")
