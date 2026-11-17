"""MOSDAC pipelines: one run = one endpoint (solar or wind) for one location.

| Pipeline | Endpoint | Table | Natural key (VERSION) |
|---|---|---|---|
| :class:`MosdacSolarPipeline` | /apienergy/solar | mosdac_solar_forecast | location_code, series_start_at, valid_at |
| :class:`MosdacWindPipeline` | /apienergy/wind | mosdac_wind_forecast | location_code, series_start_at, valid_at |

Raw storage (two write-once files per response, each with a ``.meta.json`` sidecar and SHA-256):

* ``mosdac/<dataset>/<location>-response/YYYY/MM/DD/<ts>Z.json``: the response body exactly as received;
* ``mosdac/<dataset>/<location>/YYYY/MM/DD/<ts>Z.json``: the **request-bound document** registered in
  ``source_document``: ``{document_type, request {endpoint, url, location_code, latitude, longitude},
  response_sha256, response_text | response_base64}`` (deterministic, sorted keys).

The response itself does not name its location, so two plants can receive byte-identical bodies;
registering the bare body would make the second plant a "duplicate document" and lose its rows.
Binding the request makes the checksum unique per location and content, while an identical re-fetch
for the same location is still recognised (``DUPLICATE_DOCUMENT``). Parsing reads the response from
the request-bound document and verifies its SHA-256. The document is parsed, validated and loaded in
one transaction with its data-quality issues.

Vintages: no issue time is published, so ``series_start_at`` (earliest valid time in the response)
identifies a forecast series. A response for a new series inserts new keys; a re-fetch of the same
series with changed values inserts new versions of only the changed rows (earlier rows are kept);
byte-identical content is recognised by checksum (``DUPLICATE_DOCUMENT``) and not reloaded.
"""

from __future__ import annotations

import base64
import json
from decimal import Decimal
from typing import Any, ClassVar

from src.common.checksums import sha256_bytes
from src.common.raw_storage import StoredObject
from src.ingestion.base import BaseIngestionPipeline, RunContext
from src.ingestion.catalog import MOSDAC_DATASETS, MOSDAC_SOURCE
from src.ingestion.models import (
    DataQualityIssue,
    DatasetDefinition,
    ParseResult,
    RawDocument,
    Severity,
)
from src.ingestion.validation import RequiredFields, Validator
from src.loaders.table_loader import ConflictPolicy
from src.pipelines.mosdac import api
from src.pipelines.mosdac.errors import MosdacResponseError
from src.pipelines.mosdac.parse import SOLAR, WIND, Location, MosdacParseResult, SeriesSpec, parse_series

PIPELINE_VERSION = "mosdac-1.0.0"
DOCUMENT_TYPE = "mosdac_request_response"


def request_bound_document(kind: str, location: Location, url: str, content: bytes) -> bytes:
    """Deterministic document binding the request (endpoint, location, coordinates) to the exact response."""
    document: dict[str, Any] = {
        "document_type": DOCUMENT_TYPE,
        "request": {"endpoint": kind, "url": url, "location_code": location.code,
                    "latitude": str(location.latitude), "longitude": str(location.longitude)},
        "response_sha256": sha256_bytes(content),
    }
    try:
        document["response_text"] = content.decode("utf-8")
    except UnicodeDecodeError:
        document["response_base64"] = base64.b64encode(content).decode("ascii")
    return json.dumps(document, ensure_ascii=False, sort_keys=True).encode("utf-8")


def response_bytes(document_content: bytes) -> tuple[dict[str, Any], bytes]:
    """The request and the exact response bytes of a request-bound document (SHA-256 verified)."""
    try:
        bound = json.loads(document_content)
    except ValueError:
        raise MosdacResponseError("raw document is not a MOSDAC request-bound document") from None
    if not isinstance(bound, dict) or bound.get("document_type") != DOCUMENT_TYPE:
        raise MosdacResponseError("raw document is not a MOSDAC request-bound document")
    if "response_text" in bound:
        content = str(bound["response_text"]).encode("utf-8")
    else:
        content = base64.b64decode(bound["response_base64"])
    if sha256_bytes(content) != bound.get("response_sha256"):
        raise MosdacResponseError("embedded response does not match its recorded SHA-256")
    return bound["request"], content


def _catalog_dataset(code: str) -> DatasetDefinition:
    return next(entry.definition for entry in MOSDAC_DATASETS if entry.definition.code == code)


class _MosdacPipeline(BaseIngestionPipeline):
    source = MOSDAC_SOURCE
    conflict_policy = ConflictPolicy.VERSION
    pipeline_version = PIPELINE_VERSION
    natural_key = ("location_code", "series_start_at", "valid_at")
    # Retries are HttpClient's (HTTP_MAX_RETRIES with backoff); the whole fetch is not repeated on top.
    fetch_attempts = 1
    # The published time text and the request coordinates describe a row's origin, not its values.
    extra_lineage_columns = frozenset({"source_time_text", "latitude", "longitude", "availability_basis"})
    kind: ClassVar[api.Kind]
    spec: ClassVar[SeriesSpec]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._parsed: dict[int, MosdacParseResult] = {}
        self._responses: dict[int, api.MosdacResponse] = {}

    @staticmethod
    def location_parameters(code: str, latitude: Decimal, longitude: Decimal) -> dict[str, Any]:
        """Run parameters identifying the location (stored in ``ingestion_run.parameters``)."""
        return {"location_code": code, "latitude": str(latitude), "longitude": str(longitude)}

    def _location(self, context: RunContext) -> Location:
        p = context.parameters
        return Location(str(p["location_code"]), Decimal(str(p["latitude"])), Decimal(str(p["longitude"])))

    def raw_storage_partition(self, document: RawDocument, context: RunContext) -> str | None:
        return str(context.parameters["location_code"]).lower()

    def fetch(self, context: RunContext) -> list[RawDocument]:
        location = self._location(context)
        client = api.MosdacClient(self.http, self.settings)
        context.log.info("Requesting MOSDAC %s for %s", self.kind, location.code)
        response = client.get(self.kind, str(context.parameters["latitude"]), str(context.parameters["longitude"]))
        document = RawDocument(content=request_bound_document(self.kind, location, response.url, response.content),
                               retrieved_at=response.retrieved_at, extension="json",
                               content_type=response.content_type, published_at=None,  # no issue time is published
                               source_url=response.url,
                               document_name=f"mosdac_{self.kind}_{location.code.lower()}.json")
        self._responses[id(document)] = response
        return [document]

    def _store_raw(self, document: RawDocument, context: RunContext) -> StoredObject:
        """Store the original response bytes, then the request-bound document (which is registered)."""
        code = str(context.parameters["location_code"]).lower()
        response = self._responses.pop(id(document), None)
        common = {"ingestion_run_id": context.ingestion_run_id, "source_code": self.source.code,
                  "dataset_code": self.dataset.code, "source_url": document.source_url,
                  "retrieved_at": document.retrieved_at.isoformat(), "published_at": None,
                  "location_code": context.parameters["location_code"],
                  "latitude": str(context.parameters["latitude"]), "longitude": str(context.parameters["longitude"])}
        extra: dict[str, Any] = {}
        if response is not None:
            original = self.raw_storage.save_bytes(
                response.content, source=self.storage_source, dataset=self.storage_dataset, extension="json",
                ingested_at=document.retrieved_at, partition=f"{code}-response",
                metadata={**common, "http_status": response.status_code, "content_type": response.content_type,
                          "kind": "original MOSDAC response body, unchanged"})
            context.log.info("Stored MOSDAC response %s (%d bytes, sha256=%s)", original.key, original.size_bytes,
                             original.sha256)
            extra = {"response_raw_key": original.key, "response_sha256": original.sha256}
        stored = self.raw_storage.save_bytes(
            document.content, source=self.storage_source, dataset=self.storage_dataset, extension="json",
            ingested_at=document.retrieved_at, partition=self.raw_storage_partition(document, context),
            metadata={**common, "document_name": document.document_name, "content_type": "application/json",
                      "kind": "request-bound document (registered in source_document)", **extra})
        context.log.info("Stored request-bound document %s (sha256=%s)", stored.key, stored.sha256)
        return stored

    def parse(self, document: RawDocument, context: RunContext) -> ParseResult:
        request, content = response_bytes(document.content)
        if request.get("location_code") != context.parameters["location_code"] or request.get("endpoint") != self.kind:
            raise MosdacResponseError(f"raw document is for {request.get('endpoint')} {request.get('location_code')}, "
                                      f"not {self.kind} {context.parameters['location_code']}")
        payload = api.decode(content, self.kind)
        result = parse_series(payload, self.spec, self._location(context), document.retrieved_at)
        self._parsed[id(document)] = result
        if result.series_start_at is not None:
            context.log.info("MOSDAC %s %s: %d record(s), series %s, %d parse failure(s)", self.kind,
                             context.parameters["location_code"], len(result.records),
                             result.series_start_at.isoformat(), len(result.failures))
        return ParseResult(result.records, result.failures)

    def validator(self) -> Validator:
        return RequiredFields("location_code", "series_start_at", "valid_at", "retrieved_at", "source_available_at",
                              "availability_basis")

    def document_issues(self, document: RawDocument, parsed: ParseResult, context: RunContext) -> list[DataQualityIssue]:
        result = self._parsed.pop(id(document), None)
        if result is None:
            return []
        return ([DataQualityIssue(self.target_table, c, d, Severity.WARNING, k) for c, d, k in result.warnings]
                + [DataQualityIssue(self.target_table, c, d, Severity.INFO, k) for c, d, k in result.info])


class MosdacSolarPipeline(_MosdacPipeline):
    """``/apienergy/solar`` -> ``mosdac_solar_forecast`` (air temperature, solar flux)."""

    dataset = _catalog_dataset("MOSDAC_SOLAR_FORECAST")
    target_table = "mosdac_solar_forecast"
    kind = "solar"
    spec = SOLAR


class MosdacWindPipeline(_MosdacPipeline):
    """``/apienergy/wind`` -> ``mosdac_wind_forecast`` (speed and direction; units unverified)."""

    dataset = _catalog_dataset("MOSDAC_WIND_FORECAST")
    target_table = "mosdac_wind_forecast"
    kind = "wind"
    spec = WIND
