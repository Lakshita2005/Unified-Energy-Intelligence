"""IMD pipelines: AWS stations, AWS observations (dataset IMD_AWS_OBS) and city forecasts (IMD_CITY_FORECAST).

| Pipeline | Endpoint | Table | Natural key (VERSION) |
|---|---|---|---|
| :class:`ImdStationPipeline` | /aws_data_mapping | imd_station | network, imd_id, call_sign |
| :class:`ImdObservationPipeline` | /aws_data | imd_station_observation | network, imd_id, call_sign, observation_date, observation_time |
| :class:`ImdCityForecastPipeline` | /cityforecast | imd_city_forecast | station_code, issue_date, forecast_date |

Each response is one raw document (write-once file, SHA-256, ``source_document``), parsed,
validated and loaded in one transaction with the run's data-quality issues.

* Observations: the published station context (name, district, state, coordinates, the matched
  ``imd_station_id`` and the derived ``observed_at``) is lineage, so only a change in a measured
  value creates a new version (a revised observation). ``imd_station_id`` is the latest station row
  (version) with exactly the same ``(imd_id, call_sign)``; if there is none it stays NULL, the
  observation is still loaded and the station is reported (``UNKNOWN_STATION``).
* City forecasts: every issue date is a separate vintage (its own natural key). A re-fetch of the
  same issue with changed values becomes a new version; nothing is overwritten.
"""

from __future__ import annotations

from typing import Any, ClassVar

from sqlalchemy import text

from src.common.raw_storage import StoredObject
from src.ingestion.base import BaseIngestionPipeline, RunContext
from src.ingestion.catalog import IMD_DATASETS, SOURCES
from src.ingestion.models import (
    DataQualityIssue,
    DatasetDefinition,
    ParsedRecord,
    ParseResult,
    RawDocument,
    Severity,
)
from src.ingestion.validation import RequiredFields, Validator
from src.loaders.table_loader import ConflictPolicy, LoadResult
from src.pipelines.imd import api
from src.pipelines.imd.parse import (
    ImdParseResult,
    parse_aws_observations,
    parse_aws_station_mapping,
    parse_city_forecast,
)
from src.pipelines.imd.reference import AWS_NETWORK

PIPELINE_VERSION = "imd-1.0.0"


def _catalog_dataset(code: str) -> DatasetDefinition:
    return next(entry.definition for entry in IMD_DATASETS if entry.definition.code == code)


class _ImdPipeline(BaseIngestionPipeline):
    source = SOURCES["IMD"]
    conflict_policy = ConflictPolicy.VERSION
    pipeline_version = PIPELINE_VERSION
    path: ClassVar[str]
    required: ClassVar[tuple[str, ...]]
    label: ClassVar[str]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._parsed: dict[int, ImdParseResult] = {}

    def request_params(self, context: RunContext) -> dict[str, Any]:
        return {}

    def raw_storage_partition(self, document: RawDocument, context: RunContext) -> str | None:
        return self.label

    def fetch(self, context: RunContext) -> list[RawDocument]:
        params = self.request_params(context)
        response = api.ImdClient(self.http, self.settings).get(self.path, params)
        suffix = "_".join(f"{k}-{v}" for k, v in params.items())
        return [RawDocument(content=response.content, retrieved_at=response.retrieved_at, extension="json",
                            content_type=response.content_type, published_at=None,   # IMD states no publication time
                            source_url=response.url,
                            document_name=f"imd_{self.label}{'_' + suffix if suffix else ''}.json")]

    def _parse_records(self, records: list[dict[str, Any]], document: RawDocument) -> ImdParseResult:
        raise NotImplementedError

    def parse(self, document: RawDocument, context: RunContext) -> ParseResult:
        records = api.decode_records(document.content, self.label)
        result = self._parse_records(records, document)
        if not records:
            result.info.append(("NO_DATA", f"{self.label}: the response contains no records", self.label))
        self._parsed[id(document)] = result
        return ParseResult(result.records, result.failures)

    def validator(self) -> Validator:
        return RequiredFields(*self.required)

    def document_issues(self, document: RawDocument, parsed: ParseResult, context: RunContext) -> list[DataQualityIssue]:
        result = self._parsed.pop(id(document), None)
        if result is None:
            return []
        return ([DataQualityIssue(self.target_table, c, d, Severity.WARNING, k) for c, d, k in result.warnings]
                + [DataQualityIssue(self.target_table, c, d, Severity.INFO, k) for c, d, k in result.info])


class ImdStationPipeline(_ImdPipeline):
    """``/aws_data_mapping`` -> ``imd_station`` (station attributes, versioned)."""

    dataset = _catalog_dataset("IMD_AWS_OBS")
    target_table = "imd_station"
    natural_key = ("network", "imd_id", "call_sign")
    path = api.AWS_MAPPING_PATH
    required = ("network", "imd_id", "call_sign", "retrieved_at", "source_available_at", "availability_basis")
    label = "aws_station"
    extra_lineage_columns = frozenset({"availability_basis"})

    def _parse_records(self, records: list[dict[str, Any]], document: RawDocument) -> ImdParseResult:
        return parse_aws_station_mapping(records, document.retrieved_at)


class ImdObservationPipeline(_ImdPipeline):
    """``/aws_data`` -> ``imd_station_observation``."""

    dataset = _catalog_dataset("IMD_AWS_OBS")
    target_table = "imd_station_observation"
    natural_key = ("network", "imd_id", "call_sign", "observation_date", "observation_time")
    path = api.AWS_DATA_PATH
    required = ("network", "imd_id", "call_sign", "observation_date", "observation_time", "retrieved_at",
                "source_available_at", "availability_basis")
    label = "aws_obs"
    measurement_columns = ("curr_temp_value", "dew_point_temp_value", "rh_value", "wind_direction_value",
                           "wind_speed_value", "mslp_value", "min_temp_value", "max_temp_value", "feels_like_value",
                           "weather_code", "nebulosity_value")
    # Context and derived fields: they never make an observation "changed" (only measurements do).
    extra_lineage_columns = frozenset({"imd_station_id", "observed_at", "station_name", "district_name", "state_name",
                                       "state_code", "latitude", "longitude", "availability_basis"})

    def request_params(self, context: RunContext) -> dict[str, Any]:
        params = {}
        if context.parameters.get("station"):
            params["id"] = context.parameters["station"]
        if context.parameters.get("state_id") is not None:
            params["sid"] = context.parameters["state_id"]
        return params

    def _parse_records(self, records: list[dict[str, Any]], document: RawDocument) -> ImdParseResult:
        return parse_aws_observations(records, document.retrieved_at, self.settings.imd_aws_time_zone)

    def _load_document(self, connection: Any, document: RawDocument, context: RunContext, document_id: int,
                       accepted: list, issues: list[DataQualityIssue], rejected_records: list, stored: StoredObject,
                       result: Any) -> LoadResult:
        """Entity mapping: link each observation to its station row by exact (imd_id, call_sign)."""
        keys = {(r.values["imd_id"], r.values["call_sign"]) for r, _ in accepted}
        matches: dict[tuple[str, str], set[int]] = {}
        if keys:
            rows = connection.execute(text(
                f"SELECT DISTINCT ON (imd_id, call_sign) imd_id, call_sign, imd_station_id "
                f"FROM {self.settings.db_schema}.imd_station WHERE network = :n "
                "AND (imd_id, call_sign) IN (SELECT * FROM unnest(CAST(:ids AS text[]), CAST(:signs AS text[]))) "
                "ORDER BY imd_id, call_sign, imd_station_id DESC"),
                {"n": AWS_NETWORK, "ids": [k[0] for k in keys], "signs": [k[1] for k in keys]})
            for imd_id, call_sign, station_id in rows:
                matches.setdefault((imd_id, call_sign), set()).add(station_id)
        mapped = []
        for record, record_issues in accepted:
            key = (record.values["imd_id"], record.values["call_sign"])
            found = matches.get(key, set())
            station_id = next(iter(found)) if len(found) == 1 else None
            mapped.append((ParsedRecord(record.key, {**record.values, "imd_station_id": station_id}, record.raw),
                           record_issues))
        for key in sorted(keys - set(matches)):
            issues.append(DataQualityIssue(self.target_table, "UNKNOWN_STATION",
                                           f"Station ID {key[0]} / CALL_SIGN {key[1]} is not in imd_station; "
                                           "observations loaded with imd_station_id NULL", Severity.WARNING,
                                           f"{key[0]}|{key[1]}"))
        return super()._load_document(connection, document, context, document_id, mapped, issues, rejected_records,
                                      stored, result)


class ImdCityForecastPipeline(_ImdPipeline):
    """``/cityforecast`` -> ``imd_city_forecast`` (one row per station, issue date and lead day)."""

    dataset = _catalog_dataset("IMD_CITY_FORECAST")
    target_table = "imd_city_forecast"
    natural_key = ("station_code", "issue_date", "forecast_date")
    path = api.CITY_FORECAST_PATH
    required = ("station_code", "issue_date", "forecast_date", "lead_day", "retrieved_at", "source_available_at",
                "availability_basis")
    label = "city_forecast"
    extra_lineage_columns = frozenset({"station_name", "availability_basis"})

    def request_params(self, context: RunContext) -> dict[str, Any]:
        return {"id": context.parameters["station"]} if context.parameters.get("station") else {}

    def _parse_records(self, records: list[dict[str, Any]], document: RawDocument) -> ImdParseResult:
        return parse_city_forecast(records, document.retrieved_at)
