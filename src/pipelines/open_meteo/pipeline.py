"""Open-Meteo forecast and historical ingestion pipelines.

Both pipelines reuse :class:`~src.ingestion.base.BaseIngestionPipeline`; they
only add the Open-Meteo request (``fetch``), response parsing (``parse``) and
validation rules. One :meth:`run` covers one location; see
:mod:`src.pipelines.open_meteo.runner` for multi-location execution.

Duplicate handling uses the natural key ``(weather_location_id, valid_time,
is_forecast)`` with :attr:`ConflictPolicy.VERSION`: an hour whose values are
unchanged is skipped, while an hour whose values changed gets a new row tied
to the new run. Earlier forecast vintages are therefore preserved, and the
forecast as known at time T is, per ``valid_time``, the latest row with
``retrieved_at <= T``.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any, ClassVar

from src.ingestion.base import BaseIngestionPipeline, RunContext
from src.ingestion.catalog import DATASETS, SOURCES
from src.ingestion.models import DatasetDefinition, ParsedRecord, ParseResult, RawDocument, ValidationIssue
from src.ingestion.validation import CompositeValidator, FunctionValidator, NumericRange, RequiredFields, Validator
from src.loaders.table_loader import ConflictPolicy
from src.pipelines.open_meteo import api
from src.pipelines.open_meteo.locations import WeatherLocation
from src.pipelines.open_meteo.parser import parse_hourly_response


def _catalog_dataset(code: str) -> DatasetDefinition:
    return next(entry.definition for entry in DATASETS if entry.definition.code == code)


def _all_measurements_null(record: ParsedRecord) -> list[ValidationIssue]:
    if all(record.values.get(v.column) is None for v in api.HOURLY_VARIABLES):
        return [ValidationIssue("ALL_MEASUREMENTS_NULL",
                                "Every requested variable is null for this hour (data not yet available?)")]
    return []


def weather_validator() -> Validator:
    """Required keys, plausible physical ranges, and at least one measurement per hour."""
    return CompositeValidator(
        RequiredFields("weather_location_id", "valid_time"),
        *(NumericRange(v.column, minimum=v.minimum, maximum=v.maximum) for v in api.HOURLY_VARIABLES),
        FunctionValidator(_all_measurements_null),
    )


class _OpenMeteoPipeline(BaseIngestionPipeline):
    """Behaviour shared by the forecast and historical pipelines."""

    source = SOURCES["OPEN_METEO"]
    target_table = "open_meteo_weather"
    natural_key = ("weather_location_id", "valid_time", "is_forecast")
    conflict_policy = ConflictPolicy.VERSION
    pipeline_version = "open-meteo-1.0.0"
    is_forecast: ClassVar[bool]
    base_url: ClassVar[str]

    @staticmethod
    def location_parameters(location: WeatherLocation) -> dict[str, Any]:
        """Run parameters identifying the location (stored in ``ingestion_run.parameters``)."""
        return {
            "location_code": location.code,
            "weather_location_id": location.weather_location_id,
            "latitude": str(location.latitude),
            "longitude": str(location.longitude),
        }

    def _requests(self, parameters: dict[str, Any]) -> list[dict[str, Any]]:
        raise NotImplementedError

    def raw_storage_partition(self, document: RawDocument, context: RunContext) -> str | None:
        return str(context.parameters["location_code"]).lower()

    def fetch(self, context: RunContext) -> list[RawDocument]:
        parameters = dict(context.parameters)
        api_key = self.settings.open_meteo_api_key.get_secret_value() if self.settings.open_meteo_api_key else None
        url = api.endpoint(self.base_url, api_key)
        documents = []
        for params in self._requests(parameters):
            window = {k: params[k] for k in ("forecast_days", "start_date", "end_date") if k in params}
            context.log.info("Requesting %s for %s %s", url, parameters["location_code"], window)
            response = api.call_api(self.http, url, params, api_key)
            label = "_".join(str(value) for value in window.values())
            documents.append(RawDocument(
                content=response.content,
                retrieved_at=response.retrieved_at,
                extension="json",
                content_type=response.content_type,
                published_at=None,  # the API does not state when the data was issued
                source_url=response.public_url,
                document_name=f"{self.dataset.code.lower()}_{parameters['location_code'].lower()}_{label}.json",
            ))
        return documents

    def parse(self, document: RawDocument, context: RunContext) -> ParseResult:
        return parse_hourly_response(
            document.content,
            weather_location_id=int(context.parameters["weather_location_id"]),
            location_code=str(context.parameters["location_code"]),
            is_forecast=self.is_forecast,
        )

    def validator(self) -> Validator:
        return weather_validator()


class OpenMeteoForecastPipeline(_OpenMeteoPipeline):
    """Hourly forecast from today (IST) for ``forecast_days`` days."""

    dataset = _catalog_dataset("OPEN_METEO_FORECAST")
    is_forecast = True
    base_url = api.FORECAST_URL

    def _requests(self, parameters: dict[str, Any]) -> list[dict[str, Any]]:
        return [api.forecast_params(Decimal(parameters["latitude"]), Decimal(parameters["longitude"]),
                                    int(parameters["forecast_days"]))]


class OpenMeteoHistoricalPipeline(_OpenMeteoPipeline):
    """Hourly reanalysis for ``start_date``..``end_date`` (IST days), in chunks of up to 31 days."""

    dataset = _catalog_dataset("OPEN_METEO_HISTORICAL")
    is_forecast = False
    base_url = api.ARCHIVE_URL

    def _requests(self, parameters: dict[str, Any]) -> list[dict[str, Any]]:
        latitude, longitude = Decimal(parameters["latitude"]), Decimal(parameters["longitude"])
        start, end = date.fromisoformat(parameters["start_date"]), date.fromisoformat(parameters["end_date"])
        return [api.historical_params(latitude, longitude, chunk_start, chunk_end)
                for chunk_start, chunk_end in api.split_date_range(start, end)]
