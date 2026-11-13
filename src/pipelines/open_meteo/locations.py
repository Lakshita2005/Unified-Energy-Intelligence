"""Weather location configuration and registration in ``pi.weather_location``.

Locations are supplied in a TOML file (``WEATHER_LOCATIONS_FILE``, default
``config/weather_locations.toml``); see ``config/weather_locations.example.toml``.
Every location must state where its coordinates were verified
(``coordinate_source``). No coordinates are built into the code.

``pi.weather_location`` is the registry used by the pipeline (its ids are
referenced by ``pi.open_meteo_weather``). Configured locations are registered
idempotently on ``location_code``. An existing location is never changed: if
its coordinates differ from the configuration, registration is refused, since
silently moving a location would mix weather from different places.

Codes starting with ``TEST_ONLY_`` mark technical test locations: they may not
reference a plant, and they are only ingested when requested explicitly by
code. Their data must not be used for forecasting or analysis.
"""

from __future__ import annotations

import re
import tomllib
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

from src.common.exceptions import ConfigurationError
from src.common.logging_config import get_logger

logger = get_logger(__name__)

# Also used as the raw-storage folder name (lower-cased).
_CODE = re.compile(r"^[A-Z0-9][A-Z0-9_-]{0,39}$")
_COORDINATE_SCALE = 7  # numeric(10,7) in pi.weather_location
TEST_LOCATION_PREFIX = "TEST_ONLY_"


class LocationError(ConfigurationError):
    """A location is missing, invalid or inconsistent with the database."""


@dataclass(frozen=True)
class LocationConfig:
    """One location from the configuration file."""

    code: str
    name: str
    latitude: Decimal
    longitude: Decimal
    coordinate_source: str
    elevation_m: Decimal | None = None
    plant_code: str | None = None
    state_code: str | None = None
    region_code: str | None = None


@dataclass(frozen=True)
class WeatherLocation:
    """A location registered in ``pi.weather_location``."""

    weather_location_id: int
    code: str
    name: str | None
    latitude: Decimal
    longitude: Decimal


def is_test_location(code: str) -> bool:
    """True for technical test locations, which are never real sites."""
    return code.startswith(TEST_LOCATION_PREFIX)


def _decimal(entry: dict[str, Any], field: str, minimum: int, maximum: int, where: str) -> Decimal:
    value = entry.get(field)
    if isinstance(value, bool) or not isinstance(value, (int, Decimal)):
        raise LocationError(f"{where}: '{field}' must be a number, got {value!r}")
    number = Decimal(value)
    if not minimum <= number <= maximum:
        raise LocationError(f"{where}: '{field}'={number} is outside [{minimum}, {maximum}]")
    exponent = number.normalize().as_tuple().exponent
    if isinstance(exponent, int) and -exponent > _COORDINATE_SCALE:
        raise LocationError(f"{where}: '{field}'={number} has more than {_COORDINATE_SCALE} decimal places")
    return number


def _optional_text(entry: dict[str, Any], field: str, where: str) -> str | None:
    value = entry.get(field)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise LocationError(f"{where}: '{field}' must be a non-empty string")
    return value.strip()


def parse_location_config(data: dict[str, Any], *, origin: str = "configuration") -> list[LocationConfig]:
    """Validate parsed TOML (``[[location]]`` tables) and return the locations."""
    entries = data.get("location", [])
    if not isinstance(entries, list):
        raise LocationError(f"{origin}: 'location' must be an array of tables ([[location]])")
    locations: list[LocationConfig] = []
    seen: set[str] = set()
    for index, entry in enumerate(entries, 1):
        where = f"{origin}, location #{index}"
        code = entry.get("code")
        if not isinstance(code, str) or not _CODE.fullmatch(code):
            raise LocationError(f"{where}: 'code' must match {_CODE.pattern} (upper case), got {code!r}")
        where = f"{origin}, location {code}"
        if code in seen:
            raise LocationError(f"{where}: duplicate code")
        seen.add(code)
        name = _optional_text(entry, "name", where)
        source = _optional_text(entry, "coordinate_source", where)
        if name is None or source is None:
            raise LocationError(f"{where}: 'name' and 'coordinate_source' are required")
        if is_test_location(code) and entry.get("plant_code") is not None:
            raise LocationError(f"{where}: test locations ({TEST_LOCATION_PREFIX}*) must not reference a plant")
        elevation = entry.get("elevation_m")
        locations.append(LocationConfig(
            code=code,
            name=name,
            latitude=_decimal(entry, "latitude", -90, 90, where),
            longitude=_decimal(entry, "longitude", -180, 180, where),
            coordinate_source=source,
            elevation_m=None if elevation is None else _decimal(entry, "elevation_m", -500, 9000, where),
            plant_code=_optional_text(entry, "plant_code", where),
            state_code=_optional_text(entry, "state_code", where),
            region_code=_optional_text(entry, "region_code", where),
        ))
    return locations


def load_location_config(path: Path) -> list[LocationConfig]:
    """Load locations from a TOML file. A missing file means no configured locations."""
    if not path.exists():
        logger.info("Weather location file %s not found; no locations configured", path)
        return []
    try:
        with path.open("rb") as handle:
            data = tomllib.load(handle, parse_float=Decimal)
    except tomllib.TOMLDecodeError as exc:
        raise LocationError(f"{path}: invalid TOML: {exc}") from exc
    return parse_location_config(data, origin=str(path))


def _same_coordinate(a: Decimal, b: Decimal) -> bool:
    quantum = Decimal(1).scaleb(-_COORDINATE_SCALE)
    return Decimal(a).quantize(quantum) == Decimal(b).quantize(quantum)


class WeatherLocationRepository:
    """Reads and registers rows in ``pi.weather_location``."""

    def __init__(self, schema: str) -> None:
        self.schema = schema

    def list_locations(self, connection: Connection) -> dict[str, WeatherLocation]:
        """Return registered locations that have a location_code, keyed by code."""
        rows = connection.execute(text(
            f"SELECT weather_location_id, location_code, location_name, latitude, longitude "
            f"FROM {self.schema}.weather_location WHERE location_code IS NOT NULL ORDER BY location_code"))
        return {row.location_code: WeatherLocation(row.weather_location_id, row.location_code, row.location_name,
                                                   row.latitude, row.longitude) for row in rows}

    def ensure(self, connection: Connection, config: LocationConfig) -> tuple[WeatherLocation, bool]:
        """Register ``config`` unless already present. Returns ``(location, created)``.

        Raises:
            LocationError: If the code exists with different coordinates, or the
                referenced plant does not exist.
        """
        existing = self.list_locations(connection).get(config.code)
        if existing is not None:
            self._check_matches(existing, config)
            return existing, False

        plant_id = None
        if config.plant_code is not None:
            plant_id = connection.execute(
                text(f"SELECT plant_id FROM {self.schema}.plant WHERE plant_code = :code"),
                {"code": config.plant_code}).scalar_one_or_none()
            if plant_id is None:
                raise LocationError(f"Location {config.code}: plant_code {config.plant_code!r} not found in "
                                    f"{self.schema}.plant")

        location_id = connection.execute(text(
            f"INSERT INTO {self.schema}.weather_location (plant_id, location_code, location_name, state_code, "
            "region_code, latitude, longitude, elevation_m) VALUES (:plant_id, :code, :name, :state, :region, "
            ":lat, :lon, :elev) ON CONFLICT (location_code) DO NOTHING RETURNING weather_location_id"),
            {"plant_id": plant_id, "code": config.code, "name": config.name, "state": config.state_code,
             "region": config.region_code, "lat": config.latitude, "lon": config.longitude,
             "elev": config.elevation_m}).scalar_one_or_none()
        if location_id is None:  # registered concurrently
            existing = self.list_locations(connection)[config.code]
            self._check_matches(existing, config)
            return existing, False
        logger.info("Registered weather location %s (weather_location_id=%d, source: %s)",
                    config.code, location_id, config.coordinate_source)
        return WeatherLocation(int(location_id), config.code, config.name, config.latitude, config.longitude), True

    @staticmethod
    def _check_matches(existing: WeatherLocation, config: LocationConfig) -> None:
        if not (_same_coordinate(existing.latitude, config.latitude)
                and _same_coordinate(existing.longitude, config.longitude)):
            raise LocationError(
                f"Location {config.code} is registered at ({existing.latitude}, {existing.longitude}) but configured "
                f"at ({config.latitude}, {config.longitude}). Registered locations are never moved; fix the "
                "configuration or use a new code."
            )
