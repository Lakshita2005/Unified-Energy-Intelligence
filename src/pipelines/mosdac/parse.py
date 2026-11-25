"""Parse MOSDAC solar and wind energy forecast responses.

Observed structure (live 2026-11-17, solar): a JSON **list of objects**, one per time step::

    [{"time": "13-11-26 0530", "temp": "16.91", "flux": "0.0"}, ...]

The task brief also describes the fields as parallel arrays, so an object of equal-length arrays
(``{"time": [...], "temp": [...], "flux": [...]}``) is accepted too; arrays of unequal length reject
the whole response. Anything else (an error object, a scalar, a list of non-objects, records
without the expected keys) raises :class:`MosdacResponseError` - never guessed.

Field meanings (supplied endpoint description; nothing beyond it is inferred):

* ``time``: ``DD-MM-YY HHMM`` in IST -> ``valid_at`` (the published text is kept as ``source_time_text``).
* solar ``temp``: air temperature, degC -> ``air_temperature_c``; ``flux``: solar flux, W/m2 ->
  ``solar_flux_w_m2``. Flux is NOT labelled GHI and no DNI/DHI/GTI is derived.
* wind ``speed`` / ``direction``: units NOT verified -> ``wind_speed_value`` / ``wind_direction_value``.

Vintage: MOSDAC publishes no issue time. ``series_start_at`` (the earliest valid time in the
response) identifies the forecast series; with ``valid_at`` and the location it is the natural key.

Record rules: an unparseable ``time`` or a non-numeric value rejects the record (``PARSE_ERROR``);
JSON null / empty string is stored NULL and reported (``MISSING_VALUE``); a missing measurement key
is stored NULL and reported (``MISSING_FIELD``). Series rules (reported, never rejecting a valid
response): duplicate timestamps, missing 15-minute steps, irregular spacing, out-of-order times,
point count other than about 289, valid times before retrieval, implausible values.
"""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any
from zoneinfo import ZoneInfo

from src.ingestion.models import ParseFailure, ParsedRecord
from src.pipelines.mosdac.errors import MosdacResponseError

IST = ZoneInfo("Asia/Kolkata")
STEP = timedelta(minutes=15)
EXPECTED_POINTS = 289            # about three days at 15 minutes; informational only
TIME_KEY = "time"
_TIME_PATTERN = re.compile(r"^\d{2}-\d{2}-\d{2} \d{4}$")
_SAMPLE = 5                      # sample keys quoted in aggregated issues


@dataclass(frozen=True)
class SeriesSpec:
    kind: str
    measurements: dict[str, str]                                   # published key -> column
    plausible: dict[str, tuple[Decimal | None, Decimal | None]]    # column -> (min, max); WARNING only


SOLAR = SeriesSpec(
    "solar",
    {"temp": "air_temperature_c", "flux": "solar_flux_w_m2"},
    {"air_temperature_c": (Decimal(-40), Decimal(60)), "solar_flux_w_m2": (Decimal(0), None)},
)
WIND = SeriesSpec(
    "wind",
    {"speed": "wind_speed_value", "direction": "wind_direction_value"},
    # Units unverified: only unit-independent checks (negative speed; direction outside 0-360 if degrees).
    {"wind_speed_value": (Decimal(0), None), "wind_direction_value": (Decimal(0), Decimal(360))},
)
SPECS = {"solar": SOLAR, "wind": WIND}


@dataclass(frozen=True)
class Location:
    code: str
    latitude: Decimal
    longitude: Decimal


@dataclass
class MosdacParseResult:
    records: list[ParsedRecord] = field(default_factory=list)
    failures: list[ParseFailure] = field(default_factory=list)
    warnings: list[tuple[str, str, str | None]] = field(default_factory=list)   # (code, description, key)
    info: list[tuple[str, str, str | None]] = field(default_factory=list)
    series_start_at: datetime | None = None


def to_decimal(value: Any, label: str) -> Decimal | None:
    """Exact number, or None for JSON null / empty string. Raises ValueError for anything else."""
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"{label} is a boolean ({value!r})")
    if isinstance(value, (int, Decimal)):
        number = Decimal(value)
    elif isinstance(value, float):
        number = Decimal(repr(value))
    elif isinstance(value, str):
        text = value.strip()
        if text == "":
            return None
        try:
            number = Decimal(text)
        except InvalidOperation:
            raise ValueError(f"{label} is not a number: {text[:30]!r}") from None
    else:
        raise ValueError(f"{label} is not a number: {type(value).__name__}")
    if not number.is_finite():
        raise ValueError(f"{label} is not a finite number: {value!r}")
    return number


def parse_time(value: Any) -> datetime:
    """``DD-MM-YY HHMM`` (IST) -> aware datetime. Raises ValueError otherwise."""
    if not isinstance(value, str) or not _TIME_PATTERN.fullmatch(value.strip()):
        raise ValueError(f"time {value!r} is not in the form DD-MM-YY HHMM")
    try:
        naive = datetime.strptime(value.strip(), "%d-%m-%y %H%M")
    except ValueError:
        raise ValueError(f"time {value!r} is not a valid date/time") from None
    return naive.replace(tzinfo=IST)


def _records_of(payload: Any, spec: SeriesSpec, result: MosdacParseResult) -> list[dict[str, Any]]:
    expected = (TIME_KEY, *spec.measurements)
    if isinstance(payload, list):
        if not all(isinstance(item, dict) for item in payload):
            kinds = sorted({type(item).__name__ for item in payload if not isinstance(item, dict)})
            raise MosdacResponseError(f"{spec.kind}: the list contains non-object items ({', '.join(kinds)})")
        return payload
    if isinstance(payload, dict) and isinstance(payload.get(TIME_KEY), list):
        arrays = {k: v for k, v in payload.items() if isinstance(v, list)}
        lengths = {k: len(v) for k, v in arrays.items()}
        if len(set(lengths.values())) > 1:
            detail = ", ".join(f"{k}={n}" for k, n in lengths.items())
            raise MosdacResponseError(f"{spec.kind}: arrays have unequal lengths ({detail}); response not loaded")
        missing = [k for k in expected if k not in arrays]
        if missing:
            raise MosdacResponseError(f"{spec.kind}: columnar response lacks arrays {missing}")
        result.info.append(("COLUMNAR_RESPONSE", f"{spec.kind}: response was an object of equal-length arrays "
                                                 "(not the list-of-objects structure observed live)", None))
        n = lengths[TIME_KEY]
        return [{k: arrays[k][i] for k in arrays} for i in range(n)]
    if isinstance(payload, dict):
        keys = sorted(map(str, payload))[:10]
        raise MosdacResponseError(f"{spec.kind}: expected a JSON list of records, got an object with keys {keys}")
    raise MosdacResponseError(f"{spec.kind}: expected a JSON list of records, got {type(payload).__name__}")


def _sample(keys: list[str]) -> str:
    return ", ".join(keys[:_SAMPLE]) + (f" (+{len(keys) - _SAMPLE} more)" if len(keys) > _SAMPLE else "")


def parse_series(payload: Any, spec: SeriesSpec, location: Location, retrieved_at: datetime) -> MosdacParseResult:
    """Parse one decoded response for one location. Raises MosdacResponseError for an unrecognised structure."""
    result = MosdacParseResult()
    items = _records_of(payload, spec, result)
    if not items:
        result.info.append(("NO_DATA", f"{spec.kind}: the response contains no records", location.code))
        return result

    seen_keys = Counter(k for item in items for k in item)
    if TIME_KEY not in seen_keys or not any(k in seen_keys for k in spec.measurements):
        raise MosdacResponseError(f"{spec.kind}: records lack the expected keys {[TIME_KEY, *spec.measurements]}; "
                                  f"keys seen: {sorted(seen_keys)[:10]} (response structure changed?)")
    extra = sorted(k for k in seen_keys if k != TIME_KEY and k not in spec.measurements)
    if extra:
        result.info.append(("UNDOCUMENTED_FIELDS", f"{spec.kind}: fields not in the endpoint description "
                                                   f"(kept in raw storage only): {extra}", location.code))

    missing_field: dict[str, list[str]] = {}
    missing_value: dict[str, list[str]] = {}
    out_of_range: dict[str, list[str]] = {}
    parsed_times: list[datetime] = []
    for index, item in enumerate(items):
        time_text = item.get(TIME_KEY)
        key = f"{location.code}|{time_text if isinstance(time_text, str) else f'row{index}'}"
        try:
            valid_at = parse_time(time_text)
            values: dict[str, Decimal | None] = {}
            for source_key, column in spec.measurements.items():
                if source_key not in item:
                    missing_field.setdefault(source_key, []).append(key)
                    values[column] = None
                    continue
                values[column] = to_decimal(item[source_key], source_key)
                if values[column] is None:
                    missing_value.setdefault(source_key, []).append(key)
        except ValueError as exc:
            result.failures.append(ParseFailure(key, item, f"{spec.kind}: {exc}"))
            continue
        for column, (low, high) in spec.plausible.items():
            value = values.get(column)
            if value is not None and ((low is not None and value < low) or (high is not None and value > high)):
                out_of_range.setdefault(column, []).append(f"{key}={value}")
        parsed_times.append(valid_at)
        result.records.append(ParsedRecord(key, {
            "location_code": location.code, "latitude": location.latitude, "longitude": location.longitude,
            "valid_at": valid_at, "source_time_text": time_text.strip(), **values,
            "retrieved_at": retrieved_at, "source_published_at": None, "source_available_at": retrieved_at,
            "availability_basis": "RETRIEVED_AT",
        }, item))

    for source_key, keys in missing_field.items():
        result.warnings.append(("MISSING_FIELD", f"{spec.kind}: '{source_key}' absent in {len(keys)} record(s), "
                                                 f"stored NULL: {_sample(keys)}", location.code))
    for source_key, keys in missing_value.items():
        result.warnings.append(("MISSING_VALUE", f"{spec.kind}: '{source_key}' null/empty in {len(keys)} record(s), "
                                                 f"stored NULL (never filled): {_sample(keys)}", location.code))
    for column, keys in out_of_range.items():
        low, high = spec.plausible[column]
        result.warnings.append(("OUT_OF_RANGE", f"{spec.kind}: {column} outside [{low}, {high}] in {len(keys)} "
                                                f"record(s), stored as published: {_sample(keys)}", location.code))

    if not parsed_times:
        return result
    start = min(parsed_times)
    result.series_start_at = start
    for record in result.records:
        record.values["series_start_at"] = start
    _series_checks(parsed_times, spec, location, retrieved_at, result)
    return result


def _series_checks(times: list[datetime], spec: SeriesSpec, location: Location, retrieved_at: datetime,
                   result: MosdacParseResult) -> None:
    code = location.code
    counts = Counter(times)
    duplicates = sorted(t for t, n in counts.items() if n > 1)
    if duplicates:
        result.warnings.append(("DUPLICATE_TIMESTAMP", f"{spec.kind}: {len(duplicates)} timestamp(s) appear more than "
                                                       f"once: {_sample([t.isoformat() for t in duplicates])}", code))
    if any(b < a for a, b in zip(times, times[1:])):
        result.info.append(("OUT_OF_ORDER", f"{spec.kind}: timestamps are not in ascending order", code))
    unique = sorted(counts)
    missing_steps: list[str] = []
    irregular: list[str] = []
    for a, b in zip(unique, unique[1:]):
        gap = b - a
        if gap == STEP:
            continue
        if gap % STEP == timedelta(0):
            missing_steps.append(f"{a.isoformat()}..{b.isoformat()} ({gap // STEP - 1} missing)")
        else:
            irregular.append(f"{a.isoformat()}->{b.isoformat()}")
    if missing_steps:
        total = sum(int((b - a) // STEP) - 1 for a, b in zip(unique, unique[1:]) if (b - a) % STEP == timedelta(0))
        result.warnings.append(("MISSING_TIMESTAMPS", f"{spec.kind}: {total} 15-minute step(s) missing: "
                                                      f"{_sample(missing_steps)}", code))
    if irregular:
        result.warnings.append(("IRREGULAR_SPACING", f"{spec.kind}: {len(irregular)} interval(s) not a multiple of "
                                                     f"15 minutes: {_sample(irregular)}", code))
    if len(times) != EXPECTED_POINTS:
        result.info.append(("POINT_COUNT", f"{spec.kind}: {len(times)} point(s) parsed (about {EXPECTED_POINTS} "
                                           "expected); response not rejected for its count", code))
    past = sum(1 for t in unique if t <= retrieved_at)
    if past == len(unique):
        result.warnings.append(("STALE_FORECAST", f"{spec.kind}: every valid time ({unique[0].isoformat()} .. "
                                                  f"{unique[-1].isoformat()}) is at or before retrieval "
                                                  f"({retrieved_at.isoformat()})", code))
    elif past:
        result.info.append(("POINTS_BEFORE_RETRIEVAL", f"{spec.kind}: {past} of {len(unique)} valid time(s) are at "
                                                       "or before retrieval", code))
