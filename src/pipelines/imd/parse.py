"""Parse IMD AWS station mapping, AWS observations and city forecasts (documented fields only).

Field names follow the API reference (https://api.imd.gov.in/public/api_reference.html):

* ``/aws_data`` sample record: ``ID``, ``CALL_SIGN``, ``DISTRICT``, ``STATE``, ``STATION``, ``DATE``
  (``2024-06-06``), ``TIME`` (``07:00:00``), ``CURR_TEMP``, ``DEW_POINT_TEMP``, ``RH``,
  ``WIND_DIRECTION``, ``WIND_SPEED``, ``MSLP``, ``MIN_TEMP``, ``MAX_TEMP``, ``Latitude``, ``Longitude``,
  ``WEATHER_CODE``, ``NEBULOSITY``, ``Feel Like`` (with a space). No units or time zone are documented.
* ``/aws_data_mapping``: fields are **not documented**. The parser accepts records that carry the
  same identity keys as ``/aws_data`` (``ID``, ``CALL_SIGN``, optionally ``STATION``, ``DISTRICT``,
  ``STATE``, ``Latitude``, ``Longitude``); a response without them is rejected, never guessed.
* ``/cityforecast``: ``Date`` (YYYY-mm-dd), ``Station_Code``, ``Station_Name``, forecasts
  ``Todays_Forecast_Max_Temp`` / ``Todays_Forecast_Min_temp`` / ``Todays_Forecast`` (day 1) and
  ``Day_<n>_Max_Temp`` / ``Day_<n>_Min_temp`` / ``Day_<n>_Forecast`` (days 2-7), plus observed and
  astronomical fields that are **not** loaded here (kept in raw storage; observations and forecasts
  are never mixed in one record).

Rules: values are exact ``Decimal``; JSON null or an empty string is a missing value (stored NULL,
reported); any other non-numeric value rejects the record (``PARSE_ERROR``); undocumented fields are
reported and preserved in raw storage; nothing is invented or defaulted.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any
from zoneinfo import ZoneInfo

from src.ingestion.models import ParseFailure, ParsedRecord
from src.pipelines.imd.errors import ImdResponseError
from src.pipelines.imd.reference import AWS_NETWORK, state_code_for

IST = ZoneInfo("Asia/Kolkata")

AWS_IDENTITY = ("ID", "CALL_SIGN")
AWS_CONTEXT = {"STATION": "station_name", "DISTRICT": "district_name", "STATE": "state_name"}
AWS_MEASUREMENTS = {
    "CURR_TEMP": "curr_temp_value", "DEW_POINT_TEMP": "dew_point_temp_value", "RH": "rh_value",
    "WIND_DIRECTION": "wind_direction_value", "WIND_SPEED": "wind_speed_value", "MSLP": "mslp_value",
    "MIN_TEMP": "min_temp_value", "MAX_TEMP": "max_temp_value", "Feel Like": "feels_like_value",
    "WEATHER_CODE": "weather_code", "NEBULOSITY": "nebulosity_value",
}
AWS_DOCUMENTED = {*AWS_IDENTITY, *AWS_CONTEXT, *AWS_MEASUREMENTS, "DATE", "TIME", "Latitude", "Longitude"}
# Unit-independent plausibility checks (WARNING only, because units/scales are unverified).
AWS_PLAUSIBLE = {"rh_value": (0, 100), "nebulosity_value": (0, 8), "wind_direction_value": (0, 360)}

CITY_FORECAST_KEYS = {1: ("Todays_Forecast_Max_Temp", "Todays_Forecast_Min_temp", "Todays_Forecast"),
                      **{n: (f"Day_{n}_Max_Temp", f"Day_{n}_Min_temp", f"Day_{n}_Forecast") for n in range(2, 8)}}
CITY_OBSERVED_KEYS = (
    "Today_Max_temp", "Today_Max_Departure_from_Normal", "Previous_Day_Max_temp",
    "Previous_Day_Max_Departure_from_Normal", "Today_Min_temp", "Today_Min_Departure_from_Normal",
    "Past_24_hrs_Rainfall", "Relative_Humidity_at_0830", "Relative_Humidity_at_1730",
    "Previous_Day_Relative_Humidity_at_1730", "Sunset_time", "Sunrise_time", "Moonset_time", "Moonrise_time",
)
CITY_DOCUMENTED = {"Date", "Station_Code", "Station_Name", *CITY_OBSERVED_KEYS,
                   *(k for keys in CITY_FORECAST_KEYS.values() for k in keys)}


def to_decimal(value: Any, label: str) -> Decimal | None:
    """Exact number, or None for JSON null / empty string. Raises ValueError for anything else."""
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"{label} is a boolean ({value!r})")
    if isinstance(value, (int, Decimal)):
        return Decimal(value)
    if isinstance(value, float):
        return Decimal(repr(value))
    text = str(value).strip()
    if text == "":
        return None
    try:
        number = Decimal(text)
    except InvalidOperation:
        raise ValueError(f"{label} is not a number: {text[:30]!r}") from None
    if not number.is_finite():
        raise ValueError(f"{label} is not a finite number: {text[:30]!r}")
    return number


def _text(value: Any) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None


def _timing(retrieved_at: datetime) -> dict[str, Any]:
    # IMD does not state publication times: rows are available from retrieval.
    return {"retrieved_at": retrieved_at, "source_published_at": None, "source_available_at": retrieved_at,
            "availability_basis": "RETRIEVED_AT"}


@dataclass
class ImdParseResult:
    records: list[ParsedRecord] = field(default_factory=list)
    failures: list[ParseFailure] = field(default_factory=list)
    warnings: list[tuple[str, str, str]] = field(default_factory=list)   # (code, description, key)
    info: list[tuple[str, str, str]] = field(default_factory=list)


def _undocumented(records: list[dict[str, Any]], documented: set[str], result: ImdParseResult, what: str) -> None:
    extra = sorted({k for r in records for k in r} - documented)
    if extra:
        result.info.append(("UNDOCUMENTED_FIELDS", f"{what}: fields not in the API reference are preserved in raw "
                            f"storage but not loaded: {extra[:20]}", what))


# ---------------------------------------------------------------- stations

def parse_aws_station_mapping(records: list[dict[str, Any]], retrieved_at: datetime) -> ImdParseResult:
    """``/aws_data_mapping`` -> ``imd_station`` records (structure assumed from ``/aws_data``; UNVERIFIED)."""
    result = ImdParseResult()
    if records and not any(any(k in r for k in AWS_IDENTITY) for r in records):
        raise ImdResponseError("aws_data_mapping: no record carries ID or CALL_SIGN; the mapping structure is not "
                               "documented and does not match the /aws_data identity fields (not guessed)")
    _undocumented(records, AWS_DOCUMENTED, result, "aws_data_mapping")
    by_id: dict[str, set[str]] = {}
    by_sign: dict[str, set[str]] = {}
    for position, record in enumerate(records, start=1):
        imd_id, call_sign = _text(record.get("ID")), _text(record.get("CALL_SIGN"))
        key = f"station|{imd_id}|{call_sign}|#{position}"
        if imd_id is None or call_sign is None:
            result.failures.append(ParseFailure(key, _identity(record), "station record without ID and CALL_SIGN; "
                                                "not loaded"))
            continue
        try:
            latitude, longitude = to_decimal(record.get("Latitude"), "Latitude"), to_decimal(record.get("Longitude"),
                                                                                              "Longitude")
            if latitude is not None and not -90 <= latitude <= 90:
                raise ValueError(f"Latitude {latitude} is out of range")
            if longitude is not None and not -180 <= longitude <= 180:
                raise ValueError(f"Longitude {longitude} is out of range")
        except ValueError as exc:
            result.failures.append(ParseFailure(key, _identity(record), f"station {call_sign}: {exc}; not loaded"))
            continue
        state_name = _text(record.get("STATE"))
        state_code = state_code_for(state_name)
        if state_name is not None and state_code is None:
            result.warnings.append(("UNMAPPED_STATE", f"station {imd_id}/{call_sign}: STATE {state_name!r} is not an "
                                    "exact documented IMD state name; state_code left NULL", key))
        by_id.setdefault(imd_id, set()).add(call_sign)
        by_sign.setdefault(call_sign, set()).add(imd_id)
        values = {"network": AWS_NETWORK, "imd_id": imd_id, "call_sign": call_sign,
                  "station_name": _text(record.get("STATION")), "district_name": _text(record.get("DISTRICT")),
                  "state_name": state_name, "state_code": state_code, "latitude": latitude, "longitude": longitude,
                  **_timing(retrieved_at)}
        result.records.append(ParsedRecord(f"station|{imd_id}|{call_sign}", values, _identity(record)))
    for imd_id, signs in by_id.items():
        if len(signs) > 1:
            result.warnings.append(("STATION_ID_AMBIGUOUS", f"ID {imd_id} appears with several call signs "
                                    f"{sorted(signs)} (identifier uniqueness is unverified)", f"station|{imd_id}"))
    for sign, ids in by_sign.items():
        if len(ids) > 1:
            result.warnings.append(("STATION_ID_AMBIGUOUS", f"CALL_SIGN {sign} appears with several IDs {sorted(ids)} "
                                    "(identifier uniqueness is unverified)", f"station|{sign}"))
    return result


def _identity(record: dict[str, Any]) -> dict[str, Any]:
    return {k: record.get(k) for k in ("ID", "CALL_SIGN", "STATION", "DATE", "TIME", "Station_Code", "Date")
            if k in record}


# ------------------------------------------------------------ observations

def parse_aws_observations(records: list[dict[str, Any]], retrieved_at: datetime,
                           time_zone: str = "unverified") -> ImdParseResult:
    """``/aws_data`` -> ``imd_station_observation`` records."""
    result = ImdParseResult()
    _undocumented(records, AWS_DOCUMENTED, result, "aws_data")
    if records and time_zone == "unverified":
        result.info.append(("TIME_ZONE_UNVERIFIED", "aws_data DATE/TIME stored as published; observed_at left NULL "
                            "until IMD_AWS_TIME_ZONE is verified", "aws_data"))
    zone = {"Asia/Kolkata": IST, "UTC": timezone.utc}.get(time_zone)
    retrieved_day = retrieved_at.astimezone(IST).date()
    for position, record in enumerate(records, start=1):
        imd_id, call_sign = _text(record.get("ID")), _text(record.get("CALL_SIGN"))
        key = f"obs|{imd_id}|{call_sign}|{record.get('DATE')}|{record.get('TIME')}"
        missing = [k for k in (*AWS_IDENTITY, "DATE", "TIME") if _text(record.get(k)) is None]
        if missing:
            result.failures.append(ParseFailure(f"{key}|#{position}", _identity(record),
                                                f"observation without {missing}; not loaded"))
            continue
        try:
            obs_date = date.fromisoformat(_text(record["DATE"]))
            obs_time = time.fromisoformat(_text(record["TIME"]))
        except ValueError:
            result.failures.append(ParseFailure(key, _identity(record), f"DATE {record.get('DATE')!r} / TIME "
                                                f"{record.get('TIME')!r} not in the documented YYYY-mm-dd / HH:MM:SS "
                                                "format; not loaded"))
            continue
        try:
            measures = {column: to_decimal(record.get(source), source) for source, column in AWS_MEASUREMENTS.items()}
            latitude = to_decimal(record.get("Latitude"), "Latitude")
            longitude = to_decimal(record.get("Longitude"), "Longitude")
        except ValueError as exc:
            result.failures.append(ParseFailure(key, _identity(record), f"station {call_sign}: {exc}; not loaded"))
            continue
        absent = [s for s in AWS_MEASUREMENTS if s not in record]
        empty = [s for s, c in AWS_MEASUREMENTS.items() if s in record and measures[c] is None]
        if absent:
            result.warnings.append(("MISSING_FIELD", f"station {call_sign} {obs_date} {obs_time}: fields absent "
                                    f"{absent}; stored as NULL", key))
        if empty:
            result.warnings.append(("MISSING_VALUE", f"station {call_sign} {obs_date} {obs_time}: no value for "
                                    f"{empty}; stored as NULL", key))
        if all(v is None for v in measures.values()):
            result.warnings.append(("ALL_MEASUREMENTS_NULL", f"station {call_sign} {obs_date} {obs_time}: every "
                                    "measurement is missing", key))
        for column, (low, high) in AWS_PLAUSIBLE.items():
            value = measures[column]
            if value is not None and not low <= value <= high:
                result.warnings.append(("OUT_OF_RANGE", f"station {call_sign} {obs_date} {obs_time}: {column} {value} "
                                        f"outside {low}-{high}; stored as published", key))
        if obs_date > retrieved_day + timedelta(days=1):
            result.warnings.append(("FUTURE_OBSERVATION", f"station {call_sign}: DATE {obs_date} is after the retrieval "
                                    f"day {retrieved_day}", key))
        state_name = _text(record.get("STATE"))
        values = {"network": AWS_NETWORK, "imd_id": imd_id, "call_sign": call_sign, "imd_station_id": None,
                  "observation_date": obs_date, "observation_time": obs_time,
                  "observed_at": datetime.combine(obs_date, obs_time, zone) if zone else None,
                  "station_name": _text(record.get("STATION")), "district_name": _text(record.get("DISTRICT")),
                  "state_name": state_name, "state_code": state_code_for(state_name),
                  "latitude": latitude, "longitude": longitude, **measures, **_timing(retrieved_at)}
        result.records.append(ParsedRecord(key, values, _identity(record)))
    return result


# --------------------------------------------------------------- forecasts

def parse_city_forecast(records: list[dict[str, Any]], retrieved_at: datetime) -> ImdParseResult:
    """``/cityforecast`` -> ``imd_city_forecast`` records (forecast fields only; one record per lead day)."""
    result = ImdParseResult()
    _undocumented(records, CITY_DOCUMENTED, result, "cityforecast")
    observed = sorted({k for r in records for k in r if k in CITY_OBSERVED_KEYS})
    if observed:
        result.info.append(("OBSERVED_FIELDS_NOT_LOADED", f"cityforecast: observed/astronomical fields are kept in "
                            f"raw storage only (not mixed with forecasts): {observed}", "cityforecast"))
    retrieved_day = retrieved_at.astimezone(IST).date()
    for position, record in enumerate(records, start=1):
        code, issue_text = _text(record.get("Station_Code")), _text(record.get("Date"))
        key = f"city|{code}|{issue_text}"
        if code is None or issue_text is None:
            result.failures.append(ParseFailure(f"{key}|#{position}", _identity(record),
                                                "city forecast without Station_Code or Date; not loaded"))
            continue
        try:
            issue_date = date.fromisoformat(issue_text)
        except ValueError:
            result.failures.append(ParseFailure(key, _identity(record), f"Date {issue_text!r} is not YYYY-mm-dd; "
                                                "not loaded"))
            continue
        if issue_date > retrieved_day + timedelta(days=1):
            result.warnings.append(("FUTURE_ISSUE_DATE", f"station {code}: Date {issue_date} is after the retrieval day "
                                    f"{retrieved_day}", key))
        name = _text(record.get("Station_Name"))
        for lead, (max_key, min_key, text_key) in CITY_FORECAST_KEYS.items():
            lkey = f"{key}|d{lead}"
            if not any(k in record for k in (max_key, min_key, text_key)):
                result.warnings.append(("INCOMPLETE_FORECAST", f"station {code} issue {issue_date}: day {lead} fields "
                                        "absent; not loaded", lkey))
                continue
            try:
                max_value, min_value = to_decimal(record.get(max_key), max_key), to_decimal(record.get(min_key), min_key)
            except ValueError as exc:
                result.failures.append(ParseFailure(lkey, _identity(record), f"station {code} issue {issue_date} day "
                                                    f"{lead}: {exc}; not loaded"))
                continue
            text = _text(record.get(text_key))
            if max_value is None and min_value is None and text is None:
                result.warnings.append(("INCOMPLETE_FORECAST", f"station {code} issue {issue_date}: day {lead} has no "
                                        "values; not loaded", lkey))
                continue
            empty = [k for k, v in ((max_key, max_value), (min_key, min_value), (text_key, text)) if v is None]
            if empty:
                result.warnings.append(("MISSING_VALUE", f"station {code} issue {issue_date} day {lead}: no value for "
                                        f"{empty}; stored as NULL", lkey))
            values = {"station_code": code, "station_name": name, "issue_date": issue_date,
                      "forecast_date": issue_date + timedelta(days=lead - 1), "lead_day": lead,
                      "max_temp_value": max_value, "min_temp_value": min_value, "forecast_text": text,
                      **_timing(retrieved_at)}
            result.records.append(ParsedRecord(lkey, values, _identity(record)))
    return result
