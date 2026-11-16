"""Synthetic IMD API responses and a mocked IMD API (no real IMD data, no network).

Record keys follow the official API reference exactly (e.g. ``Feel Like`` with a space,
``Todays_Forecast_Min_temp`` with a lower-case ``t``). Values are artificial, non-zero and
mostly three-decimal, so they never coincide with real records. The ``/aws_data_mapping``
structure is NOT documented: these fixtures assume the ``/aws_data`` identity keys (UNVERIFIED).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

import httpx

API = "https://imd-api.test/api/v1"


def aws_record(**overrides: Any) -> dict[str, Any]:
    record = {"ID": "SYNA0001", "CALL_SIGN": "TSA", "DISTRICT": "TEST_DISTRICT", "STATE": "DELHI",
              "STATION": "TEST STATION A", "DATE": "2026-11-10", "TIME": "07:00:00", "CURR_TEMP": 23.417,
              "DEW_POINT_TEMP": 11.263, "RH": 47.381, "WIND_DIRECTION": 187.313, "WIND_SPEED": 3.457,
              "MSLP": 1011.237, "MIN_TEMP": 19.123, "MAX_TEMP": 27.871, "Latitude": 28.123, "Longitude": 77.457,
              "WEATHER_CODE": 5, "NEBULOSITY": 4, "Feel Like": 24.193}
    record.update(overrides)
    return record


def station_record(**overrides: Any) -> dict[str, Any]:
    record = {"ID": "SYNA0001", "CALL_SIGN": "TSA", "STATION": "TEST STATION A", "DISTRICT": "TEST_DISTRICT",
              "STATE": "DELHI", "Latitude": 28.123, "Longitude": 77.457}
    record.update(overrides)
    return record


STATIONS = [station_record(),
            station_record(ID="SYNB0002", CALL_SIGN="TSB", STATION="TEST STATION B", STATE="DAMAN_AND_DIU",
                           Latitude=20.417, Longitude=72.837)]
OBSERVATIONS = [aws_record(),
                aws_record(ID="SYNB0002", CALL_SIGN="TSB", STATION="TEST STATION B", STATE="DAMAN_AND_DIU",
                           CURR_TEMP=29.613, Latitude=20.417, Longitude=72.837),
                aws_record(ID="SYNX9999", CALL_SIGN="TSX", STATION="TEST UNMAPPED STATION", CURR_TEMP=17.719)]


def city_record(issue: str = "2026-11-10", code: str = "99001", **overrides: Any) -> dict[str, Any]:
    record: dict[str, Any] = {
        "Date": issue, "Station_Code": code, "Station_Name": "TEST CITY", "Today_Max_temp": 30.137,
        "Today_Max_Departure_from_Normal": 1.217, "Previous_Day_Max_temp": 29.873,
        "Previous_Day_Max_Departure_from_Normal": 0.917, "Today_Min_temp": 17.413,
        "Today_Min_Departure_from_Normal": -0.613, "Past_24_hrs_Rainfall": 2.317,
        "Relative_Humidity_at_0830": 71.213, "Relative_Humidity_at_1730": 43.317,
        "Previous_Day_Relative_Humidity_at_1730": 45.113, "Sunset_time": "17:29", "Sunrise_time": "06:41",
        "Moonset_time": "15:02", "Moonrise_time": "03:17",
        "Todays_Forecast_Max_Temp": 29.137, "Todays_Forecast_Min_temp": 16.317, "Todays_Forecast": "Mainly clear sky",
    }
    for n in range(2, 8):
        record[f"Day_{n}_Max_Temp"] = round(29.137 + 0.111 * n, 3)
        record[f"Day_{n}_Min_temp"] = round(16.317 + 0.101 * n, 3)
        record[f"Day_{n}_Forecast"] = f"Synthetic forecast day {n}"
    record.update(overrides)
    return record


def body(payload: Any) -> bytes:
    return json.dumps(payload, default=lambda v: float(v) if isinstance(v, Decimal) else str(v)).encode()


@dataclass
class MockImd:
    """httpx handler for the three documented IMD endpoints."""

    responses: dict[str, list[Any]] = field(default_factory=dict)   # path suffix -> queue of payloads/Responses
    calls: list[str] = field(default_factory=list)
    headers_seen: list[dict[str, str]] = field(default_factory=list)

    def set(self, path: str, *payloads: Any) -> None:
        self.responses[path] = list(payloads)

    def __call__(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path.removeprefix("/api/v1")
        self.calls.append(f"{path}?{request.url.query.decode()}" if request.url.query else path)
        self.headers_seen.append(dict(request.headers))
        queue = self.responses.get(path)
        if not queue:
            return httpx.Response(404, json={"error": "not mocked"})
        payload = queue.pop(0) if len(queue) > 1 else queue[0]
        if isinstance(payload, httpx.Response):
            return payload
        if isinstance(payload, bytes):
            return httpx.Response(200, content=payload, headers={"content-type": "application/json"})
        return httpx.Response(200, content=body(payload), headers={"content-type": "application/json"})
