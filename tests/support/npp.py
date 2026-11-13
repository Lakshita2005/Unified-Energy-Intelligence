"""Builders for mocked NPP responses (test-only; no live API).

``DEMAND_ITEMS`` and ``GENERATION_ITEMS`` are verbatim excerpts of the live
responses for 2026-11-10, captured 2026-11-12.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import date
from typing import Any

import httpx

DAY = date(2026, 11, 10)
# 2026-11-10 00:01:33, 00:03:33 and 00:05:33 IST
DEMAND_ITEMS: list[dict[str, Any]] = [
    {"updated_on": 1794249093000, "name_of_data": "DEMAND MET", "value_of_data": 153196},
    {"updated_on": 1794249213000, "name_of_data": "DEMAND MET", "value_of_data": 152797},
    {"updated_on": 1794249333000, "name_of_data": "DEMAND MET", "value_of_data": 152869},
]
GENERATION_ITEMS: list[dict[str, Any]] = [
    {"updated_on": 1794249093000, "name_of_data": "HYDRO GENERATION", "value_of_data": 9461},
    {"updated_on": 1794249093000, "name_of_data": "THERMAL GENERATION", "value_of_data": 127297},
    {"updated_on": 1794249093000, "name_of_data": "GAS GENERATION", "value_of_data": 1463},
    {"updated_on": 1794249093000, "name_of_data": "SOLAR GENERATION", "value_of_data": 0},
    {"updated_on": 1794249093000, "name_of_data": "WIND GENERATION", "value_of_data": 7156},
    {"updated_on": 1794249093000, "name_of_data": "NUCLEAR GENERATION", "value_of_data": 7221},
    {"updated_on": 1794249213000, "name_of_data": "SOLAR GENERATION", "value_of_data": 0},
    {"updated_on": 1794249213000, "name_of_data": "THERMAL GENERATION", "value_of_data": 127647},
    {"updated_on": 1794249213000, "name_of_data": "HYDRO GENERATION", "value_of_data": 9117},
    {"updated_on": 1794249213000, "name_of_data": "NUCLEAR GENERATION", "value_of_data": 7212},
    {"updated_on": 1794249213000, "name_of_data": "GAS GENERATION", "value_of_data": 1479},
    {"updated_on": 1794249213000, "name_of_data": "WIND GENERATION", "value_of_data": 7190},
]


def to_bytes(items: Any) -> bytes:
    """Serialise like NPP does (compact JSON)."""
    return json.dumps(items, separators=(",", ":")).encode("utf-8")


Responder = bytes | Callable[[httpx.Request], httpx.Response]


class MockNpp:
    """httpx transport handler that records requests and returns canned responses."""

    def __init__(self, responder: Responder) -> None:
        self.responder = responder
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if callable(self.responder):
            return self.responder(request)
        return httpx.Response(200, content=self.responder, headers={"content-type": "application/json"})
