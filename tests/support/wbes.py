"""Synthetic WBES PX responses and a mocked WBES API (no real WBES data, no network).

Values are artificial with three decimal places, so they can never coincide with
published values already in the database the integration tests run against.
The mock enforces bearer authentication, rotates refresh tokens like the portal,
and can refuse refreshes or return unsuccessful responses.
"""

from __future__ import annotations

import base64
import json
import time
from dataclasses import dataclass, field
from datetime import date
from typing import Any

import httpx

API = "https://wbes-api.test"
PORTAL = "https://wbes-portal.test/site/open-access/px"
UTILITY = "0123456789ABCDEF0123456789ABCDEF"
EXCHANGES = [("IEX", 1), ("PXI", 2), ("HPX", 3)]
MARKETS = [("DAM", 11), ("RTM", 12), ("GDAM", 13), ("HPDAM", 14)]


def make_jwt(exp: float) -> str:
    """Unsigned JWT-shaped token with an ``exp`` claim (the client only reads ``exp``)."""
    def enc(obj: dict[str, Any]) -> str:
        return base64.urlsafe_b64encode(json.dumps(obj).encode()).decode().rstrip("=")
    return f"{enc({'alg': 'none'})}.{enc({'exp': exp, 'sub': 'synthetic'})}.sig"


def series(seed: float, step: float = 0.517) -> list[float]:
    """96 artificial values with 3 decimals; block 1 = seed."""
    return [round(seed + step * i, 3) for i in range(96)]


def region_items(scale: float = 1.0) -> list[dict[str, Any]]:
    return [{"Region": rid, "Injection": series(round(100.111 * rid * scale, 3)),
             "Drawal": series(round(50.222 * rid * scale, 3), 0.313)} for rid in range(1, 7)]


def link_items() -> list[dict[str, Any]]:
    return [{"LinkNumber": 1, "LinkName": "TEST LINK ER-NR", "NetAmount": series(-12.345, 0.111)},
            {"LinkNumber": 2, "LinkName": "TEST LINK WR-SR", "NetAmount": series(7.007, -0.073)}]


def entity_items() -> list[dict[str, Any]]:
    return [{"EntityAcronym": "TEST_GEN_A", "EntityUnitAcronym": "U1", "EntityPxCode": "PXA1", "EntityRegionId": 2,
             "Injection": series(31.313), "Drawal": [0.0] * 96},
            {"EntityAcronym": "TEST_DISCOM_B", "EntityUnitAcronym": None, "EntityPxCode": "PXB1", "EntityRegionId": 3,
             "Injection": [0.0] * 96, "Drawal": series(44.044, 0.021)},
            {"EntityAcronym": "TEST_GEN_A", "EntityUnitAcronym": "U2", "EntityPxCode": "PXA2", "EntityRegionId": 2,
             "Injection": series(9.009, 0.001), "Drawal": [0.0] * 96}]


def px_record(day: date, revision: int, *, created_on: Any = "2026-11-09T14:05:31", regions: Any = None,
              links: Any = None, entities: Any = None, power_ex_id: str = "PXID-SYN-1", **extra: Any) -> dict[str, Any]:
    record = {"EffectiveForDate": day.strftime("%d-%m-%Y"), "RevisionNo": revision, "PowerExId": power_ex_id,
              "CreatedOn": created_on,
              "AllRegionPowerExJsonData": {"RegionWiseJsonData": region_items() if regions is None else regions},
              "AllLinkPowerExJsonData": {"LinkWiseJsonData": link_items() if links is None else links},
              "AllEntityPowerExJsonData": {"EntityWiseJsonData": entity_items() if entities is None else entities}}
    record.update(extra)
    return record


def envelope(records: list[dict[str, Any]], code: str | None = "WBES-200", message: str = "Success") -> dict[str, Any]:
    out: dict[str, Any] = {"ResponseBody": records}
    if code is not None:
        out["ResponseStatus"] = {"Code": code, "Message": message}
    return out


@dataclass
class MockWbes:
    """httpx handler emulating the WBES endpoints used by the PX pipeline."""

    details: dict[tuple[str, str, str], Any] = field(default_factory=dict)   # (dd-mm-yyyy, px, market) -> payload
    access: str = field(default_factory=lambda: make_jwt(time.time() + 600))
    refresh: str = "refresh-0"
    refusals: int = 0                  # refresh calls to refuse (HTTP 401)
    expire_on_refresh: bool = False    # answer refresh with IsExpired = true
    force_401: int = 0                 # data calls to answer with 401 regardless of token
    exchanges: list[tuple[str, int]] = field(default_factory=lambda: list(EXCHANGES))
    markets: list[tuple[str, int]] = field(default_factory=lambda: list(MARKETS))
    calls: list[tuple[str, str]] = field(default_factory=list)
    refreshes: int = 0
    seen_bodies: list[str] = field(default_factory=list)

    def set(self, day: date, px: str, market: str, payload: Any) -> None:
        self.details[(day.strftime("%d-%m-%Y"), px, market)] = payload

    def __call__(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        body = request.content.decode() if request.content else ""
        self.calls.append((request.method, path))
        self.seen_bodies.append(body)
        if path.endswith("/auth/api/Auth/refresh-token"):
            return self._refresh(request, body)
        if request.headers.get("Authorization") != f"Bearer {self.access}" or self.force_401:
            self.force_401 = max(0, self.force_401 - 1)
            return httpx.Response(401, json={"message": "Unauthorized"})
        if "/GetPxTypeList/date/" in path:
            return httpx.Response(200, json=[{"PowerExchangeTypeName": n, "PowerExchangeTypeId": i}
                                             for n, i in self.exchanges])
        if "/GetPxTransTypeList/date/" in path:
            return httpx.Response(200, json=[{"PowerExTransactionTypeName": n, "PowerExTransactionTypeId": i}
                                             for n, i in self.markets])
        if path.endswith("/GetPowerExchangeByDetails"):
            req = json.loads(body)
            px = next(n for n, i in self.exchanges if i == req["PowerExchangeTypeId"])
            mkt = next(n for n, i in self.markets if i == req["PowerExTransactionTypeId"])
            payload = self.details.get((req["InputDate"], px, mkt), envelope([]))
            if isinstance(payload, httpx.Response):
                return payload
            return httpx.Response(200, json=payload)
        return httpx.Response(404)

    def _refresh(self, request: httpx.Request, body: str) -> httpx.Response:
        if self.refusals:
            self.refusals -= 1
            return httpx.Response(401, json={"Token": "leak-check-should-not-appear"})
        data = json.loads(body)
        if data.get("Token") != self.refresh or data.get("UtilityId") != UTILITY:
            return httpx.Response(401, json={})
        if self.expire_on_refresh:
            return httpx.Response(200, json={"IsExpired": True})
        self.refreshes += 1
        self.access = make_jwt(time.time() + 600)
        self.refresh = f"refresh-{self.refreshes}"
        return httpx.Response(200, json={"Token": self.access, "RefreshToken": self.refresh, "IsExpired": False})
