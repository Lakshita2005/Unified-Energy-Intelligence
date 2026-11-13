"""IEX reference rows in ``pi.market_master`` and ``pi.market_area``.

``pi.iex_market_price`` references both tables, so these rows must exist
before any price is loaded. They are inserted idempotently by an explicit
command (``python -m scripts.ingest_iex reference``), never implicitly by a
run. Existing rows are never changed; a row whose stored values differ is
reported.

Market codes carry the exchange (``IEX_DAM``) because ``market_master`` can
also hold other exchanges' markets. Bid areas (A1...W3) are IEX's bid-area
codes; ``MKT`` is the system-wide unconstrained price. ``country_code`` is
left NULL (``pi.country`` is empty).
"""

from __future__ import annotations

from dataclasses import dataclass, field

from sqlalchemy import text
from sqlalchemy.engine import Connection

from src.pipelines.iex.api import BID_AREAS, MARKETS, MKT_AREA

OPERATOR = "Indian Energy Exchange"
MARKET_NAMES = {"dam": "IEX Day-Ahead Market (DAM)", "gdam": "IEX Green Day-Ahead Market (G-DAM)",
                "rtm": "IEX Real-Time Market (RTM)"}
MARKET_ROWS: tuple[tuple[str, str, str], ...] = tuple(
    (market.market_code, MARKET_NAMES[kind], OPERATOR) for kind, market in MARKETS.items())
AREA_ROWS: tuple[tuple[str, str], ...] = (
    (MKT_AREA, "IEX unconstrained market clearing price (all bid areas)"),
    *((area, f"IEX bid area {area}") for area in BID_AREAS),
)


@dataclass
class SeedResult:
    inserted: list[str] = field(default_factory=list)
    unchanged: list[str] = field(default_factory=list)
    differing: list[str] = field(default_factory=list)


def seed_reference(connection: Connection, schema: str) -> SeedResult:
    """Insert missing market and area rows (``ON CONFLICT DO NOTHING``)."""
    result = SeedResult()
    for code, name, operator in MARKET_ROWS:
        inserted = connection.execute(text(
            f"INSERT INTO {schema}.market_master (market_code, market_name, market_operator) "
            "VALUES (:c, :n, :o) ON CONFLICT (market_code) DO NOTHING RETURNING market_code"),
            {"c": code, "n": name, "o": operator}).scalar_one_or_none()
        if inserted:
            result.inserted.append(f"market_master:{code}")
            continue
        stored = connection.execute(text(
            f"SELECT market_name, market_operator FROM {schema}.market_master WHERE market_code = :c"), {"c": code}).one()
        target = result.unchanged if tuple(stored) == (name, operator) else result.differing
        target.append(f"market_master:{code}")
    for code, name in AREA_ROWS:
        inserted = connection.execute(text(
            f"INSERT INTO {schema}.market_area (area_code, area_name) VALUES (:c, :n) "
            "ON CONFLICT (area_code) DO NOTHING RETURNING area_code"), {"c": code, "n": name}).scalar_one_or_none()
        if inserted:
            result.inserted.append(f"market_area:{code}")
            continue
        stored = connection.execute(text(f"SELECT area_name FROM {schema}.market_area WHERE area_code = :c"),
                                    {"c": code}).scalar_one()
        (result.unchanged if stored == name else result.differing).append(f"market_area:{code}")
    return result


def missing_reference(connection: Connection, schema: str, market_code: str, area_codes: list[str]) -> list[str]:
    """Reference rows a run needs that do not exist yet."""
    missing = []
    if connection.execute(text(f"SELECT 1 FROM {schema}.market_master WHERE market_code = :c"),
                          {"c": market_code}).first() is None:
        missing.append(f"market_master:{market_code}")
    present = {row[0] for row in connection.execute(
        text(f"SELECT area_code FROM {schema}.market_area WHERE area_code = ANY(:codes)"), {"codes": area_codes})}
    missing += [f"market_area:{code}" for code in area_codes if code not in present]
    return missing
