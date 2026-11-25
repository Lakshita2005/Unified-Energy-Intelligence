"""IEX bid area -> state rows in ``pi.area_state_map`` (Alembic ``0012_area_state_map``).

Seeded by an explicit command (``python -m scripts.ingest_iex area-states``), never by a price run, so
price ingestion does not depend on this table. Inserts are idempotent: a row is identified by
``(state_code, state_part, effective_from)``; an existing row is never changed, and one whose stored values
differ is reported.

No row here is OFFICIAL. IEX publishes no machine-readable bid-area list, so the rows rest on a
third-party reproduction of IEX's table (2016), later secondary sources, and inferences recorded in
``notes``. ``effective_from`` is the earliest date a source shows the mapping, not the date IEX introduced
it. Confirm against IEX or Grid-India before treating a row as OFFICIAL.

Not mapped: Lakshadweep (LD) and Andaman & Nicobar (AN), which are not connected to the national grid, and
``MKT`` (the system-wide price, not an area).
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date

from sqlalchemy import inspect, text
from sqlalchemy.engine import Connection

from src.common.exceptions import ConfigurationError
from src.pipelines.iex.api import BID_AREAS

TABLE = "area_state_map"
STATUSES = frozenset({"OFFICIAL", "SECONDARY", "INFERRED"})


@dataclass(frozen=True)
class Source:
    name: str
    url: str | None
    published_on: date | None


IEX_TABLE_2016 = Source(
    "IEX bid-area table (Table 1, 'Source: IEX') in Sinha & Mathur, 'Empirical Analysis of Developments in "
    "the Day Ahead Electricity Markets in India', MPRA Paper No. 72969",
    "https://mpra.ub.uni-muenchen.de/72969/1/MPRA_paper_72969.pdf", date(2016, 8, 11))
RAP_2020 = Source(
    "Regulatory Assistance Project, 'Early insights from India's first month operating the real-time market'",
    "https://www.raponline.org/blog/early-insights-from-indias-first-month-operating-the-real-time-market/",
    date(2020, 7, 10))
LADAKH_UT = Source("Jammu and Kashmir Reorganisation Act, 2019 (Ladakh a union territory from 2019-10-31)",
                   None, date(2019, 8, 9))

BASELINE = IEX_TABLE_2016.published_on
THIRTEEN_AREAS_SEEN = date(2019, 3, 1)


@dataclass(frozen=True)
class AreaState:
    area_code: str
    state_code: str
    state_part: str | None
    effective_from: date
    effective_to: date | None
    verification_status: str
    source: Source
    notes: str | None = None

    @property
    def key(self) -> tuple[str, str, date]:
        return self.state_code, self.state_part or "", self.effective_from

    @property
    def label(self) -> str:
        part = f" ({self.state_part})" if self.state_part else ""
        return f"{self.area_code}:{self.state_code}{part}@{self.effective_from}"


def _baseline(area: str, *states: str, notes: str | None = None) -> list[AreaState]:
    return [AreaState(area, state, None, BASELINE, None, "SECONDARY", IEX_TABLE_2016, notes) for state in states]


ROWS: tuple[AreaState, ...] = (
    *_baseline("N1", "HP", "CH", "HR"),
    *_baseline("N1", "JK", notes="Source lists Jammu and Kashmir, which then included Ladakh"),
    AreaState("N1", "LA", None, date(2019, 10, 31), None, "INFERRED", LADAKH_UT,
              "Ladakh was part of Jammu and Kashmir (N1) in the 2016 table; its own area is not confirmed"),
    *_baseline("N2", "UP", "UK", "RJ", "DL"),
    *_baseline("N3", "PB"),
    *_baseline("E1", "WB", "SK", "BR", "JH"),
    *_baseline("E2", "OD"),
    *_baseline("W1", "MP"),
    *_baseline("W2", "MH", "GJ"),
    *_baseline("W2", "DH", notes="Source lists Daman and Diu and Dadra and Nagar Haveli, merged into one union "
                                 "territory (DH) on 2020-01-26"),
    AreaState("W2", "GA", "North Goa", BASELINE, None, "SECONDARY", IEX_TABLE_2016),
    *_baseline("W3", "CG"),
    *_baseline("S1", "KA"),
    *_baseline("S1", "AP", notes="Telangana was formed in 2014; the source lists Andhra Pradesh only"),
    AreaState("S1", "PY", "Yanam", BASELINE, None, "SECONDARY", IEX_TABLE_2016),
    AreaState("S1", "GA", "South Goa", BASELINE, None, "SECONDARY", IEX_TABLE_2016),
    AreaState("S1", "TS", None, RAP_2020.published_on, None, "SECONDARY", RAP_2020,
              "S1 described as 'Karnataka, Andhra Pradesh and Telangana primarily' (June 2020 data)"),
    *_baseline("S2", "TN"),
    AreaState("S2", "PY", "Puducherry, Karaikal, Mahe", BASELINE, None, "SECONDARY", IEX_TABLE_2016),
    AreaState("S2", "KL", None, BASELINE, THIRTEEN_AREAS_SEEN, "SECONDARY", IEX_TABLE_2016,
              "Ended at the first evidence of 13 bid areas (IEX presentation, March 2019); Kerala's area "
              "between then and 2020-07-10 is undocumented and left unmapped"),
    AreaState("S3", "KL", None, RAP_2020.published_on, None, "INFERRED", RAP_2020,
              "The source describes S2 as Tamil Nadu and Pondicherry, without Kerala. Kerala in S3 is "
              "corroborated by KSERC order OP 37/2024 (2024-09-26), which settles KSEBL energy at the "
              "'average IEX rate of S3 region'"),
    *_baseline("A1", "TR", "ML", "MN", "MZ", "NL"),
    *_baseline("A2", "AS", "AR"),
)


def validate_rows(rows: tuple[AreaState, ...] = ROWS) -> None:
    """Raise ``ValueError`` for unknown areas or statuses, empty periods, duplicate keys or overlaps."""
    by_part: dict[tuple[str, str], list[AreaState]] = defaultdict(list)
    for row in rows:
        if row.area_code not in BID_AREAS:
            raise ValueError(f"{row.label}: {row.area_code} is not an IEX bid area")
        if row.verification_status not in STATUSES:
            raise ValueError(f"{row.label}: unknown verification_status {row.verification_status!r}")
        if row.effective_to is not None and row.effective_to <= row.effective_from:
            raise ValueError(f"{row.label}: effective_to must be after effective_from")
        by_part[(row.state_code, row.state_part or "")].append(row)
    for part_rows in by_part.values():
        part_rows.sort(key=lambda r: r.effective_from)
        for earlier, later in zip(part_rows, part_rows[1:]):
            if earlier.effective_to is None or earlier.effective_to > later.effective_from:
                raise ValueError(f"{earlier.label} overlaps {later.label}")


@dataclass
class SeedResult:
    inserted: list[str] = field(default_factory=list)
    unchanged: list[str] = field(default_factory=list)
    differing: list[str] = field(default_factory=list)
    overlaps: list[str] = field(default_factory=list)


def _values(row: AreaState) -> dict[str, object]:
    return {"area": row.area_code, "state": row.state_code, "part": row.state_part, "start": row.effective_from,
            "end": row.effective_to, "status": row.verification_status, "source": row.source.name,
            "url": row.source.url, "published": row.source.published_on, "notes": row.notes}


def seed_area_states(connection: Connection, schema: str, rows: tuple[AreaState, ...] = ROWS) -> SeedResult:
    """Insert missing rows; report rows stored with other values and any overlapping periods in the table.

    Raises:
        ConfigurationError: ``pi.area_state_map`` does not exist (migration 0012 not applied).
        ValueError: the rows fail :func:`validate_rows`.
    """
    if not inspect(connection).has_table(TABLE, schema=schema):
        raise ConfigurationError(f"{schema}.{TABLE} does not exist; apply Alembic migration 0012_area_state_map")
    validate_rows(rows)
    result = SeedResult()
    columns = ("area_code, state_code, state_part, effective_from, effective_to, verification_status, "
               "source_name, source_url, source_published_on, notes")
    for row in rows:
        values = _values(row)
        stored = connection.execute(text(
            f"SELECT {columns} FROM {schema}.{TABLE} WHERE state_code = :state "
            "AND coalesce(state_part, '') = coalesce(CAST(:part AS text), '') AND effective_from = :start"),
            values).one_or_none()
        if stored is None:
            connection.execute(text(
                f"INSERT INTO {schema}.{TABLE} ({columns}) VALUES (:area, :state, :part, :start, :end, :status, "
                ":source, :url, :published, :notes)"), values)
            result.inserted.append(row.label)
        elif tuple(stored) == tuple(values.values()):
            result.unchanged.append(row.label)
        else:
            result.differing.append(row.label)
    result.overlaps = [f"{r.a} overlaps {r.b}" for r in connection.execute(text(
        f"SELECT a.area_code || ':' || a.state_code || '@' || a.effective_from AS a, "
        f"b.area_code || ':' || b.state_code || '@' || b.effective_from AS b "
        f"FROM {schema}.{TABLE} a JOIN {schema}.{TABLE} b ON a.state_code = b.state_code "
        "AND coalesce(a.state_part, '') = coalesce(b.state_part, '') AND a.id < b.id "
        "AND daterange(a.effective_from, a.effective_to) && daterange(b.effective_from, b.effective_to)"))]
    return result
