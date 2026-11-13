"""CEA reference data: regions, states, plants, units and source name mappings.

Approved as decision D1 (docs/cea_source_assessment.md). The CEA tables
reference ``pi.plant`` / ``pi.generating_unit`` (and the monthly report
``pi.state`` / ``pi.region``), which were empty. This module seeds them from
public CEA data, idempotently and only through the explicit
``python -m scripts.ingest_cea reference`` command:

* the 5 grid regions and 36 states/UTs (:mod:`.geography`);
* one ``plant`` per Indian station in DGR sub-report 2 of a chosen report
  date (``plant_code = 'CEA:' + normalised station name``; state, region and
  CEA station type as published; capacity and fuel are not set because the
  report gives *monitored* capacity and a type, not installed capacity/fuel);
* one ``generating_unit`` per unit row (published unit label and capacity);
* ``source_entity_mapping`` rows (normalised station name -> plant) for the
  datasets that are loaded by plant.

Inserts use ``ON CONFLICT DO NOTHING`` / existence checks. Existing rows are
never updated or deleted; a row whose stored values differ is reported.
Stations that appear only in older reports are added by running the command
again with an older ``--date``.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from sqlalchemy import text
from sqlalchemy.engine import Connection

from src.pipelines.cea.cells import norm
from src.pipelines.cea.geography import REGIONS, STATES, state_code
from src.pipelines.cea.parse_dgr import Dgr2Report

PLANT_CODE_PREFIX = "CEA:"
# Datasets whose rows are keyed by plant and resolved through source_entity_mapping.
PLANT_DATASETS = ("CEA_COAL_STOCK_DAILY", "CEA_DGR_DAILY")


@dataclass
class SeedResult:
    inserted: dict[str, int] = field(default_factory=dict)
    unchanged: dict[str, int] = field(default_factory=dict)
    differing: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)

    def count(self, bucket: dict[str, int], table: str) -> None:
        bucket[table] = bucket.get(table, 0) + 1


@dataclass(frozen=True)
class EntityMap:
    """Name -> id lookups used by the parsers (loaded before a run; parsers never query the database)."""

    plants: dict[str, int]
    units: dict[tuple[int, str], int]
    unit_capacity: dict[int, object]


def plant_code(station_name: str) -> str:
    code = PLANT_CODE_PREFIX + norm(station_name)
    if len(code) > 80:
        raise ValueError(f"plant code longer than 80 characters: {code!r}")
    return code


def seed_geography(connection: Connection, schema: str, result: SeedResult) -> None:
    for code, name in REGIONS.items():
        _insert_or_compare(connection, result, "region", f"{schema}.region", {"region_code": code},
                           {"region_name": name})
    for code, name, region, _ in STATES:
        _insert_or_compare(connection, result, "state", f"{schema}.state", {"state_code": code},
                           {"state_name": name, "region_code": region})


def _insert_or_compare(connection: Connection, result: SeedResult, label: str, table: str,
                       key: dict[str, object], values: dict[str, object]) -> object | None:
    columns = {**key, **values}
    names = ", ".join(columns)
    params = ", ".join(f":{c}" for c in columns)
    conflict = ", ".join(key)
    returned = connection.execute(text(
        f"INSERT INTO {table} ({names}) VALUES ({params}) ON CONFLICT ({conflict}) DO NOTHING RETURNING 1"),
        columns).scalar_one_or_none()
    if returned:
        result.count(result.inserted, label)
        return None
    where = " AND ".join(f"{c} = :{c}" for c in key)
    stored = connection.execute(text(f"SELECT {', '.join(values)} FROM {table} WHERE {where}"), key).one()
    if tuple(stored) == tuple(values.values()):
        result.count(result.unchanged, label)
    else:
        result.differing.append(f"{label}:{'/'.join(map(str, key.values()))}")
    return stored


def dataset_ids(connection: Connection, schema: str, codes: tuple[str, ...]) -> dict[str, int]:
    rows = connection.execute(text(f"SELECT dataset_code, dataset_id FROM {schema}.source_dataset "
                                   "WHERE dataset_code = ANY(:codes)"), {"codes": list(codes)}).all()
    return {code: dataset_id for code, dataset_id in rows}


def seed_plants(connection: Connection, schema: str, report: Dgr2Report, ids: dict[str, int],
                result: SeedResult) -> None:
    """Plants, units and name mappings from a DGR sub-report 2 (Indian stations only)."""
    missing = [code for code in PLANT_DATASETS if code not in ids]
    if missing:
        raise ValueError(f"datasets not registered: {missing} (run: python -m scripts.register_sources)")
    plant_ids: dict[str, int] = {}
    for station in report.stations:
        if station.foreign:
            result.skipped.append(f"{station.name} (Bhutan import)")
            continue
        code = state_code(station.state_name) if station.state_name else None
        if station.state_name and code is None:
            raise ValueError(f"unknown state name {station.state_name!r} for station {station.name!r}")
        values = {"plant_name": station.name, "state_code": code, "region_code": station.region_code,
                  "plant_type": station.plant_type}
        _insert_or_compare(connection, result, "plant", f"{schema}.plant", {"plant_code": plant_code(station.name)},
                           values)
        plant_ids[norm(station.name)] = connection.execute(
            text(f"SELECT plant_id FROM {schema}.plant WHERE plant_code = :c"),
            {"c": plant_code(station.name)}).scalar_one()
    for unit in report.units:
        plant_id = plant_ids.get(norm(unit.station))
        if plant_id is None:
            continue
        _insert_or_compare(connection, result, "generating_unit", f"{schema}.generating_unit",
                           {"plant_id": plant_id, "unit_code": unit.label},
                           {"unit_name": f"{unit.station} Unit {unit.label}", "unit_capacity_mw": unit.capacity_mw})
    for dataset_code in PLANT_DATASETS:
        for name, plant_id in plant_ids.items():
            stored = connection.execute(text(
                f"SELECT plant_id FROM {schema}.source_entity_mapping "
                "WHERE dataset_id = :d AND source_entity_name = :n"), {"d": ids[dataset_code], "n": name}).all()
            if not stored:
                connection.execute(text(
                    f"INSERT INTO {schema}.source_entity_mapping (dataset_id, source_entity_name, plant_id, "
                    "mapping_confidence) VALUES (:d, :n, :p, 1.00)"), {"d": ids[dataset_code], "n": name, "p": plant_id})
                result.count(result.inserted, "source_entity_mapping")
            elif any(row[0] == plant_id for row in stored):
                result.count(result.unchanged, "source_entity_mapping")
            else:
                result.differing.append(f"source_entity_mapping:{dataset_code}/{name}")


def seed_reference(connection: Connection, schema: str, report: Dgr2Report | None) -> SeedResult:
    """Regions and states always; plants, units and mappings when a sub-report 2 is given."""
    result = SeedResult()
    seed_geography(connection, schema, result)
    if report is not None:
        seed_plants(connection, schema, report, dataset_ids(connection, schema, PLANT_DATASETS), result)
    return result


def load_entities(connection: Connection, schema: str, dataset_code: str) -> EntityMap:
    plants = {name: plant_id for name, plant_id in connection.execute(text(
        f"SELECT m.source_entity_name, m.plant_id FROM {schema}.source_entity_mapping m "
        f"JOIN {schema}.source_dataset d ON d.dataset_id = m.dataset_id "
        "WHERE d.dataset_code = :code AND m.plant_id IS NOT NULL"), {"code": dataset_code})}
    units, capacity = {}, {}
    for unit_id, plant_id, unit_code, unit_capacity in connection.execute(text(
            f"SELECT unit_id, plant_id, unit_code, unit_capacity_mw FROM {schema}.generating_unit")):
        units[(plant_id, unit_code)] = unit_id
        capacity[unit_id] = unit_capacity
    return EntityMap(plants, units, capacity)


def missing_geography(connection: Connection, schema: str) -> list[str]:
    present_regions = {r[0] for r in connection.execute(text(f"SELECT region_code FROM {schema}.region"))}
    present_states = {r[0] for r in connection.execute(text(f"SELECT state_code FROM {schema}.state"))}
    return ([f"region:{c}" for c in REGIONS if c not in present_regions]
            + [f"state:{c}" for c, *_ in STATES if c not in present_states])
