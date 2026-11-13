"""Idempotent registration of the source/dataset catalogue.

Upserts :mod:`src.ingestion.catalog` into ``pi.data_source`` and
``pi.source_dataset`` using ``INSERT ... ON CONFLICT DO UPDATE`` on the
existing unique keys (``source_code``, ``dataset_code``), so records are never
duplicated. Rows are only rewritten when a catalogued value actually differs.
``is_active`` and ``created_at`` are never touched, and nothing is deleted.

Unlike pipelines (which never modify existing registrations), this module is
the catalogue owner: catalogue changes are applied as updates. A dataset is
never silently moved to a different source; that raises instead.
"""

from __future__ import annotations

import enum
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field

from sqlalchemy import inspect, text
from sqlalchemy.engine import Connection

from src.common.exceptions import ConfigurationError
from src.common.logging_config import get_logger
from src.ingestion.catalog import COMMON_TABLES, CatalogDataset
from src.ingestion.models import SourceDefinition

logger = get_logger(__name__)


class Action(str, enum.Enum):
    INSERTED = "inserted"
    UPDATED = "updated"
    UNCHANGED = "unchanged"


@dataclass(frozen=True)
class Outcome:
    code: str
    record_id: int
    action: Action


@dataclass
class RegistrationReport:
    sources: list[Outcome] = field(default_factory=list)
    datasets: list[Outcome] = field(default_factory=list)

    def count(self, action: Action) -> int:
        return sum(1 for outcome in self.sources + self.datasets if outcome.action is action)


def _action(inserted: bool | None) -> Action:
    if inserted is None:
        return Action.UNCHANGED
    return Action.INSERTED if inserted else Action.UPDATED


def upsert_source(connection: Connection, schema: str, source: SourceDefinition) -> Outcome:
    """Insert or update one ``data_source`` row, keyed on ``source_code``."""
    row = connection.execute(
        text(
            f"INSERT INTO {schema}.data_source AS t (source_code, source_name, source_url) "
            "VALUES (:code, :name, :url) "
            "ON CONFLICT (source_code) DO UPDATE SET source_name = EXCLUDED.source_name, "
            "source_url = EXCLUDED.source_url "
            "WHERE (t.source_name, t.source_url) IS DISTINCT FROM (EXCLUDED.source_name, EXCLUDED.source_url) "
            "RETURNING source_id, (xmax = 0) AS inserted"
        ),
        {"code": source.code, "name": source.name, "url": source.url},
    ).one_or_none()
    if row is None:  # conflict with identical values: nothing written
        source_id = connection.execute(
            text(f"SELECT source_id FROM {schema}.data_source WHERE source_code = :code"), {"code": source.code}
        ).scalar_one()
        return Outcome(source.code, int(source_id), Action.UNCHANGED)
    return Outcome(source.code, int(row.source_id), _action(row.inserted))


def upsert_dataset(connection: Connection, schema: str, source_id: int, entry: CatalogDataset) -> Outcome:
    """Insert or update one ``source_dataset`` row, keyed on ``dataset_code``.

    Raises:
        ConfigurationError: If the code is already registered under another source.
    """
    dataset = entry.definition
    existing_source = connection.execute(
        text(f"SELECT source_id FROM {schema}.source_dataset WHERE dataset_code = :code"), {"code": dataset.code}
    ).scalar_one_or_none()
    if existing_source is not None and existing_source != source_id:
        raise ConfigurationError(
            f"Dataset {dataset.code!r} is registered under source_id={existing_source}, but the catalogue "
            f"assigns it to {entry.source_code!r} (source_id={source_id}). Reassigning requires a deliberate fix."
        )
    row = connection.execute(
        text(
            f"INSERT INTO {schema}.source_dataset AS t (source_id, dataset_code, dataset_name, endpoint_or_report, "
            "native_granularity, expected_frequency, default_timezone) "
            "VALUES (:source_id, :code, :name, :endpoint, :granularity, :frequency, :tz) "
            "ON CONFLICT (dataset_code) DO UPDATE SET dataset_name = EXCLUDED.dataset_name, "
            "endpoint_or_report = EXCLUDED.endpoint_or_report, native_granularity = EXCLUDED.native_granularity, "
            "expected_frequency = EXCLUDED.expected_frequency, default_timezone = EXCLUDED.default_timezone "
            "WHERE (t.dataset_name, t.endpoint_or_report, t.native_granularity, t.expected_frequency, "
            "t.default_timezone) IS DISTINCT FROM (EXCLUDED.dataset_name, EXCLUDED.endpoint_or_report, "
            "EXCLUDED.native_granularity, EXCLUDED.expected_frequency, EXCLUDED.default_timezone) "
            "RETURNING dataset_id, (xmax = 0) AS inserted"
        ),
        {"source_id": source_id, "code": dataset.code, "name": dataset.name,
         "endpoint": dataset.endpoint_or_report, "granularity": dataset.native_granularity,
         "frequency": dataset.expected_frequency, "tz": dataset.default_timezone},
    ).one_or_none()
    if row is None:
        dataset_id = connection.execute(
            text(f"SELECT dataset_id FROM {schema}.source_dataset WHERE dataset_code = :code"),
            {"code": dataset.code},
        ).scalar_one()
        return Outcome(dataset.code, int(dataset_id), Action.UNCHANGED)
    return Outcome(dataset.code, int(row.dataset_id), _action(row.inserted))


def register_catalog(
    connection: Connection,
    schema: str,
    sources: Mapping[str, SourceDefinition],
    datasets: Iterable[CatalogDataset],
) -> RegistrationReport:
    """Upsert all sources, then all datasets, within the caller's transaction."""
    report = RegistrationReport()
    source_ids: dict[str, int] = {}
    for source in sources.values():
        outcome = upsert_source(connection, schema, source)
        source_ids[source.code] = outcome.record_id
        report.sources.append(outcome)
        logger.info("Source %-12s source_id=%-4d %s", source.code, outcome.record_id, outcome.action.value)
    for entry in datasets:
        if entry.source_code not in source_ids:
            raise ConfigurationError(f"Dataset {entry.definition.code!r} references unknown source {entry.source_code!r}")
        outcome = upsert_dataset(connection, schema, source_ids[entry.source_code], entry)
        report.datasets.append(outcome)
        logger.info("Dataset %-26s dataset_id=%-4d %s", entry.definition.code, outcome.record_id, outcome.action.value)
    return report


def verify_catalog(
    connection: Connection,
    schema: str,
    sources: Mapping[str, SourceDefinition],
    datasets: Iterable[CatalogDataset],
) -> list[str]:
    """Compare the database with the catalogue. Returns a list of problems (empty if consistent)."""
    problems: list[str] = []
    db_sources = {
        row.source_code: row
        for row in connection.execute(text(f"SELECT source_id, source_code, source_name, source_url, is_active "
                                           f"FROM {schema}.data_source"))
    }
    for source in sources.values():
        row = db_sources.get(source.code)
        if row is None:
            problems.append(f"Source {source.code} is not registered")
        elif (row.source_name, row.source_url) != (source.name, source.url):
            problems.append(f"Source {source.code} differs from the catalogue")
        elif not row.is_active:
            problems.append(f"Source {source.code} is inactive")

    db_datasets = {
        row.dataset_code: row
        for row in connection.execute(text(
            f"SELECT d.dataset_code, s.source_code, d.dataset_name, d.endpoint_or_report, d.native_granularity, "
            f"d.expected_frequency, d.default_timezone, d.is_active FROM {schema}.source_dataset d "
            f"JOIN {schema}.data_source s ON s.source_id = d.source_id"))
    }
    tables = set(inspect(connection).get_table_names(schema=schema))
    for entry in datasets:
        dataset = entry.definition
        row = db_datasets.get(dataset.code)
        expected = (entry.source_code, dataset.name, dataset.endpoint_or_report, dataset.native_granularity,
                    dataset.expected_frequency, dataset.default_timezone)
        if row is None:
            problems.append(f"Dataset {dataset.code} is not registered")
        elif (row.source_code, row.dataset_name, row.endpoint_or_report, row.native_granularity,
              row.expected_frequency, row.default_timezone) != expected:
            problems.append(f"Dataset {dataset.code} differs from the catalogue")
        elif not row.is_active:
            problems.append(f"Dataset {dataset.code} is inactive")
        for table in (*entry.destination_tables, *COMMON_TABLES):
            if table not in tables:
                problems.append(f"Dataset {dataset.code}: destination table {schema}.{table} does not exist")

    duplicates = connection.execute(text(
        f"SELECT 'source ' || source_code FROM {schema}.data_source GROUP BY source_code HAVING count(*) > 1 "
        f"UNION ALL SELECT 'dataset ' || dataset_code FROM {schema}.source_dataset GROUP BY dataset_code "
        "HAVING count(*) > 1")).scalars().all()
    problems.extend(f"Duplicate {item}" for item in duplicates)
    return problems
