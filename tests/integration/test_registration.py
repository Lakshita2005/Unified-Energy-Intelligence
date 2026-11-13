"""Integration tests for catalogue registration (rolled back; nothing persists)."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import replace

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Connection

from config.settings import Settings
from src.common.database import DatabaseManager
from src.common.exceptions import ConfigurationError
from src.ingestion.catalog import DATASETS, SOURCES, CatalogDataset
from src.ingestion.models import DatasetDefinition, SourceDefinition
from src.ingestion.registration import Action, register_catalog, upsert_dataset, verify_catalog

pytestmark = pytest.mark.integration


@pytest.fixture
def conn() -> Iterator[Connection]:
    try:
        manager = DatabaseManager.from_settings(Settings())
    except ConfigurationError as exc:
        pytest.skip(f"PostgreSQL not configured: {exc}")
    with manager.engine.connect() as connection:
        outer = connection.begin()
        try:
            yield connection
        finally:
            outer.rollback()
    manager.dispose()


def _counts(conn: Connection) -> tuple[int, int]:
    return (conn.execute(text("SELECT count(*) FROM pi.data_source")).scalar_one(),
            conn.execute(text("SELECT count(*) FROM pi.source_dataset")).scalar_one())


def test_registration_is_idempotent(conn: Connection) -> None:
    register_catalog(conn, "pi", SOURCES, DATASETS)
    counts = _counts(conn)

    second = register_catalog(conn, "pi", SOURCES, DATASETS)

    assert second.count(Action.UNCHANGED) == len(SOURCES) + len(DATASETS)
    assert _counts(conn) == counts
    assert verify_catalog(conn, "pi", SOURCES, DATASETS) == []


def test_new_records_are_inserted_then_unchanged(conn: Connection) -> None:
    source = SourceDefinition("ZZ_TEST_SOURCE", "Test source")
    dataset = CatalogDataset("ZZ_TEST_SOURCE", DatasetDefinition("ZZ_TEST_DATASET", "Test", "ref", "daily", "daily"),
                             ("iex_market_price",))

    first = register_catalog(conn, "pi", {source.code: source}, [dataset])
    second = register_catalog(conn, "pi", {source.code: source}, [dataset])

    assert [o.action for o in first.sources + first.datasets] == [Action.INSERTED, Action.INSERTED]
    assert [o.action for o in second.sources + second.datasets] == [Action.UNCHANGED, Action.UNCHANGED]
    assert first.datasets[0].record_id == second.datasets[0].record_id


def test_changed_catalogue_values_are_updated_in_place(conn: Connection) -> None:
    register_catalog(conn, "pi", SOURCES, DATASETS)
    entry = DATASETS[0]
    changed = replace(entry, definition=replace(entry.definition, expected_frequency="twice daily"))
    counts = _counts(conn)

    report = register_catalog(conn, "pi", SOURCES, [changed])

    assert report.datasets[0].action is Action.UPDATED
    assert _counts(conn) == counts
    frequency = conn.execute(text("SELECT expected_frequency FROM pi.source_dataset WHERE dataset_code = :c"),
                             {"c": entry.definition.code}).scalar_one()
    assert frequency == "twice daily"


def test_is_active_is_never_overwritten(conn: Connection) -> None:
    register_catalog(conn, "pi", SOURCES, DATASETS)
    code = DATASETS[0].definition.code
    conn.execute(text("UPDATE pi.source_dataset SET is_active = false WHERE dataset_code = :c"), {"c": code})

    register_catalog(conn, "pi", SOURCES, DATASETS)

    assert conn.execute(text("SELECT is_active FROM pi.source_dataset WHERE dataset_code = :c"),
                        {"c": code}).scalar_one() is False


def test_dataset_is_never_moved_to_another_source(conn: Connection) -> None:
    register_catalog(conn, "pi", SOURCES, DATASETS)
    other_source_id = conn.execute(text("SELECT source_id FROM pi.data_source WHERE source_code = 'IEX'")).scalar_one()
    cea_entry = next(e for e in DATASETS if e.source_code == "CEA")

    with pytest.raises(ConfigurationError, match="Reassigning"):
        upsert_dataset(conn, "pi", other_source_id, cea_entry)
