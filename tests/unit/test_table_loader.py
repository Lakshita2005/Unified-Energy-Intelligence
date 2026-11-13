"""Tests for src.loaders.table_loader using SQLite (no PostgreSQL required)."""

from __future__ import annotations

from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import Column, Integer, MetaData, Numeric, String, Table, select

from src.common.database import DatabaseManager
from src.ingestion.models import Severity
from src.loaders.table_loader import ConflictPolicy, RowToLoad, TableLoader

pytestmark = pytest.mark.filterwarnings("ignore:Dialect sqlite\\+pysqlite does \\*not\\* support Decimal")


@pytest.fixture
def db(tmp_path: Path) -> Iterator[DatabaseManager]:
    manager = DatabaseManager(f"sqlite:///{tmp_path / 'loader.db'}")
    metadata = MetaData()
    Table(
        "prices", metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("market", String(20), nullable=False),
        Column("block", Integer, nullable=False),
        Column("price", Numeric(16, 4), nullable=False),
        Column("ingestion_run_id", Integer, nullable=False),
    )
    metadata.create_all(manager.engine)
    yield manager
    manager.dispose()


def _row(block: int, price: str, run: int = 1, market: str = "DAM") -> RowToLoad:
    return RowToLoad(f"{market}|{block}", {"market": market, "block": block, "price": Decimal(price),
                                           "ingestion_run_id": run}, raw={"block": block, "price": price})


def _load(db: DatabaseManager, rows: list[RowToLoad], policy: ConflictPolicy = ConflictPolicy.VERSION):
    loader = TableLoader("prices", natural_key=("market", "block"), conflict_policy=policy)
    with db.connect() as conn:
        return loader.load(conn, rows)


def _all(db: DatabaseManager) -> list[tuple[int, Decimal, int]]:
    with db.connect() as conn:
        table = Table("prices", MetaData(), autoload_with=conn)
        return [tuple(r) for r in conn.execute(
            select(table.c.block, table.c.price, table.c.ingestion_run_id).order_by(table.c.id))]


def test_first_load_inserts_everything(db: DatabaseManager) -> None:
    result = _load(db, [_row(1, "10"), _row(2, "20")])

    assert (result.inserted, result.unchanged, result.rejected) == (2, 0, 0)
    assert len(_all(db)) == 2


def test_reloading_identical_rows_is_idempotent(db: DatabaseManager) -> None:
    _load(db, [_row(1, "10"), _row(2, "20")])

    result = _load(db, [_row(1, "10.0000", run=2), _row(2, "20", run=2)])

    assert (result.inserted, result.unchanged) == (0, 2)
    assert len(_all(db)) == 2


def test_version_policy_keeps_history(db: DatabaseManager) -> None:
    _load(db, [_row(1, "10")])

    result = _load(db, [_row(1, "11", run=2)])

    assert (result.inserted, result.versioned) == (1, 1)
    assert _all(db) == [(1, Decimal("10"), 1), (1, Decimal("11"), 2)]
    # A third load with the latest values is unchanged.
    assert _load(db, [_row(1, "11", run=3)]).unchanged == 1


def test_update_policy_updates_in_place(db: DatabaseManager) -> None:
    _load(db, [_row(1, "10")])

    result = _load(db, [_row(1, "11", run=2)], ConflictPolicy.UPDATE)

    assert (result.inserted, result.updated) == (0, 1)
    assert _all(db) == [(1, Decimal("11"), 2)]


def test_reject_policy_reports_conflict(db: DatabaseManager) -> None:
    _load(db, [_row(1, "10")])

    result = _load(db, [_row(1, "11", run=2)], ConflictPolicy.REJECT)

    assert result.rejected == 1 and result.issues[0].issue_code == "CONFLICTING_VALUE"
    assert result.issues[0].raw == {"block": 1, "price": "11"}
    assert _all(db) == [(1, Decimal("10"), 1)]


def test_in_batch_duplicates(db: DatabaseManager) -> None:
    result = _load(db, [_row(1, "10"), _row(1, "10.00"), _row(2, "20"), _row(2, "21")])

    codes = sorted((i.issue_code, i.severity) for i in result.issues)
    assert codes == [("CONFLICTING_DUPLICATE_IN_BATCH", Severity.ERROR)] * 2 + [("DUPLICATE_IN_BATCH", Severity.INFO)]
    assert result.inserted == 1 and result.rejected == 2
    assert _all(db) == [(1, Decimal("10"), 1)]


def test_missing_natural_key_is_rejected(db: DatabaseManager) -> None:
    row = RowToLoad("bad", {"market": "DAM", "block": None, "price": Decimal("1"), "ingestion_run_id": 1})

    result = _load(db, [row])

    assert result.rejected == 1 and result.issues[0].issue_code == "MISSING_NATURAL_KEY"


def test_precision_loss_is_flagged_and_does_not_create_versions(db: DatabaseManager) -> None:
    first = _load(db, [_row(1, "10.12345")])
    second = _load(db, [_row(1, "10.12345", run=2)])

    assert first.issues[0].issue_code == "NUMERIC_PRECISION_LOSS"
    assert first.issues[0].severity is Severity.WARNING
    assert second.unchanged == 1 and second.inserted == 0


def test_unknown_columns_raise_and_roll_back(db: DatabaseManager) -> None:
    rows = [_row(1, "10"), RowToLoad("x", {"market": "DAM", "block": 2, "price": 1, "ingestion_run_id": 1,
                                           "bogus": 1})]

    with pytest.raises(ValueError, match="bogus"):
        _load(db, rows)
    assert _all(db) == []


# ------------------------------------------------- nullable key columns


@pytest.fixture
def geo_db(tmp_path: Path) -> Iterator[DatabaseManager]:
    manager = DatabaseManager(f"sqlite:///{tmp_path / 'geo.db'}")
    metadata = MetaData()
    Table(
        "readings", metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("series", String(20), nullable=False),
        Column("ts", Integer, nullable=False),
        Column("state", String(10), nullable=True),
        Column("region", String(10), nullable=True),
        Column("value", Numeric(18, 4), nullable=False),
        Column("ingestion_run_id", Integer, nullable=False),
    )
    metadata.create_all(manager.engine)
    yield manager
    manager.dispose()


def _reading(ts: int, value: str, *, state: str | None = None, region: str | None = None, run: int = 1) -> RowToLoad:
    return RowToLoad(f"{state}|{region}|{ts}", {"series": "DEMAND", "ts": ts, "state": state, "region": region,
                                                "value": Decimal(value), "ingestion_run_id": run})


def _load_geo(db: DatabaseManager, rows: list[RowToLoad], nullable: tuple[str, ...] = ("state", "region")):
    loader = TableLoader("readings", natural_key=("series", "ts", "state", "region"), nullable_key_columns=nullable)
    with db.connect() as conn:
        return loader.load(conn, rows)


def _geo_rows(db: DatabaseManager) -> list[tuple]:
    with db.connect() as conn:
        table = Table("readings", MetaData(), autoload_with=conn)
        return [tuple(r) for r in conn.execute(
            select(table.c.ts, table.c.state, table.c.region, table.c.value, table.c.ingestion_run_id)
            .order_by(table.c.id))]


def test_null_geography_reload_is_idempotent(geo_db: DatabaseManager) -> None:
    first = _load_geo(geo_db, [_reading(1, "100"), _reading(2, "110")])
    second = _load_geo(geo_db, [_reading(1, "100", run=2), _reading(2, "110", run=2)])

    assert (first.inserted, first.rejected) == (2, 0)
    assert (second.inserted, second.unchanged) == (0, 2)
    assert len(_geo_rows(geo_db)) == 2


def test_null_geography_changed_value_is_versioned(geo_db: DatabaseManager) -> None:
    _load_geo(geo_db, [_reading(1, "100")])
    result = _load_geo(geo_db, [_reading(1, "105", run=2), _reading(2, "110", run=2)])

    assert (result.inserted, result.versioned, result.unchanged) == (2, 1, 0)
    assert _geo_rows(geo_db) == [(1, None, None, Decimal("100"), 1), (1, None, None, Decimal("105"), 2),
                                 (2, None, None, Decimal("110"), 2)]


def test_null_and_non_null_geography_are_different_keys(geo_db: DatabaseManager) -> None:
    _load_geo(geo_db, [_reading(1, "100"), _reading(1, "200", region="NR"), _reading(1, "50", state="DL", region="NR")])
    result = _load_geo(geo_db, [_reading(1, "100", run=2), _reading(1, "200", region="NR", run=2),
                                _reading(1, "50", state="DL", region="NR", run=2), _reading(1, "70", state="DL")])

    assert (result.inserted, result.unchanged) == (1, 3)
    assert len(_geo_rows(geo_db)) == 4


def test_null_geography_in_batch_duplicates(geo_db: DatabaseManager) -> None:
    result = _load_geo(geo_db, [_reading(1, "100"), _reading(1, "100"), _reading(2, "1"), _reading(2, "2")])

    assert result.inserted == 1 and result.unchanged == 1 and result.rejected == 2
    assert _geo_rows(geo_db) == [(1, None, None, Decimal("100"), 1)]


def test_null_in_non_nullable_key_column_is_still_rejected(geo_db: DatabaseManager) -> None:
    row = RowToLoad("x", {"series": "DEMAND", "ts": None, "state": None, "region": None, "value": Decimal("1"),
                          "ingestion_run_id": 1})

    result = _load_geo(geo_db, [row])

    assert result.rejected == 1 and result.issues[0].issue_code == "MISSING_NATURAL_KEY"


def test_null_key_rejected_when_column_not_declared_nullable(geo_db: DatabaseManager) -> None:
    result = _load_geo(geo_db, [_reading(1, "100")], nullable=())

    assert result.rejected == 1 and result.issues[0].issue_code == "MISSING_NATURAL_KEY"


def test_nullable_columns_must_be_part_of_the_key() -> None:
    with pytest.raises(ValueError, match="natural key"):
        TableLoader("readings", natural_key=("series", "ts"), nullable_key_columns=("state",))


def test_null_safe_lookup_respects_chunking(geo_db: DatabaseManager) -> None:
    rows = [_reading(ts, "1") for ts in range(7)]
    loader = TableLoader("readings", natural_key=("series", "ts", "state", "region"),
                         nullable_key_columns=("state", "region"), chunk_size=3)
    with geo_db.connect() as conn:
        loader.load(conn, rows)
    with geo_db.connect() as conn:
        again = loader.load(conn, [_reading(ts, "1", run=2) for ts in range(7)])

    assert (again.inserted, again.unchanged) == (0, 7)
