"""Reusable, idempotent loading into existing database tables.

:class:`TableLoader` reflects an existing table (it never creates or alters
one) and loads rows identified by a *natural key*, e.g.
``(market_code, area_code, delivery_date, time_block)``.

Most ``pi`` fact tables have no unique constraint on their natural key, so
duplicates cannot be prevented with ``ON CONFLICT``. Instead, inside the
caller's transaction, the loader:

1. takes a PostgreSQL advisory lock for the table, so concurrent loaders of
   the same table are serialised;
2. finds the latest existing row (highest primary key) for each natural key;
3. compares values and, per row, inserts it (new key), skips it (identical),
   or applies the :class:`ConflictPolicy` (values changed).

Duplicate natural keys within one batch are never loaded twice: identical
copies are reported as INFO, conflicting copies are all rejected.

Natural key columns must be non-null unless listed in ``nullable_key_columns``.
Those are matched null-safely (``IS NOT DISTINCT FROM``), so NULL equals NULL;
e.g. NULL ``state_code`` and ``region_code`` meaning All-India.

The loader never deletes rows. It must run inside a transaction (e.g.
``DatabaseManager.connect()``) so a failure rolls back the whole batch.
"""

from __future__ import annotations

import enum
from collections import defaultdict
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any

from sqlalchemy import Column, Float, MetaData, Numeric, Table, insert, select, text, tuple_, update
from sqlalchemy.engine import Connection

from src.common.logging_config import get_logger
from src.ingestion.models import Severity

logger = get_logger(__name__)

# Columns describing *where a row came from* rather than *what it says*.
# Differences in these never make a row "changed".
DEFAULT_LINEAGE_COLUMNS: frozenset[str] = frozenset(
    {"ingestion_run_id", "document_id", "retrieved_at", "source_published_at", "source_available_at"}
)


class ConflictPolicy(str, enum.Enum):
    """What to do when a natural key already exists with different values.

    VERSION: insert a new row linked to the current run; earlier rows are kept
        (full history; matches tables keyed on ``(..., ingestion_run_id)``).
    UPDATE: update the latest existing row in place (counted as updated).
    REJECT: load nothing and report a ``CONFLICTING_VALUE`` error.
    """

    VERSION = "VERSION"
    UPDATE = "UPDATE"
    REJECT = "REJECT"


@dataclass(frozen=True)
class RowToLoad:
    """A row to load, with its source record key and original record for traceability."""

    key: str
    row: dict[str, Any]
    raw: Any = None


@dataclass(frozen=True)
class LoadIssue:
    """A data-quality issue found while loading."""

    key: str
    issue_code: str
    description: str
    severity: Severity
    raw: Any = None


@dataclass
class LoadResult:
    """Outcome of :meth:`TableLoader.load`."""

    inserted: int = 0
    updated: int = 0
    unchanged: int = 0
    versioned: int = 0
    issues: list[LoadIssue] = field(default_factory=list)

    @property
    def rejected(self) -> int:
        """Number of rows not loaded because of an ERROR issue."""
        return sum(1 for issue in self.issues if issue.severity is Severity.ERROR)


def _chunks(items: Sequence[Any], size: int) -> Iterator[Sequence[Any]]:
    for start in range(0, len(items), size):
        yield items[start:start + size]


class TableLoader:
    """Idempotent loader for one existing table.

    Args:
        table_name: Existing table to load into.
        natural_key: Columns that identify a record (non-null unless in ``nullable_key_columns``).
        nullable_key_columns: Natural key columns that may be NULL; matched with
            ``IS NOT DISTINCT FROM`` so a NULL key value matches existing NULLs.
        schema: Schema of the table (``None`` for the default schema).
        conflict_policy: Behaviour when a key exists with different values.
        compare_columns: Columns compared to decide whether a row changed.
            Defaults to every supplied column except the natural key, the
            primary key and ``ignore_columns``.
        ignore_columns: Columns never compared (lineage columns by default).
        chunk_size: Keys per lookup query.
    """

    def __init__(
        self,
        table_name: str,
        *,
        natural_key: Sequence[str],
        nullable_key_columns: Sequence[str] = (),
        schema: str | None = None,
        conflict_policy: ConflictPolicy = ConflictPolicy.VERSION,
        compare_columns: Sequence[str] | None = None,
        ignore_columns: frozenset[str] = DEFAULT_LINEAGE_COLUMNS,
        chunk_size: int = 500,
    ) -> None:
        if not natural_key:
            raise ValueError("natural_key must contain at least one column")
        self.table_name = table_name
        self.schema = schema
        self.natural_key = tuple(natural_key)
        self.nullable_key_columns = tuple(nullable_key_columns)
        if not set(self.nullable_key_columns) <= set(self.natural_key):
            raise ValueError("nullable_key_columns must be natural key columns")
        self.conflict_policy = conflict_policy
        self.compare_columns = tuple(compare_columns) if compare_columns is not None else None
        self.ignore_columns = ignore_columns
        self.chunk_size = chunk_size
        self._table: Table | None = None

    @property
    def qualified_name(self) -> str:
        return f"{self.schema}.{self.table_name}" if self.schema else self.table_name

    # ------------------------------------------------------------- metadata

    def table(self, connection: Connection) -> Table:
        """Reflect (once) and return the target table. Read-only."""
        if self._table is None:
            self._table = Table(self.table_name, MetaData(), schema=self.schema, autoload_with=connection)
            missing = [c for c in self.natural_key if c not in self._table.c]
            if missing:
                raise ValueError(f"Natural key column(s) {missing} not found in {self.qualified_name}")
        return self._table

    def column_names(self, connection: Connection) -> frozenset[str]:
        """Return the column names of the target table."""
        return frozenset(self.table(connection).c.keys())

    @staticmethod
    def _primary_key(table: Table) -> Column[Any]:
        columns = list(table.primary_key.columns)
        if len(columns) != 1:
            raise ValueError(f"{table.fullname} must have a single-column primary key")
        return columns[0]

    # ------------------------------------------------------------ comparison

    @staticmethod
    def _numeric_scale(column: Column[Any]) -> int | None:
        column_type = column.type
        if isinstance(column_type, Numeric) and not isinstance(column_type, Float):
            return column_type.scale
        return None

    def _normalise(self, column: Column[Any], value: Any) -> Any:
        """Normalise a value as the database would store it, for comparison."""
        if value is None or isinstance(value, bool):
            return value
        if isinstance(value, (int, float, Decimal)):
            number = Decimal(str(value))
            scale = self._numeric_scale(column)
            if scale is not None:
                number = number.quantize(Decimal(1).scaleb(-scale), rounding=ROUND_HALF_UP)
            return number
        if isinstance(value, datetime) and value.tzinfo is not None:
            return value.astimezone(timezone.utc)
        return value

    def _columns_to_compare(self, table: Table, row: dict[str, Any]) -> list[str]:
        if self.compare_columns is not None:
            return list(self.compare_columns)
        pk_name = self._primary_key(table).name
        return [
            name for name in row
            if name not in self.natural_key and name not in self.ignore_columns and name != pk_name
        ]

    def _changed_columns(self, table: Table, new: dict[str, Any], existing: dict[str, Any]) -> list[str]:
        return [
            name for name in self._columns_to_compare(table, new)
            if self._normalise(table.c[name], new.get(name)) != self._normalise(table.c[name], existing.get(name))
        ]

    def _precision_issue(self, table: Table, item: RowToLoad) -> LoadIssue | None:
        lossy = []
        for name, value in item.row.items():
            scale = self._numeric_scale(table.c[name])
            if scale is None or value is None or isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
                continue
            try:
                exponent = Decimal(str(value)).normalize().as_tuple().exponent
            except InvalidOperation:
                continue
            if isinstance(exponent, int) and -exponent > scale:
                lossy.append(f"{name}={value} (column scale {scale})")
        if not lossy:
            return None
        return LoadIssue(item.key, "NUMERIC_PRECISION_LOSS",
                         "Value(s) will be rounded by the column type: " + ", ".join(lossy),
                         Severity.WARNING, item.raw)

    # ---------------------------------------------------------------- loading

    def _key_of(self, row: dict[str, Any]) -> tuple[Any, ...] | None:
        key = tuple(row.get(column) for column in self.natural_key)
        if any(value is None and column not in self.nullable_key_columns
               for column, value in zip(self.natural_key, key)):
            return None
        return key

    def _lock(self, connection: Connection) -> None:
        if connection.dialect.name == "postgresql":
            connection.execute(text("SELECT pg_advisory_xact_lock(hashtext(:name))"), {"name": self.qualified_name})

    def _fetch_latest(
        self, connection: Connection, table: Table, keys: list[tuple[Any, ...]]
    ) -> dict[tuple[Any, ...], dict[str, Any]]:
        pk = self._primary_key(table)
        latest: dict[tuple[Any, ...], dict[str, Any]] = {}
        for statement in self._lookup_statements(table, keys):
            for row in connection.execute(statement).mappings():
                key = tuple(row[name] for name in self.natural_key)
                if key not in latest or row[pk.name] > latest[key][pk.name]:
                    latest[key] = dict(row)
        return latest

    def _lookup_statements(self, table: Table, keys: list[tuple[Any, ...]]) -> Iterator[Any]:
        """SELECTs returning existing rows for ``keys``.

        Non-null key columns are matched with a tuple ``IN``. Keys are grouped by
        their nullable-column values, each group adding ``col IS NOT DISTINCT
        FROM value`` conditions, because ``IN`` never matches NULL.
        """
        strict = [name for name in self.natural_key if name not in self.nullable_key_columns]
        strict_positions = [self.natural_key.index(name) for name in strict]
        nullable_positions = [self.natural_key.index(name) for name in self.nullable_key_columns]

        groups: dict[tuple[Any, ...], list[tuple[Any, ...]]] = defaultdict(list)
        for key in keys:
            groups[tuple(key[i] for i in nullable_positions)].append(tuple(key[i] for i in strict_positions))

        for nullable_values, strict_keys in groups.items():
            null_safe = [table.c[name].is_not_distinct_from(value)
                         for name, value in zip(self.nullable_key_columns, nullable_values)]
            if not strict:
                yield select(table).where(*null_safe)
                continue
            strict_columns = [table.c[name] for name in strict]
            for chunk in _chunks(strict_keys, self.chunk_size):
                yield select(table).where(tuple_(*strict_columns).in_(list(chunk)), *null_safe)

    def load(self, connection: Connection, items: Sequence[RowToLoad]) -> LoadResult:
        """Load rows idempotently within the caller's transaction.

        Raises:
            ValueError: If a row contains columns the table does not have
                (a pipeline bug; the caller's transaction should roll back).
        """
        result = LoadResult()
        if not items:
            return result
        table = self.table(connection)
        pk = self._primary_key(table)

        for item in items:
            unknown = set(item.row) - set(table.c.keys())
            if unknown:
                raise ValueError(f"Columns {sorted(unknown)} do not exist in {self.qualified_name}")

        # 1. Group by natural key; detect missing keys and in-batch duplicates.
        groups: dict[tuple[Any, ...], list[RowToLoad]] = defaultdict(list)
        for item in items:
            key = self._key_of(item.row)
            if key is None:
                result.issues.append(LoadIssue(
                    item.key, "MISSING_NATURAL_KEY",
                    f"Natural key column(s) {list(self.natural_key)} must not be null", Severity.ERROR, item.raw))
            else:
                groups[key].append(item)

        candidates: dict[tuple[Any, ...], RowToLoad] = {}
        for key, group in groups.items():
            first = group[0]
            if len(group) == 1:
                candidates[key] = first
            elif all(not self._changed_columns(table, other.row, first.row) for other in group[1:]):
                candidates[key] = first
                for duplicate in group[1:]:
                    result.unchanged += 1
                    result.issues.append(LoadIssue(
                        duplicate.key, "DUPLICATE_IN_BATCH",
                        f"Identical record for natural key {key} appears more than once; loaded once",
                        Severity.INFO, duplicate.raw))
            else:
                for item in group:
                    result.issues.append(LoadIssue(
                        item.key, "CONFLICTING_DUPLICATE_IN_BATCH",
                        f"Natural key {key} appears {len(group)} times with different values; none loaded",
                        Severity.ERROR, item.raw))

        for item in candidates.values():
            issue = self._precision_issue(table, item)
            if issue is not None:
                result.issues.append(issue)

        # 2. Compare with existing rows under a table-level lock.
        self._lock(connection)
        existing = self._fetch_latest(connection, table, list(candidates))

        inserts: list[dict[str, Any]] = []
        updates: list[tuple[Any, dict[str, Any]]] = []
        for key, item in candidates.items():
            current = existing.get(key)
            if current is None:
                inserts.append(item.row)
                continue
            changed = self._changed_columns(table, item.row, current)
            if not changed:
                result.unchanged += 1
            elif self.conflict_policy is ConflictPolicy.VERSION:
                inserts.append(item.row)
                result.versioned += 1
            elif self.conflict_policy is ConflictPolicy.UPDATE:
                updates.append((current[pk.name], item.row))
            else:
                result.issues.append(LoadIssue(
                    item.key, "CONFLICTING_VALUE",
                    f"Existing row {pk.name}={current[pk.name]} has different values for {changed}; not loaded",
                    Severity.ERROR, item.raw))

        # 3. Write. Group inserts by column set so omitted columns keep DB defaults.
        by_columns: dict[frozenset[str], list[dict[str, Any]]] = defaultdict(list)
        for row in inserts:
            by_columns[frozenset(row)].append(row)
        for rows in by_columns.values():
            connection.execute(insert(table), rows)
        result.inserted = len(inserts)

        for pk_value, row in updates:
            values = {name: value for name, value in row.items() if name not in self.natural_key and name != pk.name}
            connection.execute(update(table).where(pk == pk_value).values(**values))
        result.updated = len(updates)

        logger.info(
            "Loaded %s: inserted=%d (new versions=%d) updated=%d unchanged=%d rejected=%d",
            self.qualified_name, result.inserted, result.versioned, result.updated, result.unchanged, result.rejected,
        )
        return result
