"""Read-only PostgreSQL health checks.

Verifies connectivity, the connected database and user, that the target schema
exists and is usable, and that every expected table is present.

All queries run inside :meth:`DatabaseManager.read_only_connection`, a
``READ ONLY`` transaction that is always rolled back, so a health check can
never create, alter or drop database objects. Missing tables are reported,
never created.

Usage::

    report = run_health_check(get_database_manager(), schema="pi",
                              expected_tables=EXPECTED_TABLES)
    if not report.ok:
        ...
"""

from __future__ import annotations

from collections.abc import Collection
from dataclasses import asdict, dataclass, field
from typing import Any

from sqlalchemy import inspect, text
from sqlalchemy.engine import Connection, Inspector
from sqlalchemy.engine.reflection import ObjectKind
from sqlalchemy.exc import SQLAlchemyError

from src.common.database import DatabaseManager
from src.common.logging_config import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class ConnectionInfo:
    """Identity of the live connection. Contains no credentials."""

    database: str
    current_user: str
    server_version: str
    server_address: str | None
    server_port: int | None


@dataclass(frozen=True)
class TableVerification:
    """Comparison of the tables found in a schema with the expected set."""

    present: tuple[str, ...]
    missing: tuple[str, ...]
    unexpected: tuple[str, ...]

    @property
    def ok(self) -> bool:
        """True when no expected table is missing (extra tables are allowed)."""
        return not self.missing


@dataclass
class HealthReport:
    """Result of :func:`run_health_check`.

    Attributes:
        schema: Schema that was checked.
        connected: Whether a connection was established.
        connection: Database/user/server details, if connected.
        schema_exists: Whether the schema exists.
        schema_usable: Whether the current user has USAGE on the schema.
        tables: Table name -> column count for every table in the schema.
        views: View names in the schema.
        table_verification: Expected-vs-actual table comparison.
        errors: Problems that make the database unhealthy.
    """

    schema: str
    connected: bool = False
    connection: ConnectionInfo | None = None
    schema_exists: bool = False
    schema_usable: bool = False
    tables: dict[str, int] = field(default_factory=dict)
    views: list[str] = field(default_factory=list)
    table_verification: TableVerification | None = None
    errors: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        """True only if every check passed."""
        return (
            self.connected
            and self.schema_exists
            and self.schema_usable
            and self.table_verification is not None
            and self.table_verification.ok
            and not self.errors
        )

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serialisable representation, including ``ok`` flags."""
        data = asdict(self)
        data["ok"] = self.ok
        if self.table_verification is not None:
            data["table_verification"]["ok"] = self.table_verification.ok
        return data


def get_connection_info(connection: Connection) -> ConnectionInfo:
    """Return the current database, user and server details."""
    row = connection.execute(
        text(
            "SELECT current_database(), current_user, current_setting('server_version'), "
            "host(inet_server_addr()), inet_server_port()"
        )
    ).one()
    return ConnectionInfo(
        database=row[0],
        current_user=row[1],
        server_version=row[2],
        server_address=row[3],
        server_port=row[4],
    )


def schema_is_usable(connection: Connection, schema: str) -> bool:
    """Return True if the current user has USAGE privilege on ``schema``."""
    return bool(
        connection.execute(text("SELECT has_schema_privilege(:schema, 'USAGE')"), {"schema": schema}).scalar_one()
    )


def get_table_column_counts(inspector: Inspector, schema: str) -> dict[str, int]:
    """Return ``{table_name: column_count}`` for all tables in ``schema``."""
    columns = inspector.get_multi_columns(schema=schema, kind=ObjectKind.TABLE)
    return {table: len(cols) for (_, table), cols in sorted(columns.items())}


def verify_tables(actual: Collection[str], expected: Collection[str]) -> TableVerification:
    """Compare the tables found with the expected set."""
    actual_set, expected_set = set(actual), set(expected)
    return TableVerification(
        present=tuple(sorted(actual_set & expected_set)),
        missing=tuple(sorted(expected_set - actual_set)),
        unexpected=tuple(sorted(actual_set - expected_set)),
    )


def _error_message(exc: SQLAlchemyError) -> str:
    # Prefer the driver's message: SQLAlchemy's wrapper appends SQL and doc links.
    return f"{type(exc).__name__}: {getattr(exc, 'orig', None) or exc}".strip()


def run_health_check(
    manager: DatabaseManager,
    *,
    schema: str,
    expected_tables: Collection[str],
    expected_database: str | None = None,
    expected_user: str | None = None,
) -> HealthReport:
    """Run all read-only checks and return a report. Never raises for database errors.

    Args:
        manager: Database to check.
        schema: Schema expected to contain the tables.
        expected_tables: Table names that must exist in ``schema``.
        expected_database: If given, the connected database must have this name.
        expected_user: If given, the connected user must have this name.
    """
    report = HealthReport(schema=schema)
    try:
        with manager.read_only_connection() as connection:
            report.connected = True
            info = get_connection_info(connection)
            report.connection = info
            logger.info(
                "Connected to database %r as user %r (PostgreSQL %s)",
                info.database, info.current_user, info.server_version,
            )

            if expected_database is not None and info.database != expected_database:
                report.errors.append(
                    f"Connected to database {info.database!r}, expected {expected_database!r}"
                )
            if expected_user is not None and info.current_user != expected_user:
                report.errors.append(f"Connected as user {info.current_user!r}, expected {expected_user!r}")

            inspector = inspect(connection)
            report.schema_exists = inspector.has_schema(schema)
            if not report.schema_exists:
                report.errors.append(f"Schema {schema!r} does not exist")
                logger.error("Schema %r does not exist", schema)
                return report

            report.schema_usable = schema_is_usable(connection, schema)
            if not report.schema_usable:
                report.errors.append(f"User {info.current_user!r} lacks USAGE privilege on schema {schema!r}")
                logger.error("User %r lacks USAGE privilege on schema %r", info.current_user, schema)
                return report

            report.tables = get_table_column_counts(inspector, schema)
            report.views = sorted(inspector.get_view_names(schema=schema))
            verification = verify_tables(report.tables, expected_tables)
            report.table_verification = verification

            for table in verification.missing:
                logger.error("Expected table %s.%s is missing", schema, table)
            if verification.unexpected:
                logger.warning(
                    "Schema %r contains %d table(s) not in the expected list: %s",
                    schema, len(verification.unexpected), ", ".join(verification.unexpected),
                )
    except SQLAlchemyError as exc:
        message = _error_message(exc)
        report.errors.append(message)
        logger.error("Database health check failed: %s", message)

    return report
