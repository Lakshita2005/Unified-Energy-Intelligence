"""Integration tests against the real PostgreSQL database configured in .env.

Skipped automatically unless DB_HOST, DB_NAME and DB_USER are configured.
Run only these with: ``pytest -m integration``.

These tests never create, modify or drop existing tables. Writes are limited to
session-local TEMP tables, which PostgreSQL drops when the connection closes.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, IntegrityError

from config.expected_schema import EXPECTED_TABLES
from config.settings import Settings
from scripts.db_health_check import EXIT_HEALTHY, main
from src.common.database import DatabaseManager, build_connect_args, build_database_url
from src.common.db_health import run_health_check
from src.common.exceptions import ConfigurationError

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def settings() -> Settings:
    return Settings()


@pytest.fixture(scope="module")
def db(settings: Settings) -> Iterator[DatabaseManager]:
    try:
        manager = DatabaseManager.from_settings(settings)
    except ConfigurationError as exc:
        pytest.skip(f"PostgreSQL not configured: {exc}")
    yield manager
    manager.dispose()


@pytest.fixture
def single_connection_db(settings: Settings, db: DatabaseManager) -> Iterator[DatabaseManager]:
    """A manager with exactly one pooled connection, so TEMP tables persist between sessions."""
    manager = DatabaseManager(
        build_database_url(settings), pool_size=1, max_overflow=0, connect_args=build_connect_args(settings)
    )
    yield manager
    manager.dispose()  # closing the connection drops its TEMP tables


# --- Successful connections ------------------------------------------------


def test_can_connect(db: DatabaseManager) -> None:
    assert db.check_connection(raise_on_error=True) is True


def test_server_is_postgresql(db: DatabaseManager) -> None:
    with db.connect() as conn:
        assert "PostgreSQL" in conn.execute(text("SELECT version()")).scalar_one()


def test_connected_to_configured_database_and_user(db: DatabaseManager, settings: Settings) -> None:
    with db.read_only_connection() as conn:
        database, user = conn.execute(text("SELECT current_database(), current_user")).one()

    assert database == settings.db_name
    assert user == settings.db_user


def test_health_check_passes(db: DatabaseManager, settings: Settings) -> None:
    report = run_health_check(
        db,
        schema=settings.db_schema,
        expected_tables=EXPECTED_TABLES,
        expected_database=settings.db_name,
        expected_user=settings.db_user,
    )

    assert report.errors == []
    assert report.schema_exists and report.schema_usable
    assert report.table_verification is not None
    assert report.table_verification.missing == ()
    assert report.ok


def test_health_check_reports_missing_schema(db: DatabaseManager) -> None:
    report = run_health_check(db, schema="schema_that_does_not_exist", expected_tables=["plant"])

    assert report.connected and not report.schema_exists and not report.ok


def test_health_check_reports_missing_table_without_creating_it(db: DatabaseManager, settings: Settings) -> None:
    ghost = "zz_table_that_should_not_exist"
    report = run_health_check(db, schema=settings.db_schema, expected_tables=[*EXPECTED_TABLES, ghost])

    assert report.table_verification is not None
    assert report.table_verification.missing == (ghost,)
    assert not report.ok
    with db.read_only_connection() as conn:
        exists = conn.execute(text("SELECT to_regclass(:name)"), {"name": f"{settings.db_schema}.{ghost}"}).scalar_one()
    assert exists is None


def test_health_check_script_never_prints_password(
    settings: Settings, db: DatabaseManager, capsys: pytest.CaptureFixture[str]
) -> None:
    if settings.db_password is None:
        pytest.skip("No DB_PASSWORD configured")

    exit_codes = [main([], settings=settings), main(["--json"], settings=settings)]

    captured = capsys.readouterr()
    leaked = settings.db_password.get_secret_value() in captured.out + captured.err
    assert not leaked, "password appeared in health check output"
    assert exit_codes == [EXIT_HEALTHY, EXIT_HEALTHY]


# --- Failed transactions ---------------------------------------------------


def test_failed_session_transaction_is_rolled_back(single_connection_db: DatabaseManager) -> None:
    db = single_connection_db
    with db.session_scope() as session:
        session.execute(text("CREATE TEMP TABLE _pi_tx_test (id int PRIMARY KEY)"))

    with pytest.raises(IntegrityError):
        with db.session_scope() as session:
            session.execute(text("INSERT INTO _pi_tx_test VALUES (1)"))
            session.execute(text("INSERT INTO _pi_tx_test VALUES (1)"))  # duplicate key

    # The first insert was rolled back and the connection is usable again.
    with db.session_scope() as session:
        assert session.execute(text("SELECT count(*) FROM _pi_tx_test")).scalar_one() == 0


def test_failed_core_transaction_is_rolled_back(single_connection_db: DatabaseManager) -> None:
    db = single_connection_db
    with db.connect() as conn:
        conn.execute(text("CREATE TEMP TABLE _pi_core_tx_test (id int)"))

    with pytest.raises(DBAPIError):
        with db.connect() as conn:
            conn.execute(text("INSERT INTO _pi_core_tx_test VALUES (1)"))
            conn.execute(text("SELECT 1 / 0"))

    with db.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM _pi_core_tx_test")).scalar_one() == 0


def test_read_only_connection_rejects_writes(db: DatabaseManager) -> None:
    with pytest.raises(DBAPIError, match="read-only transaction"):
        with db.read_only_connection() as conn:
            conn.execute(text("CREATE TEMP TABLE _pi_ro_test (id int)"))
