"""Tests for src.common.database (using SQLite; no PostgreSQL required)."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy import Integer, String, select, text
from sqlalchemy.orm import Mapped, mapped_column

from src.common.database import (
    Base,
    DatabaseManager,
    build_connect_args,
    build_database_url,
)
from src.common.exceptions import ConfigurationError, DatabaseError
from tests.conftest import SettingsFactory


class _Item(Base):
    __tablename__ = "test_items"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(50))


@pytest.fixture
def sqlite_manager(tmp_path: Path) -> Iterator[DatabaseManager]:
    manager = DatabaseManager(f"sqlite:///{tmp_path / 'test.db'}")
    Base.metadata.create_all(manager.engine, tables=[_Item.__table__])
    yield manager
    manager.dispose()


def test_build_database_url_reports_all_missing_variables(make_settings: SettingsFactory) -> None:
    settings = make_settings(db_host=None, db_name=None, db_user=None)

    with pytest.raises(ConfigurationError) as excinfo:
        build_database_url(settings)

    message = str(excinfo.value)
    assert "DB_HOST" in message and "DB_NAME" in message and "DB_USER" in message


def test_build_database_url_escapes_special_characters(make_settings: SettingsFactory) -> None:
    settings = make_settings(
        db_host="localhost", db_name="prices", db_user="ingest", db_password="p@ss:w/rd#1", db_port=6543
    )

    url = build_database_url(settings)

    assert url.drivername == "postgresql+psycopg"
    assert url.password == "p@ss:w/rd#1"
    assert url.port == 6543
    assert "p@ss" not in url.render_as_string(hide_password=True)


def test_build_database_url_allows_missing_password(make_settings: SettingsFactory) -> None:
    settings = make_settings(db_host="localhost", db_name="prices", db_user="ingest", db_password=None)

    assert build_database_url(settings).password is None


def test_build_connect_args_includes_sslmode_only_when_set(make_settings: SettingsFactory) -> None:
    assert "sslmode" not in build_connect_args(make_settings())
    assert build_connect_args(make_settings(db_sslmode="require"))["sslmode"] == "require"


def test_engine_is_created_lazily(tmp_path: Path) -> None:
    manager = DatabaseManager(f"sqlite:///{tmp_path / 'lazy.db'}")

    assert manager._engine is None
    assert manager.check_connection() is True
    assert manager._engine is not None
    manager.dispose()


def test_session_scope_commits_on_success(sqlite_manager: DatabaseManager) -> None:
    with sqlite_manager.session_scope() as session:
        session.add(_Item(id=1, name="first"))

    with sqlite_manager.session_scope() as session:
        assert session.scalars(select(_Item.name)).all() == ["first"]


def test_session_scope_rolls_back_on_error(sqlite_manager: DatabaseManager) -> None:
    with pytest.raises(RuntimeError):
        with sqlite_manager.session_scope() as session:
            session.add(_Item(id=1, name="discarded"))
            session.flush()
            raise RuntimeError("boom")

    with sqlite_manager.session_scope() as session:
        assert session.scalars(select(_Item)).all() == []


def test_connect_runs_in_transaction(sqlite_manager: DatabaseManager) -> None:
    with sqlite_manager.connect() as conn:
        conn.execute(text("INSERT INTO test_items (id, name) VALUES (1, 'core')"))

    with sqlite_manager.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM test_items")).scalar_one() == 1


def test_check_connection_failure(tmp_path: Path) -> None:
    # A path inside a non-existent directory cannot be opened by SQLite.
    manager = DatabaseManager(f"sqlite:///{tmp_path / 'missing' / 'dir' / 'x.db'}")

    assert manager.check_connection() is False
    with pytest.raises(DatabaseError):
        manager.check_connection(raise_on_error=True)
    manager.dispose()
