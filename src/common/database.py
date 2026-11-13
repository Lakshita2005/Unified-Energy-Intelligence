"""Reusable PostgreSQL connection management built on SQLAlchemy 2.x.

The connection URL is assembled from ``DB_*`` environment variables; no
credentials are hard-coded or defaulted.

Typical usage::

    from src.common.database import get_database_manager

    db = get_database_manager()
    with db.session_scope() as session:   # commits on success, rolls back on error
        session.add(obj)

    with db.connect() as conn:            # Core connection inside a transaction
        conn.execute(text("SELECT 1"))

ORM models should subclass :class:`Base` so Alembic autogenerate can see them.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from functools import lru_cache
from types import TracebackType
from typing import Any

from sqlalchemy import MetaData, create_engine, text
from sqlalchemy.engine import URL, Connection, Engine, make_url
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from config.settings import Settings, get_settings
from src.common.exceptions import ConfigurationError, DatabaseError
from src.common.logging_config import get_logger

logger = get_logger(__name__)

# Deterministic constraint names keep Alembic migrations stable across environments.
NAMING_CONVENTION: dict[str, str] = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    """Declarative base class for all ORM models."""

    metadata = MetaData(naming_convention=NAMING_CONVENTION)


# DB_PASSWORD is deliberately optional so .pgpass / trust / peer auth still work.
_REQUIRED_DB_SETTINGS: dict[str, str] = {
    "db_host": "DB_HOST",
    "db_name": "DB_NAME",
    "db_user": "DB_USER",
}


def build_database_url(settings: Settings | None = None) -> URL:
    """Build a SQLAlchemy URL from settings.

    Uses :meth:`URL.create`, so special characters in the password are escaped
    correctly.

    Raises:
        ConfigurationError: If a required ``DB_*`` variable is not set.
    """
    settings = settings or get_settings()
    missing = [env for field, env in _REQUIRED_DB_SETTINGS.items() if getattr(settings, field) is None]
    if missing:
        raise ConfigurationError(
            f"Missing required database environment variable(s): {', '.join(missing)}. "
            "Set them in the environment or in a .env file (see .env.example)."
        )
    password = settings.db_password.get_secret_value() if settings.db_password else None
    return URL.create(
        drivername=settings.db_driver,
        username=settings.db_user,
        password=password,
        host=settings.db_host,
        port=settings.db_port,
        database=settings.db_name,
    )


def build_connect_args(settings: Settings | None = None) -> dict[str, Any]:
    """Build driver-level connection arguments (libpq options) from settings."""
    settings = settings or get_settings()
    connect_args: dict[str, Any] = {
        "connect_timeout": settings.db_connect_timeout_seconds,
        "application_name": settings.app_name,
    }
    if settings.db_sslmode:
        connect_args["sslmode"] = settings.db_sslmode
    return connect_args


class DatabaseManager:
    """Owns a SQLAlchemy engine and session factory for one database.

    The engine is created lazily on first use, so constructing a manager never
    opens a connection.
    """

    def __init__(
        self,
        url: URL | str,
        *,
        pool_size: int = 5,
        max_overflow: int = 10,
        pool_timeout: int = 30,
        pool_recycle: int = 1800,
        echo: bool = False,
        connect_args: dict[str, Any] | None = None,
    ) -> None:
        self._url: URL = make_url(url) if isinstance(url, str) else url
        self._pool_size = pool_size
        self._max_overflow = max_overflow
        self._pool_timeout = pool_timeout
        self._pool_recycle = pool_recycle
        self._echo = echo
        self._connect_args = connect_args or {}
        self._engine: Engine | None = None
        self._session_factory: sessionmaker[Session] | None = None

    @classmethod
    def from_settings(cls, settings: Settings | None = None) -> DatabaseManager:
        """Create a manager configured entirely from environment settings."""
        settings = settings or get_settings()
        return cls(
            build_database_url(settings),
            pool_size=settings.db_pool_size,
            max_overflow=settings.db_max_overflow,
            pool_timeout=settings.db_pool_timeout_seconds,
            pool_recycle=settings.db_pool_recycle_seconds,
            echo=settings.db_echo,
            connect_args=build_connect_args(settings),
        )

    @property
    def url(self) -> URL:
        """The connection URL. Use ``url.render_as_string(hide_password=True)`` to log it."""
        return self._url

    @property
    def engine(self) -> Engine:
        """The SQLAlchemy engine, created on first access."""
        if self._engine is None:
            self._engine = self._create_engine()
        return self._engine

    @property
    def session_factory(self) -> sessionmaker[Session]:
        """Factory producing ORM sessions bound to :attr:`engine`."""
        if self._session_factory is None:
            self._session_factory = sessionmaker(
                bind=self.engine, autoflush=False, expire_on_commit=False
            )
        return self._session_factory

    def _create_engine(self) -> Engine:
        engine_kwargs: dict[str, Any] = {
            "echo": self._echo,
            "pool_pre_ping": True,
            "connect_args": self._connect_args,
        }
        # SQLite (used in unit tests) does not accept QueuePool sizing options.
        if self._url.get_backend_name() != "sqlite":
            engine_kwargs.update(
                pool_size=self._pool_size,
                max_overflow=self._max_overflow,
                pool_timeout=self._pool_timeout,
                pool_recycle=self._pool_recycle,
            )
        logger.info("Creating database engine for %s", self._url.render_as_string(hide_password=True))
        return create_engine(self._url, **engine_kwargs)

    @contextmanager
    def session_scope(self) -> Iterator[Session]:
        """Provide a transactional ORM session.

        Commits when the block exits normally, rolls back and re-raises on any
        exception, and always closes the session.
        """
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            logger.warning("Database session rolled back due to an error")
            raise
        finally:
            session.close()

    @contextmanager
    def connect(self) -> Iterator[Connection]:
        """Provide a Core connection wrapped in a transaction (commit/rollback automatic)."""
        with self.engine.begin() as connection:
            yield connection

    @contextmanager
    def read_only_connection(self) -> Iterator[Connection]:
        """Provide a Core connection whose transaction cannot modify the database.

        On PostgreSQL the transaction is declared ``READ ONLY``, so any write or
        DDL statement fails. The transaction is always rolled back on exit.
        Use this for inspection and health checks.
        """
        with self.engine.connect() as connection:
            try:
                if connection.dialect.name == "postgresql":
                    connection.execute(text("SET TRANSACTION READ ONLY"))
                yield connection
            finally:
                connection.rollback()

    def check_connection(self, *, raise_on_error: bool = False) -> bool:
        """Run ``SELECT 1`` to verify the database is reachable.

        Args:
            raise_on_error: Raise :class:`DatabaseError` instead of returning False.

        Returns:
            True if the database responded, otherwise False.
        """
        try:
            with self.engine.connect() as connection:
                connection.execute(text("SELECT 1"))
        except SQLAlchemyError as exc:
            logger.error("Database health check failed: %s", exc)
            if raise_on_error:
                raise DatabaseError("Database health check failed") from exc
            return False
        return True

    def dispose(self) -> None:
        """Close all pooled connections. The engine is recreated on next use."""
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None
            self._session_factory = None

    def __enter__(self) -> DatabaseManager:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.dispose()


class SavepointDatabase:
    """Runs each transaction as a SAVEPOINT on one caller-owned connection.

    Used for dry runs and tests: the caller begins an outer transaction and
    rolls it back afterwards, so nothing persists, while each inner
    transaction still commits or rolls back exactly as it would in production.
    Satisfies the same ``connect()`` contract as :class:`DatabaseManager`.
    """

    def __init__(self, connection: Connection) -> None:
        self.connection = connection

    @contextmanager
    def connect(self) -> Iterator[Connection]:
        with self.connection.begin_nested():
            yield self.connection


@lru_cache(maxsize=1)
def get_database_manager() -> DatabaseManager:
    """Return the process-wide :class:`DatabaseManager` built from settings."""
    return DatabaseManager.from_settings()
