"""Alembic migration environment.

The connection URL is built from ``DB_*`` environment variables via
:func:`src.common.database.build_database_url`; nothing is read from alembic.ini.

For autogenerate to detect tables, import every module that defines ORM models
(subclasses of ``src.common.database.Base``) below.
"""

from __future__ import annotations

from alembic import context
from sqlalchemy import create_engine, pool

from config.settings import get_settings
from src.common.database import Base, build_connect_args, build_database_url
from src.common.logging_config import setup_logging

# Import model modules here so their tables register on Base.metadata, e.g.:
# import src.loaders.models  # noqa: F401

settings = get_settings()
setup_logging(settings)

target_metadata = Base.metadata


def include_object(obj: object, name: str | None, type_: str, reflected: bool, compare_to: object) -> bool:
    """Exclude database objects that are not defined as ORM models.

    The ``pi`` schema was created outside Alembic. Without this filter,
    autogenerate would propose dropping every existing table that has no
    corresponding model.
    """
    return not (reflected and compare_to is None)


def run_migrations_offline() -> None:
    """Emit SQL to stdout without connecting (``alembic upgrade head --sql``)."""
    context.configure(
        url=build_database_url(settings).render_as_string(hide_password=True),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        include_object=include_object,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations against the live database."""
    engine = create_engine(
        build_database_url(settings),
        poolclass=pool.NullPool,
        connect_args=build_connect_args(settings),
    )
    try:
        with engine.connect() as connection:
            context.configure(
                connection=connection,
                target_metadata=target_metadata,
                compare_type=True,
                include_object=include_object,
            )
            with context.begin_transaction():
                context.run_migrations()
    finally:
        engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
