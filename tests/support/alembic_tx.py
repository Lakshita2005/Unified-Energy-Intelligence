"""Run Alembic migrations inside a caller-owned transaction (dry runs and rolled-back tests).

Unlike ad-hoc dry-run harnesses, this passes the project's ``Base.metadata`` as
``target_metadata``, so constraint and index names follow ``NAMING_CONVENTION`` exactly as
``alembic upgrade`` through ``migrations/env.py`` does. Nothing is committed here: the caller's
outer transaction decides (tests and dry runs always roll back).
"""

from __future__ import annotations

from alembic.config import Config
from alembic.runtime.environment import EnvironmentContext
from alembic.script import ScriptDirectory
from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import DBAPIError

from config.settings import PROJECT_ROOT
from src.common.database import Base


def _config() -> Config:
    config = Config(str(PROJECT_ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(PROJECT_ROOT / "migrations"))
    return config


def current_revision(connection: Connection) -> str | None:
    """The applied revision, or None when this role may not read it.

    The application role ``pi_app`` has no SELECT on ``public.alembic_version`` (by design). The read runs in a
    savepoint, so a permission error does not abort the caller's outer transaction; callers then treat the
    database as already migrated (fixtures only migrate when they see an older, known revision).
    """
    try:
        with connection.begin_nested():
            return connection.execute(text("SELECT version_num FROM public.alembic_version")).scalar()
    except DBAPIError as exc:
        if getattr(exc.orig, "sqlstate", None) == "42501":          # insufficient_privilege
            return None
        raise


def migrate_in_transaction(connection: Connection, target: str, *, downgrade: bool = False) -> None:
    """Upgrade (or downgrade) to ``target`` on ``connection`` without committing."""
    config = _config()
    script = ScriptDirectory.from_config(config)

    def step(revision: object, context: object) -> list:
        current = context.get_current_revision()  # type: ignore[attr-defined]
        return script._downgrade_revs(target, current) if downgrade else script._upgrade_revs(target, current)

    with EnvironmentContext(config, script, fn=step, destination_rev=target) as environment:
        environment.configure(connection=connection, target_metadata=Base.metadata, transactional_ddl=True)
        environment.run_migrations()
