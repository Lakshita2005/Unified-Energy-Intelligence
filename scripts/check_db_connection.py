"""Verify PostgreSQL connectivity using the configured DB_* environment variables.

Usage (from the project root)::

    python -m scripts.check_db_connection

Exits with status 0 on success and 1 on failure.
"""

from __future__ import annotations

import sys

from sqlalchemy import text

from src.common.database import DatabaseManager
from src.common.exceptions import ConfigurationError
from src.common.logging_config import get_logger, setup_logging

logger = get_logger("scripts.check_db_connection")


def main() -> int:
    """Run the connectivity check and return a process exit code."""
    setup_logging()
    try:
        manager = DatabaseManager.from_settings()
    except ConfigurationError as exc:
        logger.error("%s", exc)
        return 1

    try:
        if not manager.check_connection():
            return 1
        with manager.connect() as connection:
            version = connection.execute(text("SELECT version()")).scalar_one()
        logger.info("Connected to %s", manager.url.render_as_string(hide_password=True))
        logger.info("Server version: %s", version)
        return 0
    finally:
        manager.dispose()


if __name__ == "__main__":
    sys.exit(main())
