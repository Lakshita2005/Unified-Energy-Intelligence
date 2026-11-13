"""PostgreSQL health check for the Price Intelligence database.

Runs entirely inside a read-only transaction and checks that:

1. a connection can be made using the DB_* settings from .env;
2. the connected database and user match DB_NAME and DB_USER;
3. the target schema (DB_SCHEMA, default ``pi``) exists and is usable;
4. every table in config/expected_schema.py exists in that schema.

It then reports every table found, with its column count. Missing tables are
reported, never created. Passwords are never printed or logged.

Usage (from the project root)::

    python -m scripts.db_health_check          # human-readable report
    python -m scripts.db_health_check --json   # JSON report on stdout

Exit codes: 0 healthy, 1 unhealthy, 2 configuration error.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence

from config.expected_schema import EXPECTED_TABLES
from config.settings import Settings, get_settings
from src.common.database import DatabaseManager
from src.common.db_health import HealthReport, run_health_check
from src.common.exceptions import ConfigurationError
from src.common.logging_config import get_logger, setup_logging

logger = get_logger("scripts.db_health_check")

EXIT_HEALTHY = 0
EXIT_UNHEALTHY = 1
EXIT_CONFIG_ERROR = 2


def format_report(report: HealthReport) -> str:
    """Render a health report as human-readable text."""
    lines = ["PostgreSQL health check", "=" * 60]

    def check(passed: bool, label: str) -> None:
        lines.append(f"[{'PASS' if passed else 'FAIL'}] {label}")

    info = report.connection
    if info is not None:
        address = f"{info.server_address or 'local socket'}:{info.server_port or '-'}"
        check(True, f"Connected to PostgreSQL {info.server_version} at {address}")
        lines.append(f"       database: {info.database}")
        lines.append(f"       user:     {info.current_user}")
    else:
        check(False, "Connection")

    if report.connected:
        check(report.schema_exists, f"Schema '{report.schema}' exists")
    if report.schema_exists:
        check(report.schema_usable, f"USAGE privilege on schema '{report.schema}'")

    verification = report.table_verification
    if verification is not None:
        expected_count = len(verification.present) + len(verification.missing)
        check(
            verification.ok,
            f"Expected tables present: {len(verification.present)}/{expected_count}",
        )
        for table in verification.missing:
            lines.append(f"       MISSING: {report.schema}.{table}")
        if verification.unexpected:
            lines.append(f"[WARN] Tables not in expected list: {', '.join(verification.unexpected)}")

    if report.tables:
        lines.append("")
        lines.append(f"Tables in schema '{report.schema}' ({len(report.tables)}):")
        width = max(len(name) for name in report.tables)
        for name, column_count in report.tables.items():
            lines.append(f"  {name:<{width}}  {column_count:>3} columns")
    if report.views:
        lines.append(f"Views in schema '{report.schema}': {', '.join(report.views)}")

    if report.errors:
        lines.append("")
        lines.append("Errors:")
        lines.extend(f"  - {error}" for error in report.errors)

    lines.append("")
    lines.append(f"Overall: {'HEALTHY' if report.ok else 'UNHEALTHY'}")
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None, *, settings: Settings | None = None) -> int:
    """Run the health check and return a process exit code."""
    parser = argparse.ArgumentParser(description="Read-only PostgreSQL health check.")
    parser.add_argument("--json", action="store_true", help="print the report as JSON")
    args = parser.parse_args(argv)

    settings = settings or get_settings()
    # Keep stdout for the report; diagnostics go to stderr and the log file.
    setup_logging(settings, stream=sys.stderr)

    try:
        manager = DatabaseManager.from_settings(settings)
    except ConfigurationError as exc:
        logger.error("%s", exc)
        return EXIT_CONFIG_ERROR

    logger.info("Running health check against %s", manager.url.render_as_string(hide_password=True))
    try:
        report = run_health_check(
            manager,
            schema=settings.db_schema,
            expected_tables=EXPECTED_TABLES,
            expected_database=settings.db_name,
            expected_user=settings.db_user,
        )
    finally:
        manager.dispose()

    print(json.dumps(report.to_dict(), indent=2) if args.json else format_report(report))
    return EXIT_HEALTHY if report.ok else EXIT_UNHEALTHY


if __name__ == "__main__":
    sys.exit(main())
