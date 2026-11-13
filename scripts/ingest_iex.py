"""IEX (Indian Energy Exchange) DAM / GDAM / RTM price ingestion CLI.

Usage (from the project root)::

    # Once: insert the market and bid-area reference rows (idempotent)
    python -m scripts.ingest_iex reference [--dry-run]

    # One delivery day, fully executed then rolled back
    python -m scripts.ingest_iex dam --date 2026-11-10 --dry-run

    # A range of delivery days (inclusive, at most 31 days per command)
    python -m scripts.ingest_iex gdam --start-date 2026-11-01 --end-date 2026-11-10
    python -m scripts.ingest_iex rtm --date 2026-11-10

DAM and GDAM load the unconstrained MCP (area MKT) and 13 bid-area prices; RTM
loads the MCP only. No scheduler is started. Exit codes: 0 SUCCESS, 1
PARTIAL/FAILED, 2 invalid arguments or configuration.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from datetime import date

from config.settings import Settings, get_settings
from src.common.database import DatabaseManager
from src.common.exceptions import ConfigurationError
from src.common.logging_config import get_logger, setup_logging
from src.pipelines.iex import api
from src.pipelines.iex.reference import seed_reference
from src.pipelines.iex.runner import RunSummary, run_iex

logger = get_logger("scripts.ingest_iex")


def _date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid date {value!r}, expected YYYY-MM-DD") from exc


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="IEX DAM / GDAM / RTM 15-minute price ingestion.")
    sub = parser.add_subparsers(dest="command", required=True)
    ref = sub.add_parser("reference", help="insert market_master / market_area reference rows (idempotent)")
    ref.add_argument("--dry-run", action="store_true", help="show what would be inserted, then roll back")
    for kind, help_text in (("dam", "Day-Ahead Market: MCP + 13 bid-area prices"),
                            ("gdam", "Green Day-Ahead Market: unconstrained MCP + 13 bid-area prices"),
                            ("rtm", "Real-Time Market: MCP (no area prices available)")):
        p = sub.add_parser(kind, help=help_text)
        when = p.add_mutually_exclusive_group(required=True)
        when.add_argument("--date", type=_date, help="one delivery day (YYYY-MM-DD)")
        when.add_argument("--start-date", type=_date, help="first delivery day of a range (YYYY-MM-DD)")
        p.add_argument("--end-date", type=_date,
                       help=f"last delivery day, inclusive (with --start-date; at most {api.MAX_DAYS_PER_COMMAND} days)")
        p.add_argument("--dry-run", action="store_true", help="execute fully, then roll back all database changes")
    return parser


def _reference(settings: Settings, dry_run: bool) -> int:
    manager = DatabaseManager.from_settings(settings)
    try:
        with manager.engine.connect() as connection:
            transaction = connection.begin()
            result = seed_reference(connection, settings.db_schema)
            transaction.rollback() if dry_run else transaction.commit()
    finally:
        manager.dispose()
    mode = "DRY RUN (rolled back)" if dry_run else "committed"
    print(f"IEX reference rows - {mode}")
    print(f"  inserted:  {len(result.inserted)} {result.inserted}")
    print(f"  unchanged: {len(result.unchanged)}")
    if result.differing:
        print(f"  existing rows with different values (left unchanged): {result.differing}")
    return 0


def _print_summary(summary: RunSummary) -> None:
    mode = "DRY RUN (rolled back)" if summary.dry_run else "committed"
    print(f"\nIEX {summary.kind.upper()} {summary.start_date}..{summary.end_date} - {mode}")
    if summary.seeded_reference:
        print(f"Reference rows seeded inside the dry run: {len(summary.seeded_reference)}")
    r = summary.result
    if r is None:
        print(f"status {summary.status.value}: {summary.error}")
        return
    print(f"status={r.status.value} run={r.ingestion_run_id} documents={r.documents_processed} "
          f"duplicate_documents={r.duplicate_documents} read={r.records_read} inserted={r.records_inserted} "
          f"unchanged={r.records_unchanged} rejected={r.records_rejected} issues={r.data_quality_issues}")
    if r.error:
        print(f"error: {r.error}")
    if summary.raw_data_dir and summary.dry_run:
        print(f"Dry-run raw files: {summary.raw_data_dir}")


def main(argv: Sequence[str] | None = None, *, settings: Settings | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    if args.command != "reference":
        if args.date is not None and args.end_date is not None:
            parser.error("--end-date requires --start-date, not --date")
        if args.start_date is not None and args.end_date is None:
            parser.error("--start-date requires --end-date")
    settings = settings or get_settings()
    setup_logging(settings, stream=sys.stderr)
    try:
        if args.command == "reference":
            return _reference(settings, args.dry_run)
        start = args.date or args.start_date
        summary = run_iex(args.command, start_date=start, end_date=args.end_date or start, dry_run=args.dry_run,
                          settings=settings)
    except (ValueError, ConfigurationError) as exc:
        logger.error("%s", exc)
        return 2
    _print_summary(summary)
    return 0 if summary.ok else 1


if __name__ == "__main__":
    sys.exit(main())
