"""NPP (National Power Portal) All-India demand and generation ingestion CLI.

Usage (from the project root)::

    # One day, fully executed then rolled back
    python -m scripts.ingest_npp demand --date 2026-11-11 --dry-run

    # One day (committed)
    python -m scripts.ingest_npp generation --date 2026-11-11

    # A range of IST days (inclusive, at most 31 days per command)
    python -m scripts.ingest_npp demand --start-date 2026-10-01 --end-date 2026-10-31

No scheduler is started. Exit codes: 0 every day SUCCESS, 1 any day
PARTIAL/FAILED, 2 invalid arguments or configuration.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from datetime import date

from config.settings import Settings, get_settings
from src.common.exceptions import ConfigurationError
from src.common.logging_config import get_logger, setup_logging
from src.pipelines.npp import api
from src.pipelines.npp.runner import RunSummary, run_npp

logger = get_logger("scripts.ingest_npp")


def _date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid date {value!r}, expected YYYY-MM-DD") from exc


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="NPP All-India real-time demand and generation ingestion.")
    sub = parser.add_subparsers(dest="command", required=True)
    for name, help_text in (("demand", "ingest All-India demand met (demandmet1chartdata)"),
                            ("generation", "ingest All-India generation by source (demandmet2chartdata)")):
        p = sub.add_parser(name, help=help_text)
        when = p.add_mutually_exclusive_group(required=True)
        when.add_argument("--date", type=_date, help="one IST day (YYYY-MM-DD)")
        when.add_argument("--start-date", type=_date, help="first IST day of a range (YYYY-MM-DD)")
        p.add_argument("--end-date", type=_date,
                       help=f"last IST day, inclusive (with --start-date; at most {api.MAX_DAYS_PER_COMMAND} days)")
        p.add_argument("--dry-run", action="store_true", help="execute fully, then roll back all database changes")
    return parser


def _print_summary(summary: RunSummary) -> None:
    mode = "DRY RUN (rolled back)" if summary.dry_run else "committed"
    print(f"\nNPP {summary.kind} ingestion - {mode}")
    print(f"{'date':<11} {'status':<8} {'run':>5} {'read':>6} {'inserted':>9} {'unchanged':>10} {'rejected':>9} "
          f"{'issues':>7} {'dup doc':>8}")
    for outcome in summary.outcomes:
        r = outcome.result
        if r is None:
            print(f"{outcome.day.isoformat():<11} {outcome.status.value:<8} {'-':>5}")
        else:
            print(f"{outcome.day.isoformat():<11} {outcome.status.value:<8} {r.ingestion_run_id:>5} {r.records_read:>6} "
                  f"{r.records_inserted:>9} {r.records_unchanged:>10} {r.records_rejected:>9} "
                  f"{r.data_quality_issues:>7} {r.duplicate_documents:>8}")
        if outcome.error:
            print(f"    error: {outcome.error}")
    if summary.raw_data_dir and summary.dry_run:
        print(f"Dry-run raw files: {summary.raw_data_dir}")


def main(argv: Sequence[str] | None = None, *, settings: Settings | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    if args.date is not None and args.end_date is not None:
        parser.error("--end-date requires --start-date, not --date")
    if args.start_date is not None and args.end_date is None:
        parser.error("--start-date requires --end-date")
    settings = settings or get_settings()
    setup_logging(settings, stream=sys.stderr)
    start = args.date or args.start_date
    try:
        summary = run_npp(args.command, start_date=start, end_date=args.end_date or start, dry_run=args.dry_run,
                          settings=settings)
    except (ValueError, ConfigurationError) as exc:
        logger.error("%s", exc)
        return 2
    _print_summary(summary)
    return 0 if summary.ok else 1


if __name__ == "__main__":
    sys.exit(main())
