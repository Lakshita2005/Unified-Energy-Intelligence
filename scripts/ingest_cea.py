"""CEA (Central Electricity Authority) ingestion CLI.

Usage (from the project root)::

    # Reference rows: regions, states, and the plants / units / name mappings of
    # DGR sub-report 2 for a report date (default: two days ago). Idempotent.
    python -m scripts.ingest_cea reference [--date 2026-11-11] [--dry-run]

    # Daily reports (inclusive report dates, at most 31 per command; latest = yesterday)
    python -m scripts.ingest_cea coal --date 2026-11-11 --dry-run
    python -m scripts.ingest_cea outages --start-date 2026-11-05 --end-date 2026-11-11
    python -m scripts.ingest_cea plant-daily --date 2026-11-11

    # Re-parse files already ingested (after a parser fix); only missing/changed records load
    python -m scripts.ingest_cea outages --start-date 2026-11-05 --end-date 2026-11-10 --reprocess

    # Monthly Power Supply Position (at most 12 months per command)
    python -m scripts.ingest_cea psp-energy --month 2026-08
    python -m scripts.ingest_cea psp-peak --start-month 2026-01 --end-month 2026-08

No scheduler is started. Exit codes: 0 SUCCESS, 1 PARTIAL/FAILED, 2 invalid
arguments or configuration (e.g. reference rows missing).
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from datetime import date

from config.settings import Settings, get_settings
from src.common.exceptions import ConfigurationError
from src.common.logging_config import get_logger, setup_logging
from src.pipelines.cea import api
from src.pipelines.cea.reference import SeedResult
from src.pipelines.cea.runner import RunSummary, run_cea_daily, run_cea_psp, run_reference

logger = get_logger("scripts.ingest_cea")

DAILY = {
    "coal": "Daily Coal Stock Report -> cea_coal_stock_daily",
    "outages": "DGR sub-report 10 (coal/lignite/nuclear unit outages) -> cea_dgr_unit_outage",
    "plant-daily": "DGR sub-report 2 (station generation, coal stock days) -> cea_dgr_plant_daily",
}
MONTHLY = {
    "psp-energy": "monthly Energy Power Supply Position -> cea_power_supply_position",
    "psp-peak": "monthly Peak Power Supply Position -> cea_power_supply_position",
}


def _date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid date {value!r}, expected YYYY-MM-DD") from exc


def _month(value: str) -> date:
    try:
        return api.parse_month(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="CEA coal stock, unit outage, generation and power supply "
                                                 "position ingestion.")
    sub = parser.add_subparsers(dest="command", required=True)
    ref = sub.add_parser("reference", help="seed regions, states, plants, units and name mappings (idempotent)")
    ref.add_argument("--date", type=_date, help="DGR report date to take stations from (default: two days ago)")
    ref.add_argument("--dry-run", action="store_true", help="show what would be inserted, then roll back")
    for kind, help_text in DAILY.items():
        p = sub.add_parser(kind, help=help_text)
        when = p.add_mutually_exclusive_group(required=True)
        when.add_argument("--date", type=_date, help="one report date (YYYY-MM-DD)")
        when.add_argument("--start-date", type=_date, help="first report date of a range (YYYY-MM-DD)")
        p.add_argument("--end-date", type=_date,
                       help=f"last report date, inclusive (with --start-date; at most {api.MAX_DAYS_PER_COMMAND} days)")
        p.add_argument("--dry-run", action="store_true", help="execute fully, then roll back all database changes")
        p.add_argument("--reprocess", action="store_true",
                       help="re-parse files already ingested (e.g. after a parser fix); unchanged records are skipped")
    for kind, help_text in MONTHLY.items():
        p = sub.add_parser(kind, help=help_text)
        when = p.add_mutually_exclusive_group(required=True)
        when.add_argument("--month", type=_month, help="one month (YYYY-MM)")
        when.add_argument("--start-month", type=_month, help="first month of a range (YYYY-MM)")
        p.add_argument("--end-month", type=_month,
                       help=f"last month, inclusive (with --start-month; at most {api.MAX_MONTHS_PER_COMMAND})")
        p.add_argument("--dry-run", action="store_true", help="execute fully, then roll back all database changes")
        p.add_argument("--reprocess", action="store_true",
                       help="re-parse files already ingested (e.g. after a parser fix); unchanged records are skipped")
    return parser


def _print_seed(result: SeedResult, report_date: date, dry_run: bool) -> None:
    mode = "DRY RUN (rolled back)" if dry_run else "committed"
    print(f"CEA reference rows from DGR sub-report 2 of {report_date} - {mode}")
    print(f"  inserted:  {result.inserted}")
    print(f"  unchanged: {result.unchanged}")
    if result.skipped:
        print(f"  skipped:   {len(result.skipped)} {result.skipped}")
    if result.differing:
        print(f"  existing rows with different values (left unchanged): {len(result.differing)} {result.differing[:20]}")


def _print_summary(summary: RunSummary) -> None:
    mode = "DRY RUN (rolled back)" if summary.dry_run else "committed"
    print(f"\nCEA {summary.kind} {summary.period} - {mode}")
    if summary.seeded_reference:
        print(f"Reference rows seeded inside the dry run: {summary.seeded_reference.inserted}")
    r = summary.result
    if r is None:
        print(f"status {summary.status.value}: {summary.error}")
        return
    print(f"status={r.status.value} run={r.ingestion_run_id} documents={r.documents_processed} "
          f"duplicate_documents={r.duplicate_documents} read={r.records_read} inserted={r.records_inserted} "
          f"unchanged={r.records_unchanged} rejected={r.records_rejected} issues={r.data_quality_issues}")
    if summary.missing:
        print(f"not published: {summary.missing}")
    if r.error:
        print(f"error: {r.error}")
    if summary.raw_data_dir and summary.dry_run:
        print(f"Dry-run raw files: {summary.raw_data_dir}")


def main(argv: Sequence[str] | None = None, *, settings: Settings | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    if args.command in DAILY:
        if args.date is not None and args.end_date is not None:
            parser.error("--end-date requires --start-date, not --date")
        if args.start_date is not None and args.end_date is None:
            parser.error("--start-date requires --end-date")
    if args.command in MONTHLY:
        if args.month is not None and args.end_month is not None:
            parser.error("--end-month requires --start-month, not --month")
        if args.start_month is not None and args.end_month is None:
            parser.error("--start-month requires --end-month")
    settings = settings or get_settings()
    setup_logging(settings, stream=sys.stderr)
    try:
        if args.command == "reference":
            result, report_date = run_reference(report_date=args.date, dry_run=args.dry_run, settings=settings)
            _print_seed(result, report_date, args.dry_run)
            return 0
        if args.command in DAILY:
            start = args.date or args.start_date
            summary = run_cea_daily(args.command, start_date=start, end_date=args.end_date or start,
                                    dry_run=args.dry_run, settings=settings, reprocess=args.reprocess)
        else:
            start = args.month or args.start_month
            summary = run_cea_psp(args.command, start_month=start, end_month=args.end_month or start,
                                  dry_run=args.dry_run, settings=settings, reprocess=args.reprocess)
    except (ValueError, ConfigurationError) as exc:
        logger.error("%s", exc)
        return 2
    _print_summary(summary)
    return 0 if summary.ok else 1


if __name__ == "__main__":
    sys.exit(main())
