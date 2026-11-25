"""IEX (Indian Energy Exchange) DAM / GDAM / RTM price ingestion CLI.

Usage (from the project root)::

    # Once: insert the market and bid-area reference rows (idempotent)
    python -m scripts.ingest_iex reference [--dry-run]

    # Once migration 0012 is applied: bid area -> state rows (pi.area_state_map, idempotent)
    python -m scripts.ingest_iex area-states [--dry-run]

    # One delivery day, fully executed then rolled back
    python -m scripts.ingest_iex dam --date 2026-11-10 --dry-run

    # A range of delivery days (inclusive, at most 31 days per command)
    python -m scripts.ingest_iex gdam --start-date 2026-11-01 --end-date 2026-11-10
    python -m scripts.ingest_iex rtm --date 2026-11-10

    # Once migration 0013 is applied: load iex_market_snapshot from stored raw files (no IEX call)
    python -m scripts.ingest_iex backfill-snapshot all --dry-run
    python -m scripts.ingest_iex backfill-snapshot dam --start-date 2026-11-01 --end-date 2026-11-30

DAM and GDAM load the unconstrained MCP (area MKT) and 13 bid-area prices; RTM
loads the MCP only. Each run then loads the snapshot's volumes and congestion
into iex_market_snapshot from the same response. No scheduler is started.
Exit codes: 0 SUCCESS, 1 PARTIAL/FAILED, 2 invalid arguments or configuration.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from datetime import date

from config.settings import Settings, get_settings
from src.common.database import DatabaseManager
from src.common.exceptions import ConfigurationError, StorageError
from src.common.logging_config import get_logger, setup_logging
from src.ingestion.models import IngestionResult
from src.pipelines.iex import api
from src.pipelines.iex.area_states import seed_area_states
from src.pipelines.iex.reference import seed_reference
from src.pipelines.iex.runner import BackfillSummary, RunSummary, backfill_snapshots, run_iex

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
    states = sub.add_parser("area-states", help="insert bid area -> state rows into area_state_map (idempotent)")
    states.add_argument("--dry-run", action="store_true", help="show what would be inserted, then roll back")
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
    back = sub.add_parser("backfill-snapshot",
                          help="load iex_market_snapshot from stored snapshot raw files (no IEX request)")
    back.add_argument("market", choices=[*api.MARKETS, "all"])
    back.add_argument("--start-date", type=_date, help="only files whose requested range ends on or after this day")
    back.add_argument("--end-date", type=_date, help="only files whose requested range starts on or before this day")
    back.add_argument("--dry-run", action="store_true", help="execute fully, then roll back all database changes")
    return parser


def _run_line(r: IngestionResult) -> str:
    return (f"status={r.status.value} run={r.ingestion_run_id} documents={r.documents_processed} "
            f"duplicate_documents={r.duplicate_documents} read={r.records_read} inserted={r.records_inserted} "
            f"unchanged={r.records_unchanged} rejected={r.records_rejected} issues={r.data_quality_issues}")


def _backfill(settings: Settings, args: argparse.Namespace) -> int:
    ok = True
    for kind in api.MARKETS if args.market == "all" else [args.market]:
        try:
            summary: BackfillSummary = backfill_snapshots(kind, start_date=args.start_date, end_date=args.end_date,
                                                          dry_run=args.dry_run, settings=settings)
        except ValueError as exc:
            print(f"\nIEX {kind.upper()} snapshot backfill: {exc}")
            continue
        mode = "DRY RUN (rolled back)" if summary.dry_run else "committed"
        print(f"\nIEX {kind.upper()} snapshot backfill - {mode}: {len(summary.files)} file(s), "
              f"{summary.identical_files} identical re-fetch(es) left out")
        if summary.result is not None:
            print(f"  {_run_line(summary.result)}")
            if summary.result.error:
                print(f"  error: {summary.result.error}")
        ok = ok and summary.ok
    return 0 if ok else 1


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


def _area_states(settings: Settings, dry_run: bool) -> int:
    manager = DatabaseManager.from_settings(settings)
    try:
        with manager.engine.connect() as connection:
            transaction = connection.begin()
            result = seed_area_states(connection, settings.db_schema)
            transaction.rollback() if dry_run or result.overlaps else transaction.commit()
    finally:
        manager.dispose()
    mode = "DRY RUN (rolled back)" if dry_run else "rolled back (overlaps)" if result.overlaps else "committed"
    print(f"IEX area_state_map rows - {mode}")
    print(f"  inserted:  {len(result.inserted)} {result.inserted}")
    print(f"  unchanged: {len(result.unchanged)}")
    if result.differing:
        print(f"  existing rows with different values (left unchanged): {result.differing}")
    if result.overlaps:
        print(f"  overlapping validity periods: {result.overlaps}")
        return 1
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
    print(_run_line(r))
    if r.error:
        print(f"error: {r.error}")
    if summary.snapshot is not None:
        print(f"snapshot volumes: {_run_line(summary.snapshot)}")
        if summary.snapshot.error:
            print(f"snapshot error: {summary.snapshot.error}")
    elif summary.snapshot_skipped:
        print(f"snapshot volumes not loaded: {summary.snapshot_skipped}")
    if summary.raw_data_dir and summary.dry_run:
        print(f"Dry-run raw files: {summary.raw_data_dir}")


def main(argv: Sequence[str] | None = None, *, settings: Settings | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    if args.command not in ("reference", "area-states", "backfill-snapshot"):
        if args.date is not None and args.end_date is not None:
            parser.error("--end-date requires --start-date, not --date")
        if args.start_date is not None and args.end_date is None:
            parser.error("--start-date requires --end-date")
    settings = settings or get_settings()
    setup_logging(settings, stream=sys.stderr)
    try:
        if args.command == "reference":
            return _reference(settings, args.dry_run)
        if args.command == "area-states":
            return _area_states(settings, args.dry_run)
        if args.command == "backfill-snapshot":
            return _backfill(settings, args)
        start = args.date or args.start_date
        summary = run_iex(args.command, start_date=start, end_date=args.end_date or start, dry_run=args.dry_run,
                          settings=settings)
    except (ValueError, ConfigurationError, StorageError) as exc:
        logger.error("%s", exc)
        return 2
    _print_summary(summary)
    return 0 if summary.ok else 1


if __name__ == "__main__":
    sys.exit(main())
