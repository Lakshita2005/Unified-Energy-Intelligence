"""Grid-India (NLDC) Daily PSP ingestion CLI.

Usage (from the project root)::

    # One report date, all implemented sections, fully executed then rolled back
    python -m scripts.ingest_grid_india psp --date 2026-11-10 --dry-run

    # A range (inclusive operating days, at most 31), chosen sections
    python -m scripts.ingest_grid_india psp --start-date 2026-11-01 --end-date 2026-11-10 --sections supply,states

    # Re-parse workbooks already ingested (after a parser fix); unchanged records are skipped
    python -m scripts.ingest_grid_india psp --date 2026-11-10 --reprocess

Each workbook is downloaded and stored once; each section has its own
ingestion run. No scheduler is started. Exit codes: 0 every section SUCCESS,
1 any section PARTIAL/FAILED, 2 invalid arguments or configuration.
Terms of use of grid-india.in are not yet verified (decision G5).
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from datetime import date

from config.settings import Settings, get_settings
from src.common.exceptions import ConfigurationError
from src.common.logging_config import get_logger, setup_logging
from src.pipelines.grid_india import api
from src.pipelines.grid_india.pipeline import SECTION_PIPELINES
from src.pipelines.grid_india.runner import PspRunSummary, run_grid_india_psp

logger = get_logger("scripts.ingest_grid_india")


def _date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid date {value!r}, expected YYYY-MM-DD") from exc


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Grid-India (NLDC) Daily PSP workbook ingestion.")
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("psp", help="Daily PSP workbook: " + ", ".join(
        f"{name} (section {cls.section_label} -> {cls.target_table})" for name, cls in SECTION_PIPELINES.items()))
    when = p.add_mutually_exclusive_group(required=True)
    when.add_argument("--date", type=_date, help="one operating day (YYYY-MM-DD)")
    when.add_argument("--start-date", type=_date, help="first operating day of a range")
    p.add_argument("--end-date", type=_date, help=f"last operating day, inclusive (max {api.MAX_DAYS_PER_COMMAND} days)")
    p.add_argument("--sections", default=",".join(SECTION_PIPELINES),
                   help=f"comma-separated sections (default: all of {','.join(SECTION_PIPELINES)})")
    p.add_argument("--dry-run", action="store_true", help="execute fully, then roll back all database changes")
    p.add_argument("--reprocess", action="store_true",
                   help="re-parse workbooks already ingested; unchanged records are skipped")
    return parser


def _print(summary: PspRunSummary) -> None:
    mode = "DRY RUN (rolled back)" if summary.dry_run else "committed"
    print(f"\nGrid-India Daily PSP {summary.start_date}..{summary.end_date} - {mode}")
    print(f"workbooks downloaded once: {len(summary.workbooks)} {summary.workbooks}")
    if summary.missing:
        print(f"no workbook listed for: {summary.missing}")
    if summary.fetch_error:
        print(f"fetch error (all sections FAILED): {summary.fetch_error}")
    for name, s in summary.sections.items():
        r = s.result
        if r is None:
            print(f"  {name}: {s.status.value} {s.error}")
            continue
        print(f"  {name:7} status={r.status.value} run={r.ingestion_run_id} documents={r.documents_processed} "
              f"duplicate_documents={r.duplicate_documents} read={r.records_read} inserted={r.records_inserted} "
              f"unchanged={r.records_unchanged} rejected={r.records_rejected} issues={r.data_quality_issues}")
        if r.error:
            print(f"           error: {r.error}")
    if summary.raw_data_dir and summary.dry_run:
        print(f"Dry-run raw files: {summary.raw_data_dir}")


def main(argv: Sequence[str] | None = None, *, settings: Settings | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    if args.date is not None and args.end_date is not None:
        parser.error("--end-date requires --start-date, not --date")
    if args.start_date is not None and args.end_date is None:
        parser.error("--start-date requires --end-date")
    sections = [s.strip() for s in args.sections.split(",") if s.strip()]
    settings = settings or get_settings()
    setup_logging(settings, stream=sys.stderr)
    start = args.date or args.start_date
    try:
        summary = run_grid_india_psp(start_date=start, end_date=args.end_date or start, sections=sections,
                                     dry_run=args.dry_run, reprocess=args.reprocess, settings=settings)
    except (ValueError, ConfigurationError) as exc:
        logger.error("%s", exc)
        return 2
    _print(summary)
    return 0 if summary.ok else 1


if __name__ == "__main__":
    sys.exit(main())
