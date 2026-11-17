"""MOSDAC (ISRO SAC) solar / wind energy forecast ingestion CLI.

Locations come from ``MOSDAC_LOCATIONS_FILE`` (default ``config/mosdac_locations.toml``). The tables
come from Alembic 0009_mosdac (written, NOT applied until approved). No scheduling, no backfill.

Usage (from the project root)::

    python -m scripts.ingest_mosdac locations                       # list configured locations (no network, no DB)
    python -m scripts.ingest_mosdac solar --location ESEPL --dry-run
    python -m scripts.ingest_mosdac wind --location ESEPL
    python -m scripts.ingest_mosdac all                             # every configured location, both endpoints

Exit codes: 0 every run SUCCESS, 1 a run PARTIAL/FAILED or a location refused, 2 invalid arguments or
configuration (e.g. migration not applied, unknown location).
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence

from config.settings import Settings, get_settings
from src.common.exceptions import ConfigurationError
from src.common.logging_config import get_logger, setup_logging
from src.pipelines.mosdac.runner import COMMANDS, MosdacRunSummary, load_locations, run_mosdac

logger = get_logger("scripts.ingest_mosdac")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="MOSDAC solar / wind energy forecast ingestion.")
    parser.add_argument("command", choices=[*sorted(COMMANDS), "locations"])
    parser.add_argument("--location", action="append", metavar="CODE",
                        help="location code from the MOSDAC locations file (repeatable; default: all)")
    parser.add_argument("--dry-run", action="store_true", help="execute fully, then roll back all database changes")
    parser.add_argument("--reprocess", action="store_true", help="re-parse responses already ingested")
    return parser


def _print(summary: MosdacRunSummary) -> None:
    mode = "DRY RUN (rolled back)" if summary.dry_run else "committed"
    print(f"\nMOSDAC {summary.command} - {mode}")
    for (code, kind), r in summary.runs.items():
        print(f"  {code:10} {kind:5} status={r.status.value} run={r.ingestion_run_id} "
              f"documents={r.documents_processed} duplicate_documents={r.duplicate_documents} read={r.records_read} "
              f"inserted={r.records_inserted} unchanged={r.records_unchanged} rejected={r.records_rejected} "
              f"issues={r.data_quality_issues}")
        if r.error:
            print(f"  {'':16} error: {r.error}")
    for code, error in summary.location_errors.items():
        print(f"  {code:10} REFUSED: {error}")
    if summary.raw_data_dir and summary.dry_run:
        print(f"Dry-run raw files: {summary.raw_data_dir}")


def main(argv: Sequence[str] | None = None, *, settings: Settings | None = None) -> int:
    args = _parser().parse_args(argv)
    settings = settings or get_settings()
    setup_logging(settings, stream=sys.stderr)
    try:
        if args.command == "locations":
            for code, c in load_locations(settings).items():
                print(f"{code:10} {c.latitude:>12} {c.longitude:>12}  {c.name}  [{c.coordinate_source}]")
            return 0
        summary = run_mosdac(args.command, locations=args.location, dry_run=args.dry_run,
                             reprocess=args.reprocess, settings=settings)
    except (ValueError, ConfigurationError) as exc:
        logger.error("%s", exc)
        return 2
    _print(summary)
    return 0 if summary.ok else 1


if __name__ == "__main__":
    sys.exit(main())
