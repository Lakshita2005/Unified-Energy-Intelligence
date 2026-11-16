"""IMD (India Meteorological Department) ingestion CLI.

Live IMD access needs IMD approval and IP whitelisting; do not run these commands against IMD
until access is authorised. The IMD tables come from Alembic 0008_imd.

Usage (from the project root)::

    # AWS/ARG: station mapping, then observations (all stations, one call sign, or one state id 1-36)
    python -m scripts.ingest_imd aws --dry-run
    python -m scripts.ingest_imd aws --station NDL
    python -m scripts.ingest_imd aws --state-id 7

    # 7-day city forecast (all cities or one station code)
    python -m scripts.ingest_imd city-forecast --dry-run
    python -m scripts.ingest_imd city-forecast --station 42182

Exit codes: 0 every run SUCCESS, 1 a run PARTIAL/FAILED, 2 invalid arguments or configuration
(e.g. migration not applied).
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence

from config.settings import Settings, get_settings
from src.common.exceptions import ConfigurationError
from src.common.logging_config import get_logger, setup_logging
from src.pipelines.imd.runner import COMMANDS, ImdRunSummary, run_imd

logger = get_logger("scripts.ingest_imd")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="IMD API ingestion (AWS/ARG observations, city forecasts).")
    parser.add_argument("command", choices=sorted(COMMANDS))
    parser.add_argument("--station", help="aws: CALL_SIGN (e.g. NDL); city-forecast: Station_Code (e.g. 42182)")
    parser.add_argument("--state-id", type=int, help="aws only: documented IMD state id 1-36 (e.g. 7 = DELHI)")
    parser.add_argument("--dry-run", action="store_true", help="execute fully, then roll back all database changes")
    parser.add_argument("--reprocess", action="store_true", help="re-parse responses already ingested")
    return parser


def _print(summary: ImdRunSummary) -> None:
    mode = "DRY RUN (rolled back)" if summary.dry_run else "committed"
    print(f"\nIMD {summary.command} - {mode}")
    for table, r in summary.runs.items():
        print(f"  {table:24} status={r.status.value} run={r.ingestion_run_id} documents={r.documents_processed} "
              f"duplicate_documents={r.duplicate_documents} read={r.records_read} inserted={r.records_inserted} "
              f"unchanged={r.records_unchanged} rejected={r.records_rejected} issues={r.data_quality_issues}")
        if r.error:
            print(f"  {'':24} error: {r.error}")
    if summary.raw_data_dir and summary.dry_run:
        print(f"Dry-run raw files: {summary.raw_data_dir}")


def main(argv: Sequence[str] | None = None, *, settings: Settings | None = None) -> int:
    args = _parser().parse_args(argv)
    settings = settings or get_settings()
    setup_logging(settings, stream=sys.stderr)
    try:
        summary = run_imd(args.command, station=args.station, state_id=args.state_id, dry_run=args.dry_run,
                          reprocess=args.reprocess, settings=settings)
    except (ValueError, ConfigurationError) as exc:
        logger.error("%s", exc)
        return 2
    _print(summary)
    return 0 if summary.ok else 1


if __name__ == "__main__":
    sys.exit(main())
