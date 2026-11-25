"""pi-worker: execute queued ingestion jobs (docs/scheduler.md).

Usage (from the project root)::

    # The long-running service process (stops gracefully on Ctrl+C / Ctrl+Break / SIGTERM)
    python -m scripts.pi_worker run                     # WORKER_CONCURRENCY slots (default 2)
    python -m scripts.pi_worker run --concurrency 1

    # Claim and execute at most one due job, then exit (operations / troubleshooting).
    # Logs to LOG_DIR/pi-worker-once.log (never to the service's log file).
    python -m scripts.pi_worker once

Jobs run the existing ingestion runners and write to PostgreSQL and RAW_DATA_DIR exactly like the
ingest_* commands. Requires migration 0011_pi_jobs.

Exit codes: 0 normal stop; 1 runtime failure (a service wrapper may restart); 2 configuration error
(missing migration, invalid option: a service wrapper must NOT restart).
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence

from config.settings import Settings, get_settings
from src.common.exceptions import ConfigurationError
from src.common.logging_config import get_logger
from src.scheduler.process import (
    EXIT_CONFIGURATION,
    EXIT_OK,
    EXIT_RUNTIME_FAILURE,
    StopSignal,
    configure_process_logging,
    install_signal_handlers,
)
from src.scheduler.runtime import build_worker, open_runtime

logger = get_logger("scripts.pi_worker")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingestion job worker (pi-worker).")
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run", help="run worker slots until stopped")
    run.add_argument("--concurrency", type=int, help="override WORKER_CONCURRENCY")
    sub.add_parser("once", help="execute at most one due job, then exit")
    return parser


def main(argv: Sequence[str] | None = None, *, settings: Settings | None = None) -> int:
    args = _parser().parse_args(argv)
    settings = settings or get_settings()
    log_name = settings.worker_once_log_file_name if args.command == "once" else settings.worker_log_file_name
    process_settings = configure_process_logging(settings, log_name)
    if args.command == "run" and args.concurrency is not None and not 1 <= args.concurrency <= 16:
        logger.error("--concurrency must be 1-16")
        return EXIT_CONFIGURATION
    try:
        runtime = open_runtime(process_settings)
        try:
            runtime.require_schema()
            worker = build_worker(runtime)
            if args.command == "once":
                job = worker.run_once()
                print(f"job {job.job_id} processed" if job else "no due job")
                return EXIT_OK
            stop = StopSignal(process_settings.worker_stop_file)
            install_signal_handlers(stop)
            worker.run(stop, concurrency=args.concurrency)
        finally:
            runtime.close()
    except ConfigurationError as exc:
        logger.error("Configuration error (not restarting): %s", exc)
        return EXIT_CONFIGURATION
    except Exception:
        logger.exception("pi-worker failed")
        return EXIT_RUNTIME_FAILURE
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
