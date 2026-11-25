"""pi-scheduler: availability checks -> jobs (docs/scheduler.md).

Usage (from the project root)::

    # The long-running service process (single instance; stops gracefully on Ctrl+C / Ctrl+Break / SIGTERM)
    python -m scripts.pi_scheduler run

    # Diagnose ONE schedule: run its availability check against the live source and print what it finds.
    # Makes real requests to the source and reads pi tables; writes nothing (no check row, no job).
    # Logs to LOG_DIR/pi-scheduler-check.log (never to the service's log file).
    python -m scripts.pi_scheduler check --schedule GRID_INDIA_PSP

Nothing happens unless SCHEDULER_GLOBAL_ENABLE=true, the schedule is enabled in config/scheduler.toml, the
configuration has been synced (python -m scripts.pi_jobs sync-config) and an administrator enabled the
schedule in the database. Requires migration 0011_pi_jobs (applied by the schema owner).

Exit codes: 0 normal stop; 1 runtime failure (lock already held / lost, unexpected error: a service
wrapper may restart); 2 configuration error (missing migration, invalid configuration, unknown schedule:
a service wrapper must NOT restart).
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence

from config.settings import Settings, get_settings
from src.common.exceptions import ConfigurationError
from src.common.http_client import HttpClient
from src.common.logging_config import get_logger
from src.scheduler.checkers.base import CheckContext
from src.scheduler.checkers.state import PostgresSourceState
from src.scheduler.clock import SystemClock
from src.scheduler.process import (
    EXIT_CONFIGURATION,
    EXIT_OK,
    EXIT_RUNTIME_FAILURE,
    StopSignal,
    configure_process_logging,
    install_signal_handlers,
)
from src.scheduler.runtime import build_scheduler, load_config, open_runtime
from src.scheduler.scheduler import SchedulerAlreadyRunning, SchedulerLockLost

logger = get_logger("scripts.pi_scheduler")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Availability-based scheduler (pi-scheduler).")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("run", help="run the scheduler loop until stopped")
    check = sub.add_parser("check", help="run one schedule's availability check now (live source, no writes)")
    check.add_argument("--schedule", required=True)
    return parser


def _check(settings: Settings, key: str) -> int:
    config, checkers = load_config(settings)
    if key not in config.schedules:
        print(f"unknown schedule {key!r}; known: {sorted(config.schedules)}")
        return EXIT_CONFIGURATION
    runtime = open_runtime(settings)
    http = HttpClient(settings=settings)
    try:
        schedule = config.schedules[key]
        ctx = CheckContext(schedule, SystemClock().now(), None, http, settings,
                           PostgresSourceState(runtime.database, settings.db_schema))
        result = checkers[key].check(ctx)
    finally:
        http.close()
        runtime.close()
    print(json.dumps({"schedule": key, "outcome": result.outcome.value, "error": result.error,
                      "detail": result.detail,
                      "detections": [{"item": d.item_key, "version": d.version_token, "target": d.target,
                                      **d.detail} for d in result.detections]}, indent=2, default=str))
    return EXIT_OK


def _run(settings: Settings) -> int:
    runtime = open_runtime(settings)
    try:
        runtime.require_schema()
        scheduler = build_scheduler(runtime)
        stop = StopSignal(settings.scheduler_stop_file)
        install_signal_handlers(stop)
        scheduler.run(stop)
    finally:
        runtime.close()
    return EXIT_OK


def main(argv: Sequence[str] | None = None, *, settings: Settings | None = None) -> int:
    args = _parser().parse_args(argv)
    settings = settings or get_settings()
    # 'check' is a manual diagnostic: its own log file, so it never interleaves with (or rotates) the service log
    log_name = settings.scheduler_check_log_file_name if args.command == "check" else settings.scheduler_log_file_name
    process_settings = configure_process_logging(settings, log_name)
    try:
        if args.command == "check":
            return _check(process_settings, args.schedule)
        return _run(process_settings)
    except ConfigurationError as exc:
        logger.error("Configuration error (not restarting): %s", exc)
        return EXIT_CONFIGURATION
    except (SchedulerAlreadyRunning, SchedulerLockLost) as exc:
        logger.error("%s", exc)
        return EXIT_RUNTIME_FAILURE
    except Exception:
        logger.exception("pi-scheduler failed")
        return EXIT_RUNTIME_FAILURE


if __name__ == "__main__":
    sys.exit(main())
