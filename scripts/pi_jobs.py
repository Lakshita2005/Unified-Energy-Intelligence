"""Scheduler / job administration (docs/scheduler.md).

Usage (from the project root)::

    python -m scripts.pi_jobs validate-config          # offline: parse config/scheduler.toml, show schedules
    python -m scripts.pi_jobs sync-config [--dry-run]  # copy the configuration into pi_jobs.schedule
                                                       # (never changes database enablement)
    python -m scripts.pi_jobs status                   # schedules, file/database sync, last checks, jobs
    python -m scripts.pi_jobs health [--max-age 180]   # exit 0 if scheduler and worker heartbeats are fresh
    python -m scripts.pi_jobs list [--status FAILED] [--schedule IEX_RTM] [--limit 50]
    python -m scripts.pi_jobs requeue JOB_ID [--force] # FAILED / CANCELLED / NOT_PUBLISHED -> PENDING
    python -m scripts.pi_jobs cancel JOB_ID            # PENDING / RETRY_WAIT -> CANCELLED

    # ADMIN ONLY (database role with UPDATE on pi_jobs.schedule.enabled; pi_app is refused):
    python -m scripts.pi_jobs enable KEY
    python -m scripts.pi_jobs disable KEY

Everything except validate-config needs migration 0011_pi_jobs. Exit codes: 0 ok, 1 refused / unhealthy /
database error, 2 configuration error.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from datetime import datetime, timedelta, timezone

from sqlalchemy.exc import DBAPIError

from config.settings import Settings, get_settings
from src.common.exceptions import ConfigurationError
from src.common.logging_config import setup_logging
from src.scheduler.runtime import load_config, open_runtime
from src.scheduler.scheduler import describe_due, sync_report

EXIT_OK, EXIT_FAILED, EXIT_CONFIGURATION = 0, 1, 2


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Scheduler and job administration.")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("validate-config", help="validate config/scheduler.toml (no database)")
    sync = sub.add_parser("sync-config", help="copy config/scheduler.toml into pi_jobs.schedule")
    sync.add_argument("--dry-run", action="store_true", help="show the changes, then roll back")
    sub.add_parser("status", help="show schedules, configuration sync, jobs and processes")
    health = sub.add_parser("health", help="exit 0 when scheduler and worker heartbeats are fresh")
    health.add_argument("--max-age", type=int, default=180, help="seconds (default 180)")
    lst = sub.add_parser("list", help="list jobs")
    lst.add_argument("--status")
    lst.add_argument("--schedule")
    lst.add_argument("--limit", type=int, default=50)
    requeue = sub.add_parser("requeue", help="put a finished job back to PENDING (version-aware)")
    requeue.add_argument("job_id", type=int)
    requeue.add_argument("--force", action="store_true",
                         help="requeue even though a newer version of the item was detected meanwhile")
    cancel = sub.add_parser("cancel", help="cancel a PENDING / RETRY_WAIT job")
    cancel.add_argument("job_id", type=int)
    for name in ("enable", "disable"):
        p = sub.add_parser(name, help=f"ADMIN ONLY: {name} a schedule in pi_jobs.schedule")
        p.add_argument("schedule")
    return parser


def _print(value: object) -> None:
    print(json.dumps(value, indent=2, default=str))


def _permission_denied(exc: DBAPIError) -> bool:
    return getattr(exc.orig, "sqlstate", None) == "42501" or "permission denied" in str(exc.orig).lower()


def main(argv: Sequence[str] | None = None, *, settings: Settings | None = None) -> int:
    args = _parser().parse_args(argv)
    settings = settings or get_settings()
    setup_logging(settings.model_copy(update={"log_to_file": False, "log_to_console": True}), stream=sys.stderr)
    now = datetime.now(timezone.utc)
    try:
        config, _checkers = load_config(settings)
        if args.command == "validate-config":
            print(f"{config.path}: {len(config.schedules)} schedule(s), "
                  f"{len(config.enabled())} enabled; SCHEDULER_GLOBAL_ENABLE={settings.scheduler_global_enable}")
            for row in describe_due(config, now):
                print(f"  {row['schedule']:<22} enabled={str(row['enabled']):<5} windows={row['windows']} "
                      f"interval_now={row['interval_now_s']}")
            _print_open_meteo(config, settings)
            return EXIT_OK
        runtime = open_runtime(settings)
        try:
            runtime.require_schema()
            repo = runtime.repository
            schedules = list(config.schedules.values())
            if args.command == "sync-config":
                if args.dry_run:
                    _print({"dry_run": True, "changes": runtime.preview_sync(schedules, now)})
                else:
                    _print({"changes": repo.sync_schedules(schedules, now)})
                return EXIT_OK
            if args.command == "status":
                report = sync_report(config, repo.db_schedules(), settings)
                out_of_sync = [r["schedule"] for r in report if r["in_sync"] is False]
                if out_of_sync:
                    print(f"WARNING: configuration file differs from pi_jobs.schedule for {out_of_sync}; "
                          "the scheduler refuses these schedules until 'pi_jobs sync-config' is run",
                          file=sys.stderr)
                _print({"global_enable": settings.scheduler_global_enable, "configuration": report,
                        **repo.status_summary()})
                return EXIT_OK
            if args.command == "health":
                processes = repo.status_summary()["processes"]
                fresh = {p["process_kind"] for p in processes
                         if now - p["last_seen_at"] <= timedelta(seconds=args.max_age)}
                ok = {"scheduler", "worker"} <= fresh
                _print({"healthy": ok, "fresh": sorted(fresh), "processes": processes})
                return EXIT_OK if ok else EXIT_FAILED
            if args.command == "list":
                _print(repo.list_jobs(status=args.status, schedule_key=args.schedule, limit=args.limit))
                return EXIT_OK
            if args.command in ("enable", "disable"):
                return _set_enabled(repo, config, args.schedule, args.command == "enable", now, settings)
            if args.command == "requeue":
                done, message = repo.requeue(args.job_id, now, force=args.force)
            else:
                done = repo.cancel(args.job_id, now)
                message = "cancelled" if done else "not changed (state does not allow it)"
            print(f"job {args.job_id}: {message}")
            return EXIT_OK if done else EXIT_FAILED
        finally:
            runtime.close()
    except ConfigurationError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return EXIT_CONFIGURATION
    except DBAPIError as exc:
        if _permission_denied(exc):
            print(f"permission denied for database user {settings.db_user!r}: this command needs a role with "
                  "the required privilege (enable/disable is admin-only; see docs/scheduler.md)", file=sys.stderr)
        else:
            print(f"database error: {type(exc.orig).__name__}", file=sys.stderr)
        return EXIT_FAILED


def _print_open_meteo(config, settings: Settings) -> None:  # noqa: ANN001
    """Plan, key presence (never the key) and the worst-case daily calls if every Open-Meteo schedule ran."""
    from src.scheduler.checkers.open_meteo import SOURCE, estimated_daily_calls, location_count

    schedules = [s for s in config.schedules.values() if s.source == SOURCE]
    if not schedules:
        return
    try:
        total = sum(estimated_daily_calls(s, location_count(s, settings)) for s in schedules)
        usage = f"worst case if all enabled: {total} calls/day (limit {settings.open_meteo_daily_call_limit})"
    except Exception as exc:
        usage = f"usage estimate unavailable ({type(exc).__name__})"
    key_set = settings.open_meteo_api_key is not None and bool(settings.open_meteo_api_key.get_secret_value())
    note = ("free endpoint, NON-COMMERCIAL use only, CC BY 4.0 attribution required" if settings.open_meteo_plan
            == "free" else "commercial plan (customer- endpoint)")
    print(f"Open-Meteo: OPEN_METEO_PLAN={settings.open_meteo_plan} ({note}); "
          f"API key set: {'yes' if key_set else 'no'}; {usage}")


def _set_enabled(repo, config, key: str, enabled: bool, now: datetime, settings: Settings) -> int:  # noqa: ANN001
    if key not in config.schedules:
        print(f"unknown schedule {key!r}; known: {sorted(config.schedules)}", file=sys.stderr)
        return EXIT_CONFIGURATION
    if not repo.set_enabled(key, enabled, now):
        print(f"{key} is not in pi_jobs.schedule; run 'pi_jobs sync-config' first", file=sys.stderr)
        return EXIT_FAILED
    row = next(r for r in sync_report(config, repo.db_schedules(), settings) if r["schedule"] == key)
    _print({"schedule": key, "db_enabled": enabled, "would_run": row["would_run"], "reason": row["reason"]})
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
