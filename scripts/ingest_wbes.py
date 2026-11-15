"""WBES (Grid-India Web Based Energy Scheduling) CLI - PX Dashboard.

Live WBES access must be explicitly authorised before any of these commands is
run against the portal (decision W2).

Usage (from the project root)::

    # Interactive login: a browser window opens; YOU enter username, OTP and CAPTCHA.
    python -m scripts.ingest_wbes login

    # Session status (never shows tokens) / delete the saved session
    python -m scripts.ingest_wbes session
    python -m scripts.ingest_wbes session --delete

    # Re-parse stored raw documents (no WBES call; checksums verified), e.g. after a parser fix
    python -m scripts.ingest_wbes px --date 2026-11-10 --reprocess

    # PX Dashboard for one delivery date (or a range of at most 31 days), fully executed then rolled back
    python -m scripts.ingest_wbes px --date 2026-11-10 --dry-run
    python -m scripts.ingest_wbes px --start-date 2026-11-08 --end-date 2026-11-10 --exchanges IEX --markets DAM,RTM

The session file lives outside the project, OneDrive and raw data
(``WBES_SESSION_FILE``, default ``<local app data>/pi/wbes/session.json``).
Ingestion never opens a browser: if the session has ended, the runs are
recorded as FAILED with "login required" and the exit code is 3.
Exit codes: 0 all parts SUCCESS, 1 a part PARTIAL/FAILED, 2 invalid arguments,
configuration or session lock, 3 login required.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from datetime import date, datetime, timezone

from config.settings import Settings, get_settings
from src.common.database import DatabaseManager
from src.common.exceptions import ConfigurationError
from src.common.logging_config import get_logger, setup_logging
from src.pipelines.wbes.errors import WbesError, WbesSessionError, WbesSessionLocked
from src.pipelines.wbes.lock import wbes_session_lock
from src.pipelines.wbes.login import interactive_login
from src.pipelines.wbes.pipeline import PX_PIPELINES
from src.pipelines.wbes.runner import PxRunSummary, run_wbes_px
from src.pipelines.wbes.session import TokenStore, session_path

logger = get_logger("scripts.ingest_wbes")


def _date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid date {value!r}, expected YYYY-MM-DD") from exc


def _csv(value: str | None) -> list[str] | None:
    return [v.strip() for v in value.split(",") if v.strip()] if value else None


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="WBES PX Dashboard ingestion.")
    sub = parser.add_subparsers(dest="command", required=True)
    login = sub.add_parser("login", help="interactive login in a browser window (you enter username, OTP, CAPTCHA)")
    login.add_argument("--wait-minutes", type=float, help="how long the login window waits (default WBES_LOGIN_WAIT_MINUTES)")
    session = sub.add_parser("session", help="show the saved session (never the tokens)")
    session.add_argument("--delete", action="store_true", help="delete the saved session file")
    px = sub.add_parser("px", help="PX Dashboard: " + ", ".join(
        f"{name} -> {cls.target_table}" for name, cls in PX_PIPELINES.items()))
    when = px.add_mutually_exclusive_group(required=True)
    when.add_argument("--date", type=_date, help="one delivery date (YYYY-MM-DD)")
    when.add_argument("--start-date", type=_date, help="first delivery date of a range")
    px.add_argument("--end-date", type=_date, help="last delivery date, inclusive (max 31 days, at most tomorrow)")
    px.add_argument("--parts", default=",".join(PX_PIPELINES), help="region,link,entity (default all)")
    px.add_argument("--exchanges", help="comma-separated exchange names as listed by WBES (default all listed)")
    px.add_argument("--markets", help="comma-separated market names as listed by WBES (default all listed)")
    px.add_argument("--dry-run", action="store_true", help="execute fully, then roll back all database changes")
    px.add_argument("--reprocess", action="store_true",
                    help="re-parse the stored raw documents for the range (no WBES call, no login needed)")
    return parser


def _login(settings: Settings, wait_minutes: float | None) -> int:
    store = TokenStore(session_path(settings))
    manager = DatabaseManager.from_settings(settings)
    try:
        if settings.wbes_utility_id:
            with wbes_session_lock(manager.engine, settings.wbes_utility_id):
                interactive_login(settings, store, wait_minutes=wait_minutes)
        else:
            tokens = interactive_login(settings, store, wait_minutes=wait_minutes, save=False)
            with wbes_session_lock(manager.engine, tokens.utility_id):
                store.save(tokens)
            print(f"WBES session saved to {store.path} (utility {tokens.utility_id}). Tokens are not displayed. "
                  "Consider setting WBES_UTILITY_ID in .env.")
    finally:
        manager.dispose()
    return 0


def _session(settings: Settings, delete: bool) -> int:
    store = TokenStore(session_path(settings))
    if delete:
        print(f"Session file {store.path}: {'deleted' if store.delete() else 'not present'}")
        return 0
    tokens = store.load()
    if tokens is None:
        print(f"No WBES session at {store.path}. Run: python -m scripts.ingest_wbes login")
        return 3
    expires = tokens.access_expires_at
    state = "unknown" if expires is None else ("expired" if expires <= datetime.now(timezone.utc) else "valid")
    print(f"WBES session at {store.path}: utility {tokens.utility_id}, logged in {tokens.login_at.isoformat()}, "
          f"last refresh {tokens.refreshed_at.isoformat() if tokens.refreshed_at else 'never'}, "
          f"access token {state} (it is refreshed automatically while the server accepts the refresh token)")
    return 0


def _print(summary: PxRunSummary) -> None:
    mode = "DRY RUN (rolled back)" if summary.dry_run else "committed"
    print(f"\nWBES PX Dashboard {summary.start_date}..{summary.end_date} - {mode}")
    print(f"responses fetched once: {len(summary.responses)}")
    if summary.not_listed:
        print(f"requested but not listed by WBES: {summary.not_listed}")
    if summary.fetch_error:
        print(f"fetch error (all parts FAILED): {summary.fetch_error}")
    for name, part in summary.parts.items():
        r = part.result
        if r is None:
            print(f"  {name}: {part.status.value} {part.error}")
            continue
        print(f"  {name:6} status={r.status.value} run={r.ingestion_run_id} documents={r.documents_processed} "
              f"duplicate_documents={r.duplicate_documents} read={r.records_read} inserted={r.records_inserted} "
              f"unchanged={r.records_unchanged} rejected={r.records_rejected} issues={r.data_quality_issues}")
    if summary.raw_data_dir and summary.dry_run:
        print(f"Dry-run raw files: {summary.raw_data_dir}")


def main(argv: Sequence[str] | None = None, *, settings: Settings | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    settings = settings or get_settings()
    setup_logging(settings, stream=sys.stderr)
    try:
        if args.command == "login":
            return _login(settings, args.wait_minutes)
        if args.command == "session":
            return _session(settings, args.delete)
        if args.date is not None and args.end_date is not None:
            parser.error("--end-date requires --start-date, not --date")
        if args.start_date is not None and args.end_date is None:
            parser.error("--start-date requires --end-date")
        start = args.date or args.start_date
        summary = run_wbes_px(start_date=start, end_date=args.end_date or start, parts=_csv(args.parts),
                              exchanges=_csv(args.exchanges), markets=_csv(args.markets), dry_run=args.dry_run,
                              reprocess=args.reprocess, settings=settings)
    except (ValueError, ConfigurationError, WbesSessionError, WbesSessionLocked) as exc:
        logger.error("%s", exc)
        return 2
    except WbesError as exc:
        logger.error("%s", exc)
        return 1
    _print(summary)
    if summary.login_required:
        return 3
    return 0 if summary.ok else 1


if __name__ == "__main__":
    sys.exit(main())
