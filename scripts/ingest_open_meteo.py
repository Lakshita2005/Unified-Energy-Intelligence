"""Open-Meteo weather ingestion CLI.

Usage (from the project root)::

    # Show configured and registered locations (no writes, no API calls)
    python -m scripts.ingest_open_meteo locations

    # Forecast for one location, fully executed then rolled back
    python -m scripts.ingest_open_meteo forecast --location PLANT_A --dry-run

    # Forecast (default 7 days) for every configured location
    python -m scripts.ingest_open_meteo forecast --forecast-days 7

    # Historical range (inclusive IST dates; capped at --max-days, default 31)
    python -m scripts.ingest_open_meteo historical --location PLANT_A --start-date 2026-11-01 --end-date 2026-11-05

No scheduler is started and no backfill runs without explicit dates.
Exit codes: 0 all locations SUCCESS, 1 any location PARTIAL/FAILED, 2 invalid arguments or configuration.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from datetime import date

from config.settings import Settings, get_settings
from src.common.database import DatabaseManager
from src.common.exceptions import ConfigurationError
from src.common.logging_config import get_logger, setup_logging
from src.pipelines.open_meteo import api
from src.pipelines.open_meteo.locations import WeatherLocationRepository, is_test_location, load_location_config
from src.pipelines.open_meteo.runner import RunSummary, run_open_meteo

logger = get_logger("scripts.ingest_open_meteo")


def _date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid date {value!r}, expected YYYY-MM-DD") from exc


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Open-Meteo weather ingestion.")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("locations", help="list configured and registered locations")

    def common(p: argparse.ArgumentParser) -> None:
        p.add_argument("--location", action="append", dest="locations", metavar="CODE",
                       help="location code (repeatable); default: all configured/registered locations "
                            "except TEST_ONLY_* test locations")
        p.add_argument("--dry-run", action="store_true", help="execute fully, then roll back all database changes")

    forecast = sub.add_parser("forecast", help="ingest hourly forecasts")
    common(forecast)
    forecast.add_argument("--forecast-days", type=int, default=7,
                          help=f"forecast horizon in days from today IST (1-{api.MAX_FORECAST_DAYS}, default 7)")

    historical = sub.add_parser("historical", help="ingest hourly historical (reanalysis) weather")
    common(historical)
    historical.add_argument("--start-date", type=_date, required=True, help="first IST date (YYYY-MM-DD)")
    historical.add_argument("--end-date", type=_date, required=True, help="last IST date, inclusive (YYYY-MM-DD)")
    historical.add_argument("--max-days", type=int, default=api.HISTORICAL_CHUNK_DAYS,
                            help="safety limit on the range length (default 31)")
    return parser


def _print_locations(settings: Settings) -> int:
    configured = load_location_config(settings.weather_locations_file)
    manager = DatabaseManager.from_settings(settings)
    try:
        with manager.read_only_connection() as conn:
            registered = WeatherLocationRepository(settings.db_schema).list_locations(conn)
    finally:
        manager.dispose()
    print(f"Configuration file: {settings.weather_locations_file} "
          f"({'found' if settings.weather_locations_file.exists() else 'not found'})")
    codes = sorted({c.code for c in configured} | set(registered))
    if not codes:
        print("No locations configured or registered. See config/weather_locations.example.toml.")
        return 0
    by_code = {c.code: c for c in configured}
    for code in codes:
        cfg, reg = by_code.get(code), registered.get(code)
        coords = f"({(cfg or reg).latitude}, {(cfg or reg).longitude})"  # type: ignore[union-attr]
        state = "registered" if reg else "configured only (registered on first ingestion)"
        if is_test_location(code):
            state += ", TEST ONLY (explicit --location only)"
        print(f"  {code:<24} {coords:<28} {state}" + (f"  source: {cfg.coordinate_source}" if cfg else ""))
    return 0


def _print_summary(summary: RunSummary) -> None:
    mode = "DRY RUN (rolled back)" if summary.dry_run else "committed"
    print(f"\nOpen-Meteo {summary.kind} ingestion - {mode}")
    if summary.registered_locations:
        print(f"Registered new weather locations: {', '.join(summary.registered_locations)}")
    print(f"{'location':<24} {'status':<8} {'run':>5} {'read':>6} {'inserted':>9} {'unchanged':>10} "
          f"{'rejected':>9} {'issues':>7}")
    for outcome in summary.outcomes:
        r = outcome.result
        if r is None:
            print(f"{outcome.location_code:<24} {outcome.status.value:<8} {'-':>5}")
        else:
            print(f"{outcome.location_code:<24} {outcome.status.value:<8} {r.ingestion_run_id:>5} {r.records_read:>6} "
                  f"{r.records_inserted:>9} {r.records_unchanged:>10} {r.records_rejected:>9} "
                  f"{r.data_quality_issues:>7}")
        if outcome.error:
            print(f"    error: {outcome.error}")
    if summary.raw_data_dir and summary.dry_run:
        print(f"Dry-run raw files: {summary.raw_data_dir}")


def main(argv: Sequence[str] | None = None, *, settings: Settings | None = None) -> int:
    args = _parser().parse_args(argv)
    settings = settings or get_settings()
    setup_logging(settings, stream=sys.stderr)
    try:
        if args.command == "locations":
            return _print_locations(settings)
        if args.command == "forecast":
            summary = run_open_meteo("forecast", locations=args.locations, dry_run=args.dry_run,
                                     forecast_days=args.forecast_days, settings=settings)
        else:
            summary = run_open_meteo("historical", locations=args.locations, dry_run=args.dry_run,
                                     start_date=args.start_date, end_date=args.end_date,
                                     max_days=args.max_days, settings=settings)
    except (ValueError, ConfigurationError) as exc:
        logger.error("%s", exc)
        return 2
    _print_summary(summary)
    return 0 if summary.ok else 1


if __name__ == "__main__":
    sys.exit(main())
