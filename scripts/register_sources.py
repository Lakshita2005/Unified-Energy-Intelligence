"""Register the Price Intelligence source/dataset catalogue and verify it.

Upserts every source and dataset in src/ingestion/catalog.py into
pi.data_source and pi.source_dataset (idempotent; safe to re-run), then reads
the records back and verifies them against the catalogue. Datasets listed as
pending in the catalogue are reported but not registered.

Usage (from the project root)::

    python -m scripts.register_sources                 # register, commit, verify
    python -m scripts.register_sources --dry-run       # register and verify, then roll back
    python -m scripts.register_sources --write-mapping docs/dataset_table_mapping.md

Exit codes: 0 success, 1 verification failed, 2 configuration error.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

from sqlalchemy import text

from config.settings import PROJECT_ROOT, Settings, get_settings
from src.common.database import DatabaseManager
from src.common.exceptions import ConfigurationError
from src.common.logging_config import get_logger, setup_logging
from src.ingestion.catalog import COMMON_TABLES, DATASETS, PENDING, PENDING_DATASETS, SOURCES
from src.ingestion.registration import Action, register_catalog, verify_catalog

logger = get_logger("scripts.register_sources")


def render_mapping_document() -> str:
    """Render the dataset -> destination table mapping as Markdown, from the catalogue."""
    lines = [
        "# Dataset to PostgreSQL table mapping",
        "",
        "Generated from `src/ingestion/catalog.py` by `python -m scripts.register_sources --write-mapping`. "
        "Do not edit by hand; change the catalogue and regenerate.",
        "",
        f"Values marked `{PENDING}` have not been verified against the publisher and must be confirmed "
        "before a pipeline relies on them. All tables are in the `pi` schema.",
        "",
        "## Sources",
        "",
        "| Code | Name | URL |",
        "|---|---|---|",
    ]
    for source in SOURCES.values():
        lines.append(f"| `{source.code}` | {source.name} | {source.url or f'— (no single site)'} |")

    lines += ["", "## Registered datasets", "",
              "| Dataset code | Source | Destination table(s) | Granularity | Collection | Timezone |",
              "|---|---|---|---|---|---|"]
    for entry in DATASETS:
        d = entry.definition
        tables = "<br>".join(f"`{table}`" for table in entry.destination_tables)
        lines.append(f"| `{d.code}` | `{entry.source_code}` | {tables} | {d.native_granularity} | "
                     f"{d.expected_frequency} | {d.default_timezone} |")

    lines += ["", "### Dataset details", ""]
    for entry in DATASETS:
        d = entry.definition
        lines += [f"#### `{d.code}` — {d.name}", "",
                  f"- **Source:** `{entry.source_code}`",
                  f"- **Endpoint / report:** {d.endpoint_or_report}",
                  f"- **Destination:** {', '.join(f'`{t}`' for t in entry.destination_tables)}"]
        if entry.notes:
            lines.append(f"- **Notes:** {entry.notes}")
        lines.append("")

    lines += ["## Pending datasets (not registered)", "",
              "These are intentionally absent from `pi.source_dataset` until their publisher is confirmed.", "",
              "| Proposed code | Dataset | Candidate publisher(s) | Destination table(s) | Why pending |",
              "|---|---|---|---|---|"]
    for pending in PENDING_DATASETS:
        tables = "<br>".join(f"`{table}`" for table in pending.destination_tables)
        lines.append(f"| `{pending.proposed_code}` | {pending.name} | {pending.candidate_publishers} | "
                     f"{tables} | {pending.reason} |")

    lines += ["", "## Tables used by every dataset", "",
              *[f"- `{table}`" for table in COMMON_TABLES],
              "",
              "Reference tables needed before loading: `market_master`, `market_area` (IEX); `plant`, "
              "`generating_unit`, `source_entity_mapping` (CEA, DSM); `state`, `region` (NPP, Grid-India); "
              "`weather_location` (Open-Meteo); `interconnector`, `country` (Grid-India exchange).",
              ""]
    return "\n".join(lines)


def _print_records(manager: DatabaseManager, settings: Settings) -> None:
    schema = settings.db_schema
    with manager.read_only_connection() as conn:
        print("\npi.data_source")
        for row in conn.execute(text(f"SELECT source_id, source_code, source_name, source_url, is_active "
                                     f"FROM {schema}.data_source ORDER BY source_id")):
            print(f"  {row.source_id:>3}  {row.source_code:<12} active={row.is_active!s:<5} {row.source_url or '-'}")
        print("\npi.source_dataset")
        for row in conn.execute(text(
                f"SELECT d.dataset_id, d.dataset_code, s.source_code, d.native_granularity, d.expected_frequency, "
                f"d.default_timezone, d.is_active FROM {schema}.source_dataset d "
                f"JOIN {schema}.data_source s USING (source_id) ORDER BY d.dataset_id")):
            print(f"  {row.dataset_id:>3}  {row.dataset_code:<26} {row.source_code:<11} "
                  f"{row.native_granularity:<21} {row.expected_frequency:<21} {row.default_timezone}")


def main(argv: Sequence[str] | None = None, *, settings: Settings | None = None) -> int:
    """Run registration and verification; return a process exit code."""
    parser = argparse.ArgumentParser(description="Register and verify the source/dataset catalogue.")
    parser.add_argument("--dry-run", action="store_true", help="roll back instead of committing")
    parser.add_argument("--write-mapping", metavar="PATH", help="write the mapping document and exit")
    args = parser.parse_args(argv)

    if args.write_mapping:
        path = Path(args.write_mapping)
        path = path if path.is_absolute() else PROJECT_ROOT / path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(render_mapping_document(), encoding="utf-8")
        print(f"Wrote {path}")
        return 0

    settings = settings or get_settings()
    setup_logging(settings, stream=sys.stderr)
    try:
        manager = DatabaseManager.from_settings(settings)
    except ConfigurationError as exc:
        logger.error("%s", exc)
        return 2

    schema = settings.db_schema
    try:
        with manager.engine.connect() as conn:
            transaction = conn.begin()
            try:
                report = register_catalog(conn, schema, SOURCES, DATASETS)
                problems = verify_catalog(conn, schema, SOURCES, DATASETS)
                if args.dry_run or problems:
                    transaction.rollback()
                else:
                    transaction.commit()
            except Exception:
                transaction.rollback()
                raise

        print(f"Sources:  {len(report.sources)} in catalogue")
        print(f"Datasets: {len(report.datasets)} in catalogue, {len(PENDING_DATASETS)} pending (not registered)")
        for action in Action:
            print(f"  {action.value:<9} {report.count(action)}")
        if problems:
            print("\nVerification FAILED (changes rolled back):")
            for problem in problems:
                print(f"  - {problem}")
            return 1
        print("\nVerification passed." + (" Dry run: changes rolled back." if args.dry_run else " Committed."))
        if not args.dry_run:
            _print_records(manager, settings)
        return 0
    except ConfigurationError as exc:
        logger.error("%s", exc)
        return 2
    finally:
        manager.dispose()


if __name__ == "__main__":
    sys.exit(main())
