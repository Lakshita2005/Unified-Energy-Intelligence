# Price Intelligence – Data Ingestion

Foundation for ingesting price data from external sources into PostgreSQL.
Source-specific pipelines are not implemented yet; this repository provides the
shared infrastructure they will build on.

## Layout

| Path | Responsibility |
|---|---|
| `config/settings.py` | Typed settings loaded from environment variables / `.env` |
| `src/common/logging_config.py` | Centralised logging (console + rotating file, text or JSON) |
| `src/common/database.py` | SQLAlchemy engine/session management, declarative `Base` |
| `src/common/http_client.py` | HTTPX client with timeouts, retry/backoff, typed errors |
| `src/common/raw_storage.py` | Write-once raw payload storage with checksums and metadata |
| `src/common/exceptions.py` | Project exception hierarchy |
| `src/ingestion/` | Source extractors (fetch raw data) |
| `src/transformations/` | Raw → canonical record transformations |
| `src/loaders/` | Persist canonical records to PostgreSQL |
| `src/pipelines/` | Orchestrate ingestion → storage → transform → load |
| `src/scheduler/` | Trigger pipelines on a schedule |
| `migrations/` | Alembic migrations (URL built from `DB_*` env vars) |
| `tests/unit/`, `tests/integration/` | Tests; integration tests need a live database |
| `scripts/` | Operational scripts |
| `raw_data/`, `logs/` | Default local data and log directories (git-ignored) |

## Setup

```bash
python -m venv .venv
.venv\Scripts\activate            # Windows  (source .venv/bin/activate on Linux/macOS)
pip install -r requirements.txt
copy .env.example .env            # then fill in DB_HOST, DB_NAME, DB_USER (and DB_PASSWORD if needed)
python -m scripts.check_db_connection
```

## Database health check

```bash
python -m scripts.db_health_check          # human-readable report
python -m scripts.db_health_check --json   # JSON on stdout (logs go to stderr)
```

Runs in a read-only transaction and verifies the connection, that the current
database/user match `DB_NAME`/`DB_USER`, that `DB_SCHEMA` (default `pi`) exists
and is usable, and that every table in `config/expected_schema.py` is present.
It lists all tables with column counts. Missing tables are reported, never
created. Exit codes: `0` healthy, `1` unhealthy, `2` configuration error.

The `pi` schema is managed outside Alembic (there is no `alembic_version`
table). `migrations/env.py` excludes tables without ORM models, so autogenerate
will never propose dropping them. Agree on a baseline strategy before running
`alembic upgrade`, which would create `alembic_version`.

## Ingestion framework

Source pipelines subclass `src.ingestion.base.BaseIngestionPipeline`, declare
`source`, `dataset`, `target_table` and `natural_key`, and implement:

- `fetch(context)` → `RawDocument`s, unmodified, with `retrieved_at` (ours) and
  `published_at` (the source's) kept separate. Use `self.http` for HTTP.
- `parse(document, context)` → `ParseResult` (records + parse failures). No DB access.
- `validator()` (optional) → rules from `src.ingestion.validation`.

`run()` handles everything else: registration in `pi.data_source` /
`pi.source_dataset`, `pi.ingestion_run` status tracking, raw storage with
SHA-256 (before any DB work, never deleted), `pi.source_document`
(dedupe on checksum), idempotent loading via `src.loaders.table_loader.TableLoader`,
rejected records in `pi.data_quality_issue` plus a `<dataset>-rejected` raw file,
and retries for transient network failures. Each document is one transaction.

`tests/support/mock_pipeline.py` is a complete worked example (test-only).

## Open-Meteo pipeline

Hourly forecast and historical weather for configured locations into
`pi.open_meteo_weather`. Locations go in `config/weather_locations.toml`
(copy the `.example.toml`). Full details: `docs/open_meteo_pipeline.md`.

**Licence and usage (free plan, `OPEN_METEO_PLAN=free`, the default):** the free endpoint
`https://api.open-meteo.com/v1/forecast` needs no API key and may be used for **non-commercial purposes
only**, with fewer than **10,000 API calls a day**. Keep `OPEN_METEO_API_KEY` empty: with a key, the
pipeline switches to the paid `customer-` hosts, so the scheduler refuses that combination unless
`OPEN_METEO_PLAN=commercial`. A commercial plan needs a paid subscription and its own key.

**Attribution:** Open-Meteo data is licensed under
[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/). Anything that shows or shares it must credit
"Weather data by [Open-Meteo.com](https://open-meteo.com/)" (CC BY 4.0) and say if the data was changed.

```bash
python -m scripts.ingest_open_meteo locations
python -m scripts.ingest_open_meteo forecast --location SITE_CODE --dry-run
python -m scripts.ingest_open_meteo historical --location SITE_CODE --start-date 2026-11-01 --end-date 2026-11-05
```

`TEST_ONLY_DELHI_GRID` (in `config/weather_locations.test_only.toml`) is a
technical test point, not a plant. Its data must not be used for analysis, and
it is never included unless named with `--location`.

## NPP pipeline

All-India real-time demand and generation (source: MERIT India, via undocumented
NPP dashboard endpoints) into `pi.npp_demand_generation`. One run per IST day,
at most 31 days per command. Full details: `docs/npp_pipeline.md`.

```bash
python -m scripts.ingest_npp demand --date 2026-11-11 --dry-run
python -m scripts.ingest_npp generation --start-date 2026-11-01 --end-date 2026-11-11
```

## IEX pipeline

DAM, GDAM and RTM 15-minute clearing prices (MCP as area `MKT`, plus 13 bid-area
prices for DAM/GDAM) into `pi.iex_market_price`. IEX's website terms restrict use
to personal, non-commercial purposes unless authorised. Full details:
`docs/iex_pipeline.md`.

```bash
python -m scripts.ingest_iex reference
python -m scripts.ingest_iex dam --date 2026-11-10 --dry-run
python -m scripts.ingest_iex rtm --start-date 2026-11-01 --end-date 2026-11-10
```

## CEA pipeline

CEA daily coal stock (`cea_coal_stock_daily`), unit outages from DGR sub-report 10
(`cea_dgr_unit_outage`), station generation and coal stock days from DGR sub-report 2
(`cea_dgr_plant_daily`), and the monthly state-wise energy / peak power supply position
(`cea_power_supply_position`, Alembic migration `0001_cea`). The plant / unit / state
reference data is seeded from CEA reports by an explicit command. DGR reports state
"for operational purpose only and not for commercial purpose". Full details:
`docs/cea_pipeline.md`.

```bash
alembic upgrade head                                   # once: applies 0001_cea
python -m scripts.ingest_cea reference --dry-run       # then without --dry-run
python -m scripts.ingest_cea coal --date 2026-11-11 --dry-run
python -m scripts.ingest_cea outages --start-date 2026-11-05 --end-date 2026-11-11
python -m scripts.ingest_cea psp-energy --month 2026-08
```

## Grid-India pipeline

NLDC Daily PSP workbook (XLS from 2023): each workbook is downloaded and stored
once and parsed by one pipeline per table, each with its own ingestion run.
Implemented: Section A (`grid_daily_power_supply`, All India = NULL region),
Sections C + J (`grid_state_power_supply`), Section G (`grid_sourcewise_generation`),
Section F (`grid_generation_outage`), Section E (`grid_regional_exchange`, migration `0003`) and IR-Line
(`grid_interregional_line_exchange`, `grid_international_line_exchange`, migration `0004`) and CrossBorder
(`grid_crossborder_market_schedule`, Export/Import/Net schedules, migration `0005`; country rows via
`python -m scripts.ingest_grid_india reference`) and TimeSeries (`grid_15min_scada`, all-India instantaneous
15-minute SCADA, migration `0006`). Availability: actual upload time
from 2025-05-12, else an explicit D+2 09:00 IST estimate (Alembic `0002_grid_india`).
Terms of use not yet verified. Full details: `docs/grid_india_pipeline.md`.

```bash
python -m scripts.ingest_grid_india psp --date 2026-11-10 --dry-run
python -m scripts.ingest_grid_india psp --start-date 2026-11-04 --end-date 2026-11-09
```

## WBES pipeline (PX Dashboard)

Grid-India WBES power-exchange schedules behind a personal login: one PX response per
delivery date x exchange x market feeds `wbes_px_region_schedule`, `wbes_px_link_schedule` and
`wbes_px_entity_schedule` (migration `0007_wbes_px`). Independent implementation; **live WBES
access is not yet authorised**, so it has only been run against mocked HTTP. Login is interactive
(you enter username, OTP, CAPTCHA); the session file lives outside the project and OneDrive; one
WBES process at a time (advisory lock). Units, CreatedOn format and region mapping are unverified.
Full details: `docs/wbes_pipeline.md`.

```bash
python -m scripts.ingest_wbes login
python -m scripts.ingest_wbes px --date 2026-11-10 --exchanges IEX --markets DAM --dry-run
```

## IMD pipeline (AWS observations, city forecasts)

India Meteorological Department API (`api.imd.gov.in`): AWS/ARG stations and observations
(`imd_station`, `imd_station_observation`) and 7-day city forecasts (`imd_city_forecast`, one
vintage per issue date). Separate from Open-Meteo. **Not live:** IMD access needs approval and IP
whitelisting (not yet authorised); migration `0008_imd` is applied and both datasets are registered. Units, timestamps and
identifiers are unverified. Full details: `docs/imd_pipeline.md`.

```bash
python -m scripts.ingest_imd aws --dry-run
python -m scripts.ingest_imd city-forecast --dry-run
```

## MOSDAC pipeline (solar and wind energy forecasts)

MOSDAC (ISRO SAC) `apienergy/solar` and `apienergy/wind` forecasts per plant location
(`config/mosdac_locations.toml`, eight plants) into `mosdac_solar_forecast` (air temperature, solar
flux, not GHI) and `mosdac_wind_forecast` (speed and direction, units unverified). Every forecast
series and revision is kept. Separate from Open-Meteo and IMD, and not a confirmed IMD replacement.
Migration `0009_mosdac` is applied and both datasets are registered; no ingestion run yet. Full details: `docs/mosdac_pipeline.md`.

```bash
python -m scripts.ingest_mosdac locations
python -m scripts.ingest_mosdac all --location ESEPL --dry-run
```

## Scheduler and worker (availability-based)

`pi-scheduler` runs source-specific availability checks (new CEA reports, IEX delivery dates and RTM
blocks, Grid-India workbooks, NPP snapshots, MOSDAC series, Open-Meteo refresh slots) and turns new or
revised data into jobs in a PostgreSQL queue (schema `pi_jobs`). `pi-worker` executes the jobs by
calling the existing ingestion runners. There is no Celery or broker. The scheduler has leases,
retries with exponential backoff, advisory locks and restart recovery.

**Deployed, not active:**
- every schedule in `config/scheduler.toml` is disabled, and `SCHEDULER_GLOBAL_ENABLE=false`;
- migrations up to `0011_pi_jobs` are applied, and the 13 schedules are synced to `pi_jobs.schedule` (all disabled);
- two Windows Task Scheduler tasks (`\PI\pi-scheduler`, `\PI\pi-worker`, account `LOCAL SERVICE`) are registered and disabled.

A schedule runs only when the global switch is on, it is enabled in the file, its configuration is synced
to the database unchanged (hash check), and an administrator enabled it in `pi_jobs.schedule` (the
application role `pi_app` cannot). Open-Meteo schedules are additionally refused when `OPEN_METEO_PLAN`
and `OPEN_METEO_API_KEY` disagree, or when their worst-case daily calls would exceed the free limit; on
the free plan they need no key (non-commercial use only).

Full details, including the Task Scheduler deployment and how to enable schedules: `docs/scheduler.md`.

```bash
python -m scripts.pi_jobs validate-config        # offline
python -m scripts.pi_scheduler run               # service process (after activation)
python -m scripts.pi_worker run                  # service process (after activation)
python -m scripts.pi_jobs status                 # includes file/database configuration sync
python -m scripts.pi_jobs enable IEX_RTM         # ADMIN role only, after approval
```

## Common commands

```bash
pytest                            # all tests; integration tests skip if DB_* is unset
pytest tests/unit                 # unit tests only
pytest -m integration             # integration tests only

alembic revision --autogenerate -m "describe change"
alembic upgrade head
alembic upgrade head --sql        # print SQL without connecting
```

When you add ORM models, subclass `src.common.database.Base` and import the
model module in `migrations/env.py` so autogenerate can detect them.

## Usage sketch

```python
from src.common.logging_config import setup_logging, get_logger
from src.common.http_client import HttpClient
from src.common.raw_storage import create_raw_storage_manager
from src.common.database import get_database_manager

setup_logging()
logger = get_logger(__name__)
storage = create_raw_storage_manager()

with HttpClient(base_url="https://api.example.com") as client:
    response = client.get("/prices", params={"date": "2026-01-01"})
    stored = storage.save_bytes(response.content, source="example_api",
                                dataset="daily_prices", extension="json")

with get_database_manager().session_scope() as session:
    ...
```

## Conventions

- **Secrets** come only from the environment. `.env` is git-ignored.
- **Raw data is immutable**: keys are `source/dataset/YYYY/MM/DD/dataset_<UTC ts>Z.ext`,
  each with a `.meta.json` sidecar containing size and SHA-256.
- **HTTP**: only idempotent methods are retried by default (pass `retry=True` to
  opt in for POST). Query strings are stripped from logs and errors.
- **Logging**: each worker process should use its own `LOG_FILE_NAME`
  (`RotatingFileHandler` is not multi-process safe).
