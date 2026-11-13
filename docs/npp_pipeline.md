# NPP ingestion pipeline

All-India real-time demand and generation from the National Power Portal
(NPP), loaded into `pi.npp_demand_generation`. The data originates from MERIT
India; NPP labels it "Real Time Generation and Demand Data (Source-MERIT INDIA)".

## Status

| Check | State |
|---|---|
| Unit tests (parser, endpoints, date guards, null-safe loader) | ✅ passing |
| End-to-end tests on the real `pi` schema, mocked API, rolled back | ✅ passing |
| Live dry run, both datasets | ✅ 2026-11-12, see [Live verification record](#live-verification-record) |
| Live ingestion and re-run, both datasets | ✅ 2026-11-12, one day (2026-11-11) |
| Scheduling | not configured |

## Scope

| Dataset | Endpoint | Series (`name_of_data`) |
|---|---|---|
| `NPP_DEMAND` (194) | `GET {NPP_BASE_URL}/demandmet1chartdata?date=YYYY-MM-DD` | `DEMAND MET` |
| `NPP_GENERATION` (195) | `GET {NPP_BASE_URL}/demandmet2chartdata?date=YYYY-MM-DD` | `THERMAL GENERATION`, `HYDRO GENERATION`, `GAS GENERATION`, `NUCLEAR GENERATION`, `SOLAR GENERATION`, `WIND GENERATION` |

- `NPP_BASE_URL` defaults to `https://npp.gov.in/dashBoard`.
- Both endpoints are **undocumented**. They are the JSON behind the dashboard at `https://npp.gov.in/dashBoard/gc-map-dashboard-meritchart`, and were found by reading that page's code and verified live on 2026-11-12.
- No authentication is required.
- Out of scope: NPP's state-wise, zone-wise and other dashboard data (installed capacity, daily generation, coal stock, PLF and so on).

## Response format

```json
[{"updated_on": 1794335493000, "name_of_data": "DEMAND MET", "value_of_data": 158519}, ...]
```

- **One IST calendar day per request.** The `date` parameter is always sent; without it, NPP silently returns today.
- **Irregular snapshots,** roughly every 2–4 minutes and at irregular seconds (for example 00:01:33).
- **Values** are integers, in MW. The dashboard labels them "(MW)"; the response itself carries no unit.
- **History** starts between 2025-09-02 and 2025-10-01. The pipeline refuses dates before `2025-10-01` (`api.HISTORY_START`).
- **Empty responses:** NPP answers `[]` (HTTP 200) for a day with no data, a future date or a malformed date alike, so an empty response always fails the run.
- **Repeat fetches:** a past day re-fetched gives the same data, but the generation endpoint may return it in a **different item order**, so the bytes, and therefore the checksum, differ.
- **Today's response grows** during the day.

## Field mapping

| NPP field | Meaning | Unit | Column | Notes |
|---|---|---|---|---|
| `updated_on` | MERIT India snapshot time; the only timestamp provided | epoch ms (UTC) | `source_updated_on_ms` (unchanged) and `observed_at` (timestamptz, exact to the millisecond) | **Never rounded** to 15-minute blocks or resampled |
| `name_of_data` | Series name | — | `name_of_data` (verbatim) | Unknown names are rejected (`UNKNOWN_SERIES`) |
| `value_of_data` | Instantaneous All-India value | MW | `value_of_data` (exact `Decimal`) | Must be 0 to 500,000 |
| — | Unit | — | `unit_code = 'MW'` | Set explicitly |
| — | Geography | — | `state_code = NULL`, `region_code = NULL` | NULL/NULL means **All-India** |

Lineage comes from `ingestion_run_id` and `document_id`. The dataset can be traced through `source_document.dataset_id`.

**THERMAL GENERATION:** NPP and MERIT India don't document what it includes, so it is stored exactly as published.
- It must **not** be treated as coal-only generation.
- Gas and nuclear are reported separately, but whether lignite or other fuels are included is unknown.
- The six generation series need not add up to `DEMAND MET`. There is no "total" or "other renewables" series (biomass, small hydro), and imports and exports aren't shown.

## Architecture

```
src/pipelines/npp/
  api.py       endpoints (NppEndpoint: path + expected series), date guards, HTTP call
  parser.py    response -> records (pure; structural checks, per-item failures)
  pipeline.py  NppDemandPipeline / NppGenerationPipeline (BaseIngestionPipeline) + validation
  runner.py    one ingestion run per day, 31-day cap, dry-run context
  errors.py    NppApiError / EmptyResponseError / InvalidResponseError
scripts/ingest_npp.py   CLI
```

Each day is a separate ingestion run (its own `pi.ingestion_run` row), so a failing day never affects the others.

## Duplicates and idempotency

- **Document level:** byte-identical responses are recognised by their SHA-256 checksum. They are recorded as an INFO `DUPLICATE_DOCUMENT` issue and not reloaded.
- **Record level:** the natural key is (`name_of_data`, `observed_at`, `state_code`, `region_code`), with the VERSION policy.
  - Re-fetching a day inserts only snapshots not seen before.
  - An unchanged snapshot is skipped.
  - A snapshot whose value changed gets a new row tied to the new run, and earlier rows are kept.
- **Null-safe matching:** `TableLoader` compares `state_code` and `region_code` with `IS NOT DISTINCT FROM`, so NULL (All-India) matches NULL. All-India rows never collide with rows that have a state or region.
  - This is enabled with `nullable_key_columns`. Pipelines that don't set it, such as Open-Meteo, behave exactly as before.
  - PostgreSQL turns `IS NOT DISTINCT FROM NULL` into `IS NULL` and uses the existing `ix_npp_geo_time` index (checked with `EXPLAIN ANALYZE`).
- **No database unique constraint:** there is none on the natural key, so duplicates are prevented by the app, under an advisory lock. A unique index was proposed separately and is **not applied**.

## Validation and failure handling

| Situation | Result |
|---|---|
| HTTP error (after retries for 408/425/429/5xx) | Run FAILED, `NppApiError` |
| `[]` | Run FAILED, `EmptyResponseError` |
| Not JSON (for example an HTML maintenance page), not an array, items without exactly the three fields, or none of the expected series | Run FAILED, `InvalidResponseError`; **nothing loaded** |
| One item with a bad timestamp, name or value, or a timestamp outside the requested IST day | That item rejected (`PARSE_ERROR`); the rest load; run PARTIAL |
| Value outside 0–500,000 MW | Rejected (`OUT_OF_RANGE`); run PARTIAL |
| Unknown series | Rejected (`UNKNOWN_SERIES`); run PARTIAL |

In every case the raw response is saved **before** parsing. Rejected records also go to `pi.data_quality_issue` and to a `npp_<dataset>-rejected` raw file.

## Raw file storage

```
RAW_DATA_DIR/npp/<dataset>/<requested IST date>/<YYYY>/<MM>/<DD>/<retrieved UTC timestamp>Z.json (+ .meta.json)
```

- Files are write-once and never deleted. Every fetch is kept, including duplicates.
- `source_document` records the URI, the SHA-256 and `source_report_date` (the requested day). `raw_payload` stays NULL.

## Running

```bash
python -m scripts.ingest_npp demand --date 2026-11-11 --dry-run       # full run, rolled back
python -m scripts.ingest_npp demand --date 2026-11-11
python -m scripts.ingest_npp generation --date 2026-11-11
python -m scripts.ingest_npp generation --start-date 2026-10-01 --end-date 2026-10-31   # max 31 days
```

- **Dry run:** calls the real endpoint and executes everything, but rolls back every database change. Raw files go to a temporary directory, which is printed.
- **Refused before any request:** ranges over 31 days, future dates, dates before 2025-10-01, and a reversed range.
- **Exit codes:** `0` every day SUCCESS, `1` any day PARTIAL or FAILED, `2` invalid arguments.
- **Scheduling:** none is configured. Today's date can be ingested repeatedly; each run adds only the new snapshots.

## Live verification record

Run on 2026-11-12 for IST day 2026-11-11:

| Step | Run | Status | Read | Inserted | Unchanged | Duplicate doc |
|---|---|---|---|---|---|---|
| Demand dry run (rolled back) | 377 | SUCCESS | 557 | 557 (rolled back) | 0 | 0 |
| Generation dry run (rolled back) | 378 | SUCCESS | 3,342 | 3,342 (rolled back) | 0 | 0 |
| Demand | 379 | SUCCESS | 557 | 557 | 0 | 0 |
| Generation | 380 | SUCCESS | 3,342 | 3,342 | 0 | 0 |
| Demand re-run | 381 | SUCCESS | 557 | **0** | — | 1 (identical bytes) |
| Generation re-run | 382 | SUCCESS | 3,342 | **0** | 3,342 | 0 (same items, different order; matched record by record) |

Checks after the runs:

- 3,899 rows: 557 for each of the 7 series, from 2026-11-11 00:01:33 to 23:59:33 IST.
- No natural key appears twice.
- Every row has NULL `state_code` and `region_code`, and `unit_code = 'MW'`.
- `observed_at` equals `updated_on` exactly in every row, and no snapshot was rounded.
- 0 rejected records.
- 3 `source_document` rows, each with a SHA-256 that matches its raw file.

## Known limitations

- **Undocumented endpoints:** NPP may change or remove them without notice. Structural changes fail the run safely, and the raw response is kept for diagnosis.
- **Ambiguous empty responses:** `[]` is ambiguous, so a genuinely empty day can't be told apart from a bad request.
- **History start:** the start date (about 2025-10-01) was found by probing and isn't documented.
- **Snapshot meaning:** MERIT India doesn't document exactly what `updated_on` represents. It is treated as the observation time.
- **Series composition:** THERMAL's composition is unknown, and there are no state or region breakdowns.
- **No database-level uniqueness:** see the proposed unique index, which is not applied.
