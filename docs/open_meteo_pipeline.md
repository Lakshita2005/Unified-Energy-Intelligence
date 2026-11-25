# Open-Meteo ingestion pipeline

Hourly weather for configured locations, from the Open-Meteo forecast and
historical APIs, loaded into `pi.open_meteo_weather`.

## Status

| Check | State |
|---|---|
| Unit tests (parser, API parameters, locations), mocked API | ✅ passing |
| End-to-end tests on the real `pi` schema, mocked API, rolled back | ✅ passing |
| Live technical test (API, parsing, DB insert, raw JSON, duplicates), `TEST_ONLY` location | ✅ passed 2026-11-12, see [Technical test record](#technical-test-record-test_only) |
| **Live ingestion for real plant locations** | ⏳ **pending**: needs verified plant coordinates |

The only registered location is `TEST_ONLY_DELHI_GRID`, a synthetic technical
test point. It is **not a plant**. Its data must not be used for forecasting or
analysis: exclude `location_code LIKE 'TEST_ONLY_%'` in any downstream query.
`pi.plant` is still empty. No scheduled or recurring ingestion has been set up.

## Architecture

```
scripts/ingest_open_meteo.py         CLI: forecast | historical | locations
        │
src/pipelines/open_meteo/runner.py   locations → one ingestion run per location; dry-run wrapper
        │
src/pipelines/open_meteo/pipeline.py OpenMeteoForecastPipeline / OpenMeteoHistoricalPipeline
        │   (subclasses src/ingestion/base.py BaseIngestionPipeline)
        ├── api.py        endpoints, variables, request parameters, HTTP call (HttpClient: timeouts + retries)
        ├── parser.py     JSON → records (pure; no DB or network)
        ├── locations.py  TOML location config → pi.weather_location
        └── errors.py     OpenMeteoApiError | EmptyResponseError | InvalidResponseError
```

Each location is a separate run of the shared framework:

1. Register (or reuse) the source/dataset. These already exist and are never modified.
2. Create a `pi.ingestion_run` row (RUNNING).
3. Call the API. Transient failures (timeouts, connection errors, 429, 5xx) are retried with exponential backoff.
4. Save the raw JSON **before parsing**, with a SHA-256 checksum.
5. Parse, then validate.
6. In one transaction: write `pi.source_document`, load the rows idempotently, and write a `pi.data_quality_issue` row for every rejected record.
7. Finish the run as SUCCESS, PARTIAL (some records rejected) or FAILED (with `error_message`).

## Datasets and tables

| Dataset (registered) | Endpoint | `is_forecast` | Destination |
|---|---|---|---|
| `OPEN_METEO_FORECAST` (id 203) | `https://api.open-meteo.com/v1/forecast` | `true` | `pi.open_meteo_weather` |
| `OPEN_METEO_HISTORICAL` (id 204) | `https://archive-api.open-meteo.com/v1/archive` | `false` | `pi.open_meteo_weather` |

Supporting tables: `pi.weather_location` (locations), `pi.ingestion_run`,
`pi.source_document`, `pi.data_quality_issue`.

### Variable → column mapping

| API hourly variable | Column | Unit (requested & validated) | API definition | Accepted range |
|---|---|---|---|---|
| `temperature_2m` | `temperature_2m_c` | °C | instantaneous | -90 … 60 |
| `shortwave_radiation` | `shortwave_radiation_wm2` | W/m² | preceding-hour mean; equals GHI | 0 … 1500 |
| `direct_radiation` | `direct_radiation_wm2` | W/m² | preceding-hour mean | 0 … 1500 |
| `diffuse_radiation` | `diffuse_radiation_wm2` | W/m² | preceding-hour mean | 0 … 1500 |
| `wind_speed_10m` | `wind_speed_10m_ms` | m/s (`wind_speed_unit=ms`) | instantaneous | 0 … 120 |
| `cloud_cover` | `cloud_cover_pct` | % | instantaneous | 0 … 100 |
| `relative_humidity_2m` | `relative_humidity_2m_pct` | % | instantaneous | 0 … 100 |
| `precipitation` | `precipitation_mm` | mm | preceding-hour sum | 0 … 500 |

**Not populated:**
- `ghi_wm2`: Open-Meteo's GHI is `shortwave_radiation`, which is stored in `shortwave_radiation_wm2`. The value is not duplicated.
- `wind_speed_80m_ms`: not requested.

### Request parameters

These parameters are sent on every request:

| Parameter | Value |
|---|---|
| `latitude`, `longitude` | From the location |
| `hourly` | The 8 variables above |
| `timezone` | `Asia/Kolkata` |
| `timeformat` | `iso8601` |
| `temperature_unit` | `celsius` |
| `wind_speed_unit` | `ms` |
| `precipitation_unit` | `mm` |

Each dataset adds its own range parameters:

- **Forecast:** `forecast_days` (1–16, default 7) and `past_days=0`.
- **Historical:** `start_date` and `end_date` (inclusive IST dates), split into requests of at most 31 days.

## Timestamp convention

- **Storage:** every timestamp is `timestamptz`, an absolute instant. PostgreSQL shows it in the session time zone (Asia/Kolkata on this server).
- **`valid_time`:** the hour the value applies to, exactly as the API returns it. Requests use `timezone=Asia/Kolkata`, so the API returns IST wall-clock times. The parser attaches the response's `utc_offset_seconds` (+19800, since India has no DST). For preceding-hour variables (radiation, precipitation), `valid_time` is the *end* of the hour.
- **`retrieved_at`:** when this system received the response (UTC). The same value is on `source_document.retrieved_at`.
- **`forecast_issued_at`:** always **NULL**. The API does not expose the model initialisation time, and it is never derived from valid or retrieval time.
- **`source_document.published_at`** and **`ingestion_run.source_published_at`:** NULL, for the same reason.
- **Date ranges:** start and end dates are IST calendar days.

## Duplicates and forecast vintages

`pi.open_meteo_weather` has no unique constraint, so an `ON CONFLICT` upsert
is not possible. Duplicate prevention uses the framework's `TableLoader`:

- **Natural key:** `(weather_location_id, valid_time, is_forecast)`.
- **Locking:** a per-table PostgreSQL advisory lock serialises concurrent loads.
- **Comparison:** the latest existing row for each key is compared on the measurement columns. `retrieved_at` and `ingestion_run_id` are ignored.
  - Identical values: the row is **skipped** (counted as unchanged).
  - Changed values: a **new row** is inserted for the new run (policy `VERSION`). Earlier forecasts are never overwritten.
- **Identical responses:** a response with byte-identical content is detected by its checksum (`source_document` is unique on `(dataset_id, checksum_sha256)`) and is not reloaded. Live responses always differ slightly (`generationtime_ms`), so in practice the record-level check does the work.

To reconstruct the forecast as known at time *T*, take, for each `valid_time`,
the latest row with `retrieved_at <= T` and `is_forecast`. Forecast and
historical rows for the same hour coexist, distinguished by `is_forecast`.

## Validation and failure handling

| Situation | Result |
|---|---|
| HTTP 4xx (e.g. invalid coordinates) | Run FAILED, `error_message` starts `OpenMeteoApiError:` and includes the API's `reason`. Not retried. |
| Timeout, connection error, 429, 5xx | Retried with backoff by `HttpClient`, plus one whole-fetch retry. After that: FAILED (`HttpRequestError` / `OpenMeteoApiError`). |
| HTTP 200 with no hours | FAILED, `EmptyResponseError`. Raw file kept. |
| HTTP 200 with an invalid structure (non-JSON, wrong timezone or units, missing or mismatched series) | FAILED, `InvalidResponseError`. Raw file kept, nothing loaded. |
| Hour with a non-numeric value or bad timestamp | Record rejected (`PARSE_ERROR`); the run continues and ends PARTIAL. |
| Value outside the accepted range | Record rejected (`OUT_OF_RANGE`); PARTIAL. |
| Every variable null for an hour (e.g. archive delay) | Record rejected (`ALL_MEASUREMENTS_NULL`); PARTIAL. |
| Some variables null | Loaded with SQL NULL (never 0). |
| Database error during load | The whole document's transaction is rolled back (no rows, no document, no issues). Run FAILED. Raw file kept. |
| One location fails | Other locations are unaffected. Each location has its own `ingestion_run`. |

Rejected records are stored in full under the `…-rejected` raw folder. Their
original values are also included in `data_quality_issue.issue_description`.

## Location configuration

No coordinates are built into the code. Supply locations in
`config/weather_locations.toml`, or a file named by `WEATHER_LOCATIONS_FILE`.
Copy `config/weather_locations.example.toml` and add one `[[location]]` table
per site:

```toml
[[location]]
code = "SITE_CODE"            # stable, upper case, max 40 chars
name = "Human-readable name"
latitude = 0.0                # WGS84, max 7 decimals
longitude = 0.0
coordinate_source = "Document/system that verifies these coordinates"
# elevation_m, plant_code, state_code, region_code are optional
```

- On first ingestion, each configured location is registered in `pi.weather_location` (idempotent on `location_code`).
- Locations already present in `pi.weather_location` (with a `location_code`) can be ingested without being in the file.
- A registered location is **never moved**. If configured coordinates differ from the registered ones, that location fails with a clear error; use a new code instead.
- `python -m scripts.ingest_open_meteo locations` lists what is configured and registered.
- **Test locations:** codes starting with `TEST_ONLY_` are technical test points. They may not set `plant_code`, and runs without `--location` never include them, even after they are registered. Keep them in `config/weather_locations.test_only.toml`, never in the production file.

**Coordinates still to be supplied:** every solar plant or forecasting region
that needs weather data. Adding them needs no code change: create
`config/weather_locations.toml` with the verified entries and run without
overriding `WEATHER_LOCATIONS_FILE`.

### Field reference

| Field | Required | Format / rule | Stored in |
|---|---|---|---|
| `code` | yes | `A-Z`, `0-9`, `_`, `-`; starts with a letter or digit; max 40 chars; never reused for other coordinates | `weather_location.location_code`, raw folder name |
| `name` | yes | non-empty text | `weather_location.location_name` |
| `latitude` | yes | WGS84 decimal degrees, -90…90, max 7 decimals (north positive) | `weather_location.latitude` |
| `longitude` | yes | WGS84 decimal degrees, -180…180, max 7 decimals (east positive) | `weather_location.longitude` |
| `coordinate_source` | yes | the document or system that verifies the coordinates | config file and logs only (no DB column) |
| `elevation_m` | no | metres above sea level | `weather_location.elevation_m` |
| `plant_code` | no | must already exist in `pi.plant` | `weather_location.plant_id` |
| `state_code` / `region_code` | no | must already exist in `pi.state` / `pi.region` | `weather_location.state_code` / `region_code` |

### Obtaining and verifying coordinates

**Where to get them**, in rough order of preference:

1. The plant's own records: commissioning certificate, land or lease documents, survey or GIS drawings, EPC as-built documents.
2. Grid connectivity or PPA documents that state the project site or the location of its pooling substation.
3. Site metadata held in the plant's SCADA, inverter or monitoring system, confirmed by the asset owner.

For a forecasting region rather than a single plant, document the method used to choose the representative point (for example, "capacity-weighted centroid of plants X, Y and Z").

**Checklist before adding a location:**

- [ ] The coordinates are in **decimal degrees, WGS84**. Convert degrees-minutes-seconds: `deg + min/60 + sec/3600`.
- [ ] The signs are right: Indian sites have **positive** latitude (about 6–37° N) and **positive** longitude (about 68–98° E). A swapped or negated pair is the most common error.
- [ ] Viewed on a satellite map, the point falls on the plant (panel arrays are visible) or inside the intended region. For large plants, use the centroid of the array.
- [ ] Use 4–5 decimal places. 5 decimals is about 1 m and 4 is about 11 m. Open-Meteo snaps to model grid cells of roughly 1–11 km, so more precision adds nothing.
- [ ] `coordinate_source` names the document and its date or version, so the value can be re-checked later.
- [ ] The code is new. A registered location cannot be moved; if a location was wrong, add a corrected one under a new code.

After the first ingestion, compare the grid-cell `latitude`, `longitude` and
`elevation` in the raw JSON with the configured point. A difference of more
than about 0.1° suggests the configured coordinates are wrong.

## Raw file storage

```
RAW_DATA_DIR/open_meteo/<dataset>/<location>/<YYYY>/<MM>/<DD>/<retrieved UTC timestamp>Z.json
RAW_DATA_DIR/open_meteo/<dataset>/<location>/<YYYY>/<MM>/<DD>/<…>Z.json.meta.json   (size, SHA-256, run id, URL)
RAW_DATA_DIR/open_meteo/<dataset>-rejected/<location>/…                           (rejected records, if any)
```

- `<dataset>` is `open_meteo_forecast` or `open_meteo_historical`, and `<location>` is the lower-cased location code.
- Files are write-once and are never overwritten or deleted.
- `source_document` stores the file URI and checksum. The JSON itself is **not** stored in PostgreSQL (`raw_payload` stays NULL).
- The stored request URL never contains the API key.

## Running

```bash
python -m scripts.ingest_open_meteo locations
python -m scripts.ingest_open_meteo forecast --location SITE_CODE --dry-run      # full run, rolled back
python -m scripts.ingest_open_meteo forecast --location SITE_CODE --forecast-days 3
python -m scripts.ingest_open_meteo forecast                                       # all locations
python -m scripts.ingest_open_meteo historical --location SITE_CODE --start-date 2026-11-01 --end-date 2026-11-05
```

- **Dry run:** calls the real API and executes everything, but rolls back every database change. Raw files go to a temporary directory, which is printed.
- **Historical ranges:** capped at `--max-days` (default 31), so a backfill must be requested explicitly.
- **Scheduling:** none is started.
- **Exit codes:** `0` all locations SUCCESS, `1` any location PARTIAL or FAILED, `2` invalid arguments or configuration.

## Technical test record (TEST_ONLY)

Run on 2026-11-12 against the live Open-Meteo API with
`config/weather_locations.test_only.toml`. The location is `TEST_ONLY_DELHI_GRID`,
a synthetic round-number point (28.6 N, 77.2 E) that is not a plant. Open-Meteo
snapped it to grid cell (28.576448, 77.18678), elevation 224 m.

```bash
WEATHER_LOCATIONS_FILE=config/weather_locations.test_only.toml   python -m scripts.ingest_open_meteo forecast --location TEST_ONLY_DELHI_GRID --forecast-days 1 [--dry-run]
WEATHER_LOCATIONS_FILE=config/weather_locations.test_only.toml   python -m scripts.ingest_open_meteo historical --location TEST_ONLY_DELHI_GRID --start-date 2026-11-01 --end-date 2026-11-02
```

| Step | Run | Status | Read | Inserted | Unchanged | Rejected |
|---|---|---|---|---|---|---|
| Forecast dry run (rolled back) | — | SUCCESS | 24 | 24 (rolled back) | 0 | 0 |
| Forecast, 1 day | 247 | SUCCESS | 24 | 24 | 0 | 0 |
| Forecast re-run | 248 | SUCCESS | 24 | 0 | 24 | 0 |
| Historical, 2026-11-01 to 2026-11-02 | 249 | SUCCESS | 48 | 48 | 0 | 0 |
| Historical re-run | 250 | SUCCESS | 48 | 0 | 48 | 0 |

Checks after the test:

- The dry run left no rows in any table.
- `open_meteo_weather` holds 72 rows: 24 forecast rows (2026-11-12 00:00–23:00 IST) and 48 historical rows (2026-11-01 00:00 to 2026-11-02 23:00 IST).
- No `(location, valid_time, is_forecast)` key appears twice.
- The 8 variables have no NULLs, and `forecast_issued_at` is NULL in every row.
- There are 0 data-quality issues.
- 4 raw JSON files are in `raw_data/open_meteo/…/test_only_delhi_grid/`. Each file's SHA-256 matches `source_document.checksum_sha256`, and `raw_payload` is NULL.
- The responses report timezone `Asia/Kolkata`, offset 19800 s, and the expected units (`°C`, `W/m²`, `m/s`, `%`, `mm`).
- `pi.plant` is still empty.

## Live verification for real locations (pending)

Run these steps once a verified location is in `config/weather_locations.toml`.
`SITE_CODE` is its code.

```bash
# 1. Check the configuration is read correctly (no writes, no API call)
python -m scripts.ingest_open_meteo locations

# 2. Dry run: real API call, full pipeline, every DB change rolled back
python -m scripts.ingest_open_meteo forecast --location SITE_CODE --forecast-days 1 --dry-run

# 3. Small real ingestion (24 hourly rows)
python -m scripts.ingest_open_meteo forecast --location SITE_CODE --forecast-days 1

# 4. Run it again: expect inserted=0 and unchanged=24, unless the forecast was revised in between
python -m scripts.ingest_open_meteo forecast --location SITE_CODE --forecast-days 1
```

Verify in PostgreSQL:

```sql
SELECT r.ingestion_run_id, r.run_status, r.records_read, r.records_inserted, r.records_rejected, r.error_message
FROM pi.ingestion_run r JOIN pi.source_dataset d USING (dataset_id)
WHERE d.dataset_code = 'OPEN_METEO_FORECAST' ORDER BY r.ingestion_run_id DESC LIMIT 5;

SELECT w.valid_time, w.retrieved_at, w.forecast_issued_at, w.temperature_2m_c, w.shortwave_radiation_wm2,
       w.wind_speed_10m_ms, w.cloud_cover_pct, w.relative_humidity_2m_pct, w.precipitation_mm
FROM pi.open_meteo_weather w JOIN pi.weather_location l USING (weather_location_id)
WHERE l.location_code = 'SITE_CODE' ORDER BY w.valid_time LIMIT 24;

-- Must return no rows: no duplicate hour with identical values
SELECT weather_location_id, valid_time, is_forecast, temperature_2m_c, count(*)
FROM pi.open_meteo_weather
GROUP BY weather_location_id, valid_time, is_forecast, temperature_2m_c, shortwave_radiation_wm2,
         direct_radiation_wm2, diffuse_radiation_wm2, wind_speed_10m_ms, cloud_cover_pct,
         relative_humidity_2m_pct, precipitation_mm
HAVING count(*) > 1;

SELECT document_id, checksum_sha256, raw_storage_uri, retrieved_at FROM pi.source_document
WHERE dataset_id = (SELECT dataset_id FROM pi.source_dataset WHERE dataset_code = 'OPEN_METEO_FORECAST')
ORDER BY document_id DESC LIMIT 5;
```

The API's `hourly_units` were confirmed in the technical test. If they ever
change, the run fails with `InvalidResponseError` and nothing is loaded.

To remove the technical test data later (this deletes rows, so only do it deliberately):

```sql
BEGIN;
DELETE FROM pi.open_meteo_weather WHERE weather_location_id =
  (SELECT weather_location_id FROM pi.weather_location WHERE location_code = 'TEST_ONLY_DELHI_GRID');
DELETE FROM pi.weather_location WHERE location_code = 'TEST_ONLY_DELHI_GRID';
COMMIT;
```

The run and document audit rows (`ingestion_run` 247–250, `source_document`
185–188) and the raw files can stay as a record of the test.

## Tests

```bash
pytest tests/unit/test_open_meteo_parser.py tests/unit/test_open_meteo_api.py tests/unit/test_open_meteo_locations.py
pytest tests/integration/test_open_meteo_pipeline.py     # real schema, rolled back; mocked API
pytest                                                   # everything
```

No test calls the live API.

## Known limitations

- **Licence.** The free API (`OPEN_METEO_PLAN=free`, the default, no API key) is for **non-commercial use only**: under 10,000 calls a day, 5,000 an hour and 600 a minute. Commercial use needs a paid plan: set `OPEN_METEO_PLAN=commercial` and `OPEN_METEO_API_KEY`, and requests go to the `customer-` hosts. **The pipeline itself switches to the `customer-` host whenever a key is set**, so keep the key empty on the free plan. The scheduler refuses the mismatch; manual `ingest_open_meteo` runs do not check it.
- **Attribution (CC BY 4.0).** Open-Meteo data is licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/). Anything that shows or shares it, such as reports, dashboards or exports, must credit "Weather data by [Open-Meteo.com](https://open-meteo.com/)", link to the licence, and say if the data was changed.
- **No forecast issue time.** `forecast_issued_at` is NULL, and vintages are identified by `retrieved_at` and the run. The API docs mention separate model-run metadata, but it is not used because it was not verified for the default "best match" model selection.
- **Grid-cell snapping.** Open-Meteo returns data for the nearest grid cell; the cell's coordinates and elevation are in the raw file. They are not stored in the database, because the schema has no columns for them.
- **Historical delay.** ERA5 has about a 5-day delay. Recent dates may return nulls, which are rejected as `ALL_MEASUREMENTS_NULL`; re-run later to fill them.
- **No database uniqueness constraint.** Duplicate prevention relies on the framework's advisory lock and natural-key comparison. Writes that bypass the framework are not protected. A proposed migration (not applied) is an index on `(weather_location_id, valid_time, is_forecast, ingestion_run_id)`, matching the CEA tables' pattern; see the README.
- **Whole-fetch retry.** A historical run's chunks are fetched together; if a later chunk fails, the run is retried as a whole.
