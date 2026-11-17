# MOSDAC ingestion pipeline: solar and wind energy forecasts per plant

MOSDAC (ISRO Space Applications Centre) energy forecasts for configured plant locations. The source
is `MOSDAC`, kept entirely separate from Open-Meteo and IMD. **MOSDAC is not a confirmed
replacement for IMD.**

> **No ingestion run yet.** Migration `0009_mosdac` is **applied** (2026-11-17) and the source and
> both datasets are registered; the tables are empty. One live read-only probe per endpoint (ESEPL,
> 2026-11-17): solar HTTP 200, wind **HTTP 500**.

## Status

| Item | State |
|---|---|
| Alembic `0009_mosdac` (2 tables + 6 lookup indexes) | ✅ applied 2026-11-17 to `price_intelligence_dev` after a rolled-back dry run (22/22); head `0009_mosdac` |
| Source `MOSDAC`, datasets `MOSDAC_SOLAR_FORECAST`, `MOSDAC_WIND_FORECAST` | ✅ in `SOURCES`/`DATASETS` and registered (source_id 16602; dataset_id 20935, 20936); mapping doc regenerated |
| Client, parser, pipelines, runner, CLI | implemented; 50 unit + 16 integration tests (mocked HTTP; the "refuses without migration" test now skips because 0009 is applied) |
| Live | solar verified (structure, timestamps, count, spacing); wind returned HTTP 500 |
| Scheduling, backfill | none (MOSDAC serves only its current series) |

## Endpoints

| Command | Endpoint | Pipeline | Table |
|---|---|---|---|
| `solar` | `GET https://mosdac.gov.in/apienergy/solar?lat=<lat>&lon=<lon>` | `MosdacSolarPipeline` | `mosdac_solar_forecast` |
| `wind` | `GET https://mosdac.gov.in/apienergy/wind?lat=<lat>&lon=<lon>` | `MosdacWindPipeline` | `mosdac_wind_forecast` |
| `all` | both | both | both |

One ingestion run per endpoint × location, so a failing endpoint or plant never discards another's
data (partial success; exit code 1).

## Observed response (live, 2026-11-17, ESEPL 17.4042, 77.3753)

**Solar:** HTTP 200 and `application/json`, 16,011 bytes (Apache server). The body is a JSON **list
of 289 objects**, each `{"time": "13-11-26 0530", "temp": "16.91", "flux": "0.0"}`, and **every value
is a string**.
- **Window:** 13-11-26 05:30 to 16-11-26 05:30 IST (72 h), exactly 15-minute spacing, no gaps or
  duplicates.
- **Values:** temp 13.49–33.59; flux 0–908.94, zero at night.
- **Stale:** the series ended about 29 h before retrieval (17-11 10:36 IST), so every point is past.
- **No issue time** is published anywhere in the body or headers.

**Wind:** HTTP 500 `{"status": "Internal Server Error", "message": "The server encountered an internal
error ... overloaded or there is an error in the application."}`. Its structure is unverified.

The brief describes the fields as arrays, but the live solar body is a list of objects. Both forms
are accepted: the observed list of objects, and an object of equal-length arrays. Arrays of unequal
length reject the whole response.

## Field mapping

| Published | Column | Status |
|---|---|---|
| `time` (`DD-MM-YY HHMM`, IST) | `valid_at` (aware), `source_time_text` (as published) | IST per the supplied description; the live diurnal flux curve (peak about 12:00 IST) is consistent |
| solar `temp` | `air_temperature_c` | degC per the supplied description (values plausible; not independently verified) |
| solar `flux` | `solar_flux_w_m2` | W/m² per the supplied description. **Not GHI; no DNI/DHI/GTI derived** |
| wind `speed` | `wind_speed_value` | unit **unverified** |
| wind `direction` | `wind_direction_value` | unit **unverified** |
| — | `series_start_at` | earliest valid time in the response (identifies the forecast series) |
| request | `location_code`, `latitude`, `longitude` | from `config/mosdac_locations.toml` |

## Locations

`config/mosdac_locations.toml` (`MOSDAC_LOCATIONS_FILE`) uses the weather-location `[[location]]`
format and the same validator, holding the eight plants supplied 2026-11-17. Coordinates are sent
exactly as written (e.g. `78.0630`).

The plants are **not** registered in `pi.weather_location`, because Open-Meteo's all-locations
runs include every registered weather location. The plant codes are not in `pi.plant`. A code
already stored with different coordinates is refused, since locations are never moved.

## Raw storage, vintages and idempotency

Each response produces two write-once files under `RAW_DATA_DIR`, each with SHA-256 and a
`.meta.json` sidecar:

```
mosdac/mosdac_solar_forecast/esepl-response/YYYY/MM/DD/<ts>Z.json   original body, unchanged (HTTP status in sidecar)
mosdac/mosdac_solar_forecast/esepl/YYYY/MM/DD/<ts>Z.json            request-bound document -> source_document
mosdac/mosdac_solar_forecast-rejected/esepl/YYYY/MM/DD/<ts>Z.json   rejected source records (if any)
```

**Why a request-bound document:** the response does not name its location, so two plants can get
byte-identical bodies. If the bare body were registered, the second plant would be a "duplicate
document" and its rows would be lost; a test caught exactly this. The registered document is
deterministic: endpoint, URL, location, coordinates, the exact response text and its SHA-256.
Parsing verifies that SHA-256. The sidecar links to the original file (`response_raw_key`,
`response_sha256`).

**Vintages:**
- **Natural key:** (`location_code`, `series_start_at`, `valid_at`) under the VERSION policy, with
  no unique constraint.
- **New series:** a new `series_start_at` gives new keys.
- **Revised re-fetch of the same series:** new versions of only the changed rows; earlier rows are
  kept.
- **Byte-identical re-fetch for the same location:** `DUPLICATE_DOCUMENT`; nothing is reloaded and
  the raw files are still kept.
- **Availability:** no issue time is published, so `availability_basis = RETRIEVED_AT` and
  `source_published_at` is NULL.

## Data-quality rules

| Situation | Handling |
|---|---|
| HTTP 5xx/408/425/429, connection errors (incl. SSL EOF), timeouts | retried `HTTP_MAX_RETRIES` times with exponential backoff, then the run FAILS (the fetch is not repeated on top) |
| Other 4xx | not retried; run FAILED |
| Not JSON; an error object; non-object items; records without the expected keys | run FAILED (`MosdacResponseError`); raw files kept |
| Unequal array lengths (columnar form) | run FAILED; raw files kept |
| Invalid `time`, non-numeric value | record rejected (`PARSE_ERROR`), kept in the `-rejected` raw file |
| null / empty string | stored NULL, `MISSING_VALUE` |
| Measurement key absent | stored NULL, `MISSING_FIELD` |
| Extra fields | `UNDOCUMENTED_FIELDS` (raw only) |
| Duplicate timestamps | `DUPLICATE_TIMESTAMP`; identical copies loaded once (`DUPLICATE_IN_BATCH`); conflicting copies all rejected |
| Missing 15-min steps / off-grid spacing / unordered | `MISSING_TIMESTAMPS` / `IRREGULAR_SPACING` / `OUT_OF_ORDER` |
| Count ≠ 289 | `POINT_COUNT` INFO, never rejected |
| All valid times ≤ retrieval / some | `STALE_FORECAST` WARNING / `POINTS_BEFORE_RETRIEVAL` INFO |
| flux < 0, temp outside −40–60, speed < 0, direction outside 0–360 | `OUT_OF_RANGE` WARNING, stored as published |

## Running

```bash
python -m scripts.ingest_mosdac locations
python -m scripts.ingest_mosdac solar --location ESEPL --dry-run
python -m scripts.ingest_mosdac all --location ESEPL
```

**Exit codes:** 0 when every run succeeds; 1 for a PARTIAL or FAILED run, or a refused location; 2
for invalid arguments or configuration (including a missing migration, which is checked before any
request).

## Open items

1. **Permission:** MOSDAC's data guidelines give general users free access but forbid
   reselling or redistributing downloaded products. Business and industry use is governed by
   agreement. Confirm Resolven's use with SAC/MOSDAC.
2. **Wind:** HTTP 500 on the only attempt. Its structure and units are unverified.
3. **Issue time:** not published. The solar series was already 29 h old when retrieved, and how
   often MOSDAC refreshes the forecast is unknown.
4. **Interpretation:** `temp`/`flux` units and the IST time zone come from the supplied
   description. They are consistent with the data but not confirmed by MOSDAC documentation.
5. **Coordinates:** whether MOSDAC snaps them to a grid (identical bodies for nearby plants) is
   unknown.
