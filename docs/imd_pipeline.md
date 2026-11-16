# IMD ingestion pipeline: AWS observations and city forecasts (Phase 2)

India Meteorological Department data from the official API platform ([API reference](https://api.imd.gov.in/public/api_reference.html)). The source is `IMD`, kept entirely separate from Open-Meteo.

> **Not live.** IMD API access needs IMD approval (nodal officer) and IP whitelisting, and is not yet authorised; commercial-use permission is also pending. Everything has been run only against mocked HTTP built from the documented examples. Response structure, units, timestamps and identifiers are **UNVERIFIED** until authorised live testing. Migration `0008_imd` is **applied** (2026-11-16); the IMD tables are empty.

## Status

| Item | State |
|---|---|
| Alembic `0008_imd` (3 tables + lookup indexes) | ✅ applied 2026-11-16 to `price_intelligence_dev` after a rolled-back dry run (23/23); head `0008_imd` |
| Source `IMD` | ✅ registered (source_id 15041) |
| Datasets `IMD_AWS_OBS` → `imd_station`, `imd_station_observation`; `IMD_CITY_FORECAST` → `imd_city_forecast` | ✅ in `DATASETS` and registered (dataset_id 19106, 19107); mapping doc regenerated |
| Pipelines, parsers, client, CLI | implemented; tested with mocks only |
| Tests | 27 unit (`tests/unit/test_imd_parse.py`), 11 integration (`tests/integration/test_imd_pipeline.py`; the "refuses without the migration" test now skips because 0008 is applied) |
| Live verification, scheduling, backfill | not started |

## Endpoints and datasets

| Command | Endpoint (documented) | Pipeline | Dataset | Table |
|---|---|---|---|---|
| `aws` | `GET /aws_data_mapping` | `ImdStationPipeline` | `IMD_AWS_OBS` | `imd_station` |
| `aws` | `GET /aws_data` (optional `?id=<call sign>` or `?sid=<state id>`) | `ImdObservationPipeline` | `IMD_AWS_OBS` | `imd_station_observation` |
| `city-forecast` | `GET /cityforecast` (optional `?id=<station code>`) | `ImdCityForecastPipeline` | `IMD_CITY_FORECAST` | `imd_city_forecast` |

Each response is one raw document: a write-once file with SHA-256, registered in `source_document` and linked to its own ingestion run. It is parsed, validated and loaded in one transaction together with its data-quality issues. `aws` loads stations first, so observations can be linked to them.

## Field mapping (documented keys)

**`/aws_data` → `imd_station_observation`** (no units or time zone documented):

| Published | Column |
|---|---|
| `ID`, `CALL_SIGN` | `imd_id`, `call_sign` (both required) |
| `DATE` (`YYYY-mm-dd`), `TIME` (`HH:MM:SS`) | `observation_date`, `observation_time`, as published |
| — | `observed_at`: NULL while `IMD_AWS_TIME_ZONE=unverified`; DATE+TIME in that zone once set |
| `STATION`, `DISTRICT`, `STATE`, `Latitude`, `Longitude` | context columns (lineage); `state_code` from the exact state name |
| `CURR_TEMP`, `DEW_POINT_TEMP`, `RH`, `WIND_DIRECTION`, `WIND_SPEED`, `MSLP`, `MIN_TEMP`, `MAX_TEMP`, `Feel Like` | `curr_temp_value`, `dew_point_temp_value`, `rh_value`, `wind_direction_value`, `wind_speed_value`, `mslp_value`, `min_temp_value`, `max_temp_value`, `feels_like_value` (neutral names; units unverified) |
| `WEATHER_CODE`, `NEBULOSITY` | `weather_code`, `nebulosity_value` |
| — | `imd_station_id`: the latest `imd_station` row with exactly the same (ID, CALL_SIGN); NULL if none |

**`/aws_data_mapping` → `imd_station`:** the fields are **not documented**. The parser accepts records carrying the `/aws_data` identity keys (`ID`, `CALL_SIGN`, optionally `STATION`, `DISTRICT`, `STATE`, `Latitude`, `Longitude`). A response without them fails loudly and is never guessed.

**`/cityforecast` → `imd_city_forecast`:**

| Published | Column |
|---|---|
| `Station_Code`, `Station_Name` | `station_code` (required), `station_name` |
| `Date` | `issue_date`: the forecast vintage; the issue time is not published |
| `Todays_Forecast_Max_Temp` / `Todays_Forecast_Min_temp` / `Todays_Forecast` | lead 1 (`forecast_date = issue_date`) |
| `Day_<n>_Max_Temp` / `Day_<n>_Min_temp` / `Day_<n>_Forecast`, n = 2–7 | lead n (`forecast_date = issue_date + n − 1`) |
| → | `max_temp_value`, `min_temp_value` (documented °C, unverified), `forecast_text` |

The response's **observed and astronomical fields** (`Today_Max_temp`, departures, `Past_24_hrs_Rainfall`, RH at 08:30 and 17:30, sunrise and moonrise times) are **not** loaded into the forecast table, so observations and forecasts never share records. They are preserved in raw storage, and an `OBSERVED_FIELDS_NOT_LOADED` INFO note is recorded. Where to store them is an open decision.

## Natural keys and versions (VERSION policy; no unique constraints)

| Table | Natural key | Lineage (never makes a row "changed") |
|---|---|---|
| `imd_station` | network, imd_id, call_sign | availability fields |
| `imd_station_observation` | network, imd_id, call_sign, observation_date, observation_time | station context, `state_code`, coordinates, `imd_station_id`, `observed_at`, availability fields |
| `imd_city_forecast` | station_code, issue_date, forecast_date | `station_name`, availability fields |

- **Observations:** a revised measurement for the same station and time is a new version. A change only in station context is not.
- **Forecasts:** each issue date is a separate vintage. A re-fetch of the same issue with changed values becomes a new version of only the changed rows. Older vintages are never overwritten.
- **Station identifiers:** their uniqueness is unverified, so there is no unique constraint. An ID with several call signs, or vice versa, is reported (`STATION_ID_AMBIGUOUS`).
- **Availability:** IMD publishes no times, so `availability_basis = RETRIEVED_AT` and `source_available_at = retrieved_at`. `source_published_at` is NULL.

## Data-quality rules

| Situation | Handling |
|---|---|
| Response not a JSON list of objects (container undocumented) | run FAILED (`ImdResponseError`); raw file kept |
| HTTP 401/403 | run FAILED with "access not approved or IP not whitelisted"; never retried or worked around |
| 5xx / 429 / timeouts | retried by `HttpClient` with backoff |
| Empty list | `NO_DATA` INFO |
| Missing `ID`/`CALL_SIGN`/`DATE`/`TIME` (AWS) or `Station_Code`/`Date` (city) | record rejected (`PARSE_ERROR`) |
| DATE/TIME/Date not in the documented format | record rejected |
| Non-numeric value (anything except a number, numeric string, null or empty) | record rejected (AWS) or that lead day rejected (city); `PARSE_ERROR` |
| JSON null or empty string | stored NULL, `MISSING_VALUE` WARNING; never filled |
| Documented field absent | stored NULL, `MISSING_FIELD` WARNING |
| Every AWS measurement missing | row kept, `ALL_MEASUREMENTS_NULL` WARNING |
| RH outside 0–100, NEBULOSITY outside 0–8, WIND_DIRECTION outside 0–360 | stored as published, `OUT_OF_RANGE` WARNING (units unverified, so not rejected) |
| Forecast day fields all absent / all empty | that lead not loaded, `INCOMPLETE_FORECAST` WARNING |
| Unknown station in observations | observation loaded with `imd_station_id` NULL, `UNKNOWN_STATION` WARNING |
| STATE not an exact documented IMD name | `state_code` NULL, `UNMAPPED_STATE` WARNING |
| Same key twice in one response | identical copies loaded once (`DUPLICATE_IN_BATCH`); different copies all rejected (`CONFLICTING_DUPLICATE_IN_BATCH`) |
| Undocumented fields | preserved in raw storage, `UNDOCUMENTED_FIELDS` INFO |
| Byte-identical re-fetch | `DUPLICATE_DOCUMENT` (idempotent) |

**State mapping:** the 36 state names documented for `sid` map exactly to `pi.state` (`src/pipelines/imd/reference.py`). `DAMAN_AND_DIU` maps to DH (merged UT, "Dadra and Nagar Haveli and Daman and Diu").

## Running (only after migration approval and authorised IMD access)

```bash
python -m scripts.ingest_imd aws --dry-run
python -m scripts.ingest_imd aws --state-id 7          # one documented state (7 = DELHI)
python -m scripts.ingest_imd aws --station NDL         # one call sign
python -m scripts.ingest_imd city-forecast --dry-run
python -m scripts.ingest_imd city-forecast --station 42182
```

- **Settings:** `IMD_API_BASE_URL`; `IMD_API_KEY` and `IMD_API_KEY_HEADER` (sent only if both are set, because no auth mechanism is assumed); `IMD_REQUEST_PAUSE_SECONDS`; `IMD_AWS_TIME_ZONE` (`unverified` until confirmed).
- **Exit codes:** 0 success; 1 a run PARTIAL/FAILED; 2 invalid arguments or configuration (including "migration 0008_imd not applied", which is checked before any request).

## City observed fields (decision 2026-11-16)

`Today_Max_temp`, the temperature departures, `Past_24_hrs_Rainfall`, the humidity values and the astronomical times stay in immutable raw response storage only. No table or dataset is created for them in this phase.

## Unverified assumptions (confirm during authorised live testing)

1. The top-level response container (a JSON list is assumed for all three endpoints).
2. The `/aws_data_mapping` field structure (assumed to carry the `/aws_data` identity keys).
3. AWS units, and the time zone of `DATE`/`TIME`.
4. Uniqueness and stability of `ID`, `CALL_SIGN` and `Station_Code`.
5. Whether the AWS `STATE` uses exactly the documented `sid` spellings.
6. Whether numbers arrive as JSON numbers or strings, and how missing values are encoded (only null and empty string are treated as missing).
7. Whether city `Date` is the issue date, and the actual issue time.
8. The units of `Past_24_hrs_Rainfall` and the forecast text codes.
9. Whether IMD issues a key or token in addition to IP whitelisting, and any rate limits.
