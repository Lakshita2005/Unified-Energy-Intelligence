# IEX source assessment and field mapping

Status: **implemented; see `docs/iex_pipeline.md`** (this file is the original assessment). Prepared on 2026-11-12 from the catalogue, the live `pi` schema, the IEX website's own JavaScript, and about 90 read-only test calls to the IEX API. Delivery dates tested: 2015-01-01 to 2026-11-14.

Every statement is labelled:
- **[V]** verified by inspection or a live call;
- **[D]** derived deterministically from verified data;
- **[A]** an assumption that needs confirmation;
- **[P]** pending, not resolved.

---

## 0. Blocker: the terms of use

**[V]** The IEX Terms of Use (https://www.iexindia.com/terms-of-uses) say:

> "You may use the Content only for personal, non-commercial purpose. Except as expressly authorized by IEX you must not download, copy, extract or distribute the Content in any manner."

**[V]** Every market-data page also says:

> "The above information is a publication of IEX. All rights reserved. Only correct quotation with attribution is permitted."

**[V]** The site runs Radware Bot Manager (`/…/stormcaster.js`, `__uzm*` cookies). Plain requests were answered during this assessment, but automated access could be blocked at any time.

**[P]** I found no public IEX API product, data licence or subscription page.

**Consequence:** routinely downloading IEX data into a company database would conflict with these terms unless IEX has authorised it. I recommend getting written authorisation or a data licence from IEX, or an authorised file source, **before** any live ingestion. The design below would work with either the website API (if authorised) or a licensed feed, if the feed has the same fields.

---

## 1. Current registrations

| Dataset | ID | Registered endpoint text | Granularity / frequency | Destination |
|---|---|---|---|---|
| `IEX_DAM` | 200 | IEX DAM market snapshot page | `15min` / `daily` | `iex_market_price` |
| `IEX_GDAM` | 201 | IEX G-DAM market snapshot page | `15min` / `daily` | `iex_market_price` |
| `IEX_RTM` | 202 | IEX RTM market snapshot page | `15min` / `daily` | `iex_market_price` |

Source `IEX`: "Indian Energy Exchange", https://www.iexindia.com/.

**Catalogue notes that need correcting** (only after approval):
- **[V]** The GDAM note says 15-minute data starts 2022-04-01. The API returned 96 blocks for 2021-10-31.
- **[V]** The RTM note says data starts from delivery day 2022-04-01. The API returned 96 blocks for 2020-06-01.
- **[V]** The registered URLs are web pages, not the API endpoints.
- **[V]** RTM's expected frequency of "daily" is incomplete: RTM data for the current day arrives during the day (see §4.5).

---

## 2. Destination schema

### `pi.iex_market_price` (0 rows) [V]

| Column | Type | Constraint |
|---|---|---|
| `id` | bigint | primary key |
| `market_code` | varchar(20) | required; references `market_master(market_code)` |
| `area_code` | varchar(40) | required; references `market_area(area_code)` |
| `ingestion_run_id` | bigint | required; references `ingestion_run` |
| `delivery_date` | date | required |
| `time_block` | smallint | required; must be from 1 to 96 |
| `price_inr_per_mwh` | numeric(16,4) | required |
| `retrieved_at` | timestamptz | required; default `now()` |
| `source_published_at` | timestamptz | nullable |

- **Indexes:** the primary key, and a **non-unique** index `ix_iex_market_block` on (`market_code`, `area_code`, `delivery_date`, `time_block`). There is no unique constraint.
- **No `document_id` column.** Row-to-document lineage goes through `ingestion_run_id`. That is exact as long as each run loads one document.

### Reference tables, all empty [V]

| Table | Columns | Needed for |
|---|---|---|
| `pi.market_master` | `market_code` (PK), `market_name`, `market_operator` (all required) | every row, through the foreign key |
| `pi.market_area` | `area_code` (PK), `area_name` (required), `country_code` (nullable, references `country`) | every row, through the foreign key |
| `pi.country` | `country_code` varchar(3), `country_name` | optional |

**No IEX row can be inserted until `market_master` and `market_area` are populated.**

---

## 3. Endpoints

### Found [V]

The website calls an internal JSON API at `https://www.iexindia.com/api/v1/` (set in the site's JavaScript as `baseURL:"https://www.iexindia.com/api/"` with a `v1/` prefix). It is not documented publicly.

| Purpose | Request | Markets |
|---|---|---|
| Market snapshot | `GET /api/v1/{dam\|gdam\|rtm}/market-snapshot?interval=ONE_FOURTH_HOUR&fromDate=DD-MM-YYYY&toDate=DD-MM-YYYY` | DAM, GDAM, RTM |
| Area prices | `GET /api/v1/{dam\|gdam}/area-price?interval=ONE_FOURTH_HOUR&fromDate=…&toDate=…&bidArea=<area>` | DAM, GDAM (RTM: see below) |

- **Allowed `interval` values** (from the API's own validation error): `ONE_FOURTH_HOUR, ONE_HOUR, DAILY, WEEKLY, MONTHLY, YEARLY`.
- **Bid areas** that returned data: `A1, A2, E1, E2, N1, N2, N3, S1, S2, S3, W1, W2, W3, MKT`, one area per request. `ALL` returns 400 "INVALID BID AREA".
- **Response envelope:** `{"statusCode":200,"message":"…fetched successfully","data":[…]}`.
- **Errors:** a structured 400 for validation errors, and a 404 for unknown paths.
- **Authentication:** none. No API key, token or cookie was needed.
- **Range size:** a single request for 92 days (2026-08-01 to 2026-10-31, 8,832 rows) succeeded, so no range cap was observed.
- **Empty responses:** a future date with no data returns **HTTP 200 with `data: []`**, which can't be told apart from a day with no data.

### Not used or not found

- `v1/market-data/buy-sell-curve/zip` exists (bid curves). Out of scope.
- High-Price DAM (`high-price-day-ahead-market`) appears in the site. Out of scope.
- **[P]** RTM area prices: `rtm/area-price` returned **0 rows for every bid area** on 2026-11-10, even though 7 RTM blocks were congested that day. RTM area prices may not be available through this API.
- **[P]** Final versus provisional figures: the DAM page links to "View Unconstrained DAM MCP & MCV" and to a third-party site (`iexrtmprice.com/view-dam-provisional-mcv-and-mcp-data/`). Whether IEX revises published figures, and which values are provisional, is not documented.

---

## 4. Response formats

### 4.1 DAM, 15-minute [V]

Sample, 2026-11-10:

```json
{"date":"10-11-2026","period":"00:00 - 00:15","purchase_bid":"10288.80","sell_bid":"15248.50",
 "mcv":"4413.40","final_scheduled_volume":"4413.40","mcp":"3114.65","congestion":"NO"}
```

- 96 rows per day.
- Numbers are **strings** with 2 decimals.
- `period` uses the format `HH:MM - HH:MM` (with spaces); the last block is `23:45 - 24:00`.

### 4.2 RTM, 15-minute [V]

- Same fields as DAM.
- `period` has **no spaces**: `00:00-00:15`.
- Numbers are strings.

### 4.3 GDAM, 15-minute [V]

- Numbers are **JSON numbers** (int or float), not strings, so they must be parsed as `Decimal` to avoid float errors.
- **There is no `mcp` field.** The price is `unconstrained_m_c_p`.
- **The field set changed on 2026-07-08:**

| Delivery date | Volume breakdown fields |
|---|---|
| up to 2026-07-07 (checked 2021-11-01 and 2026-07-07) | `solar_*`, `non_solar_*`, `hydro_*` for sell bid, unconstrained MCV and cleared volume |
| from 2026-07-08 (checked 2026-07-08 and 2026-11-10) | `hydro_*`, `wind_*`, `other_re_*`, `dre_*` |

The site's own configuration confirms this: `gDamSummaryHeaders` versus `gDamSummaryHeadersAfter8July2026`.

### 4.4 Area prices [V]

```json
{"date":"10-11-2026","timeperiod":"00:00","mkt":3114.65}
```

- The value key is the lower-cased bid area (`n1`, `s3`, `mkt` …).
- `timeperiod` is the **block start** time.
- Values are JSON numbers.

### 4.5 Timing and history [V]

| Market | Earliest 15-minute day found | Next day | Today |
|---|---|---|---|
| DAM | 2015-01-01 had 96 blocks; 2012-01-01 had 0 (**[P]** exact start) | 2026-11-13 available by about 16:15 IST on 2026-11-12 | complete |
| GDAM | 2021-10-31 (**[P]** exact start) | 2026-11-13 available | complete |
| RTM | 2020-06-01 | empty | **partial**: 70 of 96 blocks (up to `17:15-17:30`) at about 16:15 IST |

- RTM data for the current day grows during the day, so a re-fetch must add new blocks without duplicating earlier ones.
- **[V]** 31 consecutive DAM days returned exactly 96 blocks each.

### 4.6 Units [V]

From the site's own column definitions for the 15-minute view:

| Field | Unit |
|---|---|
| Purchase Bid, Sell Bid, MCV, Final Scheduled Volume / FSV, and all GDAM breakdowns | **MW** |
| MCP and unconstrained MCP | **Rs/MWh** |

In the daily, weekly and monthly views, volumes are labelled **MWh**, so units depend on the interval. Only 15-minute data is in scope.

### 4.7 Prices, areas and congestion [V]

- **DAM:** snapshot `mcp` equals area-price `MKT` in all 96 blocks on 2026-11-10.
- **GDAM:** snapshot `unconstrained_m_c_p` equals area-price `MKT` in all 96 blocks.
- **Under congestion, area prices differ from MCP.** DAM 2026-10-01, 10:30–10:45: MCP 2,001.97, S3 2,001.96, N2 1,918.78 Rs/MWh. At 11:00–11:15: MCP 2,001.29, S3 2,110.38, N2 1,950.66.
- **Under congestion, MCV differs from FSV** (14,931.20 vs 14,849.70 MW in that block).
- October 2026 had 285 congested DAM blocks.

So the snapshot price is the system-wide **unconstrained** price. The price a buyer or seller in a region actually pays is the **area** price.

---

## 5. Field mapping

### 5.1 Price → `pi.iex_market_price`

| Source | Meaning | Unit | Column | Status |
|---|---|---|---|---|
| (market endpoint) | DAM / GDAM / RTM | — | `market_code` = `DAM`, `GDAM`, `RTM` | **[A]** codes need approving and seeding into `market_master` |
| (area) | `MKT` = system-wide unconstrained price; `A1`…`W3` = bid-area prices | — | `area_code` = `MKT`, `A1` … `W3` | **[A]** codes need approving and seeding into `market_area` |
| `date` (`DD-MM-YYYY`) | Delivery day (IST) | — | `delivery_date` | **[V]** format; **[A]** IST (an Indian market; the API states no timezone) |
| `period` / `timeperiod` | 15-minute delivery interval | — | `time_block` = (start minutes ÷ 15) + 1, so `00:00`→1 … `23:45`→96 | **[D]** from the period strings; **[A]** that block 1 = 00:00–00:15 matches how IEX numbers blocks (consistent with the 1–96 check and the ONE_HOUR `hour:"1"` for 00:00–01:00) |
| DAM/RTM `mcp`; GDAM `unconstrained_m_c_p` | Unconstrained market clearing price | Rs/MWh | `price_inr_per_mwh` with `area_code='MKT'` | **[V]** |
| area-price `<area>` | Area clearing price | Rs/MWh | `price_inr_per_mwh` with `area_code=<area>` | **[V]** DAM/GDAM; **[P]** RTM (no data) |
| — | Retrieval time | — | `retrieved_at` | set by the pipeline |
| — | Publication time | — | `source_published_at` = NULL | **[V]** the API gives no publication time |

### 5.2 Fields with no destination column

| Market | Fields |
|---|---|
| DAM, RTM | `purchase_bid`, `sell_bid`, `mcv`, `final_scheduled_volume` (MW), `congestion` (YES/NO) |
| GDAM | `purchase_bid`, `total_sell_bid`, `total_unconstrained_m_c_v`, `total_cleared_volume`, the source breakdowns (solar/non-solar/hydro before 2026-07-08; hydro/wind/other-RE/DRE after), `congestion` |

These would be kept in the raw files but **cannot be stored in the database without a schema change** (see §7).

---

## 6. Ambiguities and open questions

1. **Licence and terms (§0).** This is the blocker.
2. **Which price is "the" price.** The table has one price per market, area and block. Storing MKT (unconstrained) alone would misrepresent regional prices in congested blocks. I recommend storing MKT plus all 13 areas.
3. **RTM area prices** are unavailable through this API. RTM would then have MKT only, which is wrong for regional prices in congested blocks.
4. **GDAM price name:** `unconstrained_m_c_p` (15-minute) versus `mcp` (daily view). They are treated as the same concept because the 15-minute value equals area MKT.
5. **Revisions:** IEX doesn't say whether published prices or volumes are revised. The "provisional" link suggests some figures may be provisional. VERSION handling would keep any change, but nothing has been observed over time yet.
6. **Block numbering:** block 1 = 00:00–00:15 is derived, not stated by the API.
7. **Timezone:** IST is assumed; the API states none.
8. **Exact history starts** for DAM and GDAM.
9. **Empty `data: []`** is ambiguous (future date or no data), as with NPP.
10. **Number formats:** strings (DAM, RTM) versus JSON numbers (GDAM, area prices). Both must be parsed exactly.

---

## 7. Schema limitations and possible migrations (none applied)

| Limitation | Effect | Options |
|---|---|---|
| Price-only table | Volumes, MCV/FSV and congestion can't be stored | **(a)** keep prices only and leave the rest in raw files; **(b)** an Alembic migration adding a new table, e.g. `pi.iex_market_snapshot` (market, delivery_date, time_block, purchase_bid_mw, sell_bid_mw, mcv_mw, fsv_mw, congestion, plus GDAM breakdowns as nullable columns or a key/value child table, `ingestion_run_id`, `document_id`) |
| No `document_id` | Lineage only through the run | Acceptable with one document per run; optionally add a nullable `document_id` column in the same migration |
| No unique constraint | App-level dedupe only (as for the other sources) | Review database-level uniqueness separately |
| Empty reference tables | Foreign keys block every insert | Seed `market_master` (3 rows) and `market_area` (14 rows). This is reference data, not a schema change, but it needs your approval of the codes and names |

---

## 8. Loader compatibility

**[V]** The existing `TableLoader` supports IEX as it is:
- **Natural key:** (`market_code`, `area_code`, `delivery_date`, `time_block`), all non-null, so the null-safe option isn't needed. It matches `ix_iex_market_block`, so lookups use the index.
- **Versioning:** a changed price inserts a new row tied to the new run, and earlier rows are kept.
- **Comparison:** `retrieved_at` and `source_published_at` are lineage columns and never count as a change.
- **Decimals:** `numeric(16,4)` holds the 2-decimal prices without loss.
- **Base pipeline:** it adds `ingestion_run_id` and `retrieved_at`, and leaves out `document_id` because the table has no such column.
- **Re-fetching today's RTM:** the blocks already stored are unchanged; new blocks are inserted.

No loader or framework change is needed.

---

## 9. Proposed design (once access is authorised)

Same architecture and standards as Open-Meteo and NPP.

- **Package:** `src/pipelines/iex/` with `api`, `parser`, `pipeline`, `runner` and `errors`, plus a CLI `scripts/ingest_iex.py {dam|gdam|rtm} --date | --start-date --end-date [--dry-run]`, capped at 31 days.
- **Runs:** one run per market per delivery day.
  - **Documents:** the snapshot plus 13 area-price responses for DAM and GDAM; the snapshot only for RTM, until RTM area prices are available.
  - **Raw storage:** every response is saved raw with SHA-256 under `raw_data/iex/<dataset>/<delivery date>/…`.
- **Parser rejects the whole response if:**
  - the envelope is wrong or `statusCode` isn't 200;
  - `data` isn't a list;
  - fields are missing or unexpected (two accepted GDAM field sets, switching on 2026-07-08);
  - periods are malformed or repeated;
  - there are more than 96 blocks.
- **Empty `[]`** fails the run. Partial RTM days are accepted and marked in the run parameters.
- **Item-level checks:**
  - prices within IEX's price-cap bounds (**[P]** the cap must be confirmed and configurable);
  - volumes ≥ 0 (if stored);
  - snapshot MCP must equal area MKT, otherwise logged as a data-quality warning.
- **Reference rows:** `market_master` and `market_area` seeded through a separate approved, idempotent step. They are never created implicitly by the pipeline.
- **Tests:** unit and integration tests with real response excerpts, as for NPP.
- **Live verification:** only after authorisation.

---

## 10. Decisions needed

1. **Access:** do you have, or will you get, IEX authorisation or a data licence? Without it I recommend not implementing live website ingestion.
2. **Price scope:** MKT only, or MKT plus the 13 bid areas (recommended)?
3. **Volumes and congestion:** keep them in raw files only, or propose an Alembic migration for a snapshot table?
4. **Reference codes:** approve `market_master` `DAM`, `GDAM`, `RTM` (operator "Indian Energy Exchange") and `market_area` `MKT`, `A1`, `A2`, `E1`, `E2`, `N1`, `N2`, `N3`, `S1`, `S2`, `S3`, `W1`, `W2`, `W3` (country `IND` or NULL).
5. **RTM:** accept MKT-only prices until an area-price source is confirmed?
6. **Catalogue corrections in §1:** apply now, or together with the implementation?
