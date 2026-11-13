# CEA ingestion pipeline

Central Electricity Authority reports, loaded into PostgreSQL schema `pi`. The source assessment and the approved decisions D1–D6 are in `docs/cea_source_assessment.md`.

> **Use restriction.** Every Daily Generation Report (DGR) footer says: "Data published in this report is for operational purpose only and not for commercial purpose." Confirm that the intended use is acceptable before running this regularly. No schedule is configured.

## Status

| Check | State |
|---|---|
| Alembic migration `0001_cea` (new `cea_power_supply_position`; outage category columns) | ✅ applied 2026-11-13 |
| Reference data (regions, states, plants, units, name mappings) | ✅ seeded 2026-11-13 from DGR sub-report 2 of 2026-11-11 |
| Unit tests (parsers, conversions, request rules) | ✅ 43 |
| Integration tests (real `pi` schema, mocked HTTP, rolled back) | ✅ 24 |
| Live dry run, ingestion, re-run and reprocess check | ✅ 2026-11-13, see the [record](#live-verification-record) |
| Scheduling | not configured |

## What is loaded

| Command | Report | Table | Natural key (VERSION policy) |
|---|---|---|---|
| `coal` | Daily Coal Stock Report (`dailyCoal1-*.xls`) | `cea_coal_stock_daily` | `plant_id, report_date` |
| `outages` | DGR sub-report 10, daily maintenance report for coal, lignite and nuclear units (`dgr10-*.xls`) | `cea_dgr_unit_outage` | `plant_id, unit_label, report_date` |
| `plant-daily` | DGR sub-report 2, station-wise generation (`dgr2-*.xls`) | `cea_dgr_plant_daily` | `plant_id, report_date` |
| `psp-energy` | monthly "Power Supply Position – Energy" (PDF) | `cea_power_supply_position` (`measure = 'ENERGY'`, MU) | `area_code, report_month, period_scope, measure` |
| `psp-peak` | monthly "Power Supply Position – Peak" (PDF) | `cea_power_supply_position` (`measure = 'PEAK'`, MW) | same |

Code: `src/pipelines/cea/` (`api`, `cells`, `geography`, `parse_coal`, `parse_dgr`, `parse_psp`, `pipeline`, `reference`, `runner`) and `scripts/ingest_cea.py`.

## Sources

| Report | URL | Published | History |
|---|---|---|---|
| Coal stock "AS ON" D | `{CEA_NPP_REPORTS_BASE_URL}/daily/fuel/{DD-MM-YYYY}/dailyCoal1-{YYYY-MM-DD}.xls` | D+1 ~15:30 IST | from 2024-06-15 |
| DGR sub-report N for D | `{CEA_NPP_REPORTS_BASE_URL}/daily/dgr/{DD-MM-YYYY}/dgr{N}-{YYYY-MM-DD}.xls` | D+1 ~17:00 IST | from 2018-04-01 |
| Monthly PSP listing | `{CEA_SITE_BASE_URL}/reports/dashboardView?headingId=107&subHeadingId=1003&subSubHeadingId=10010` (energy) or `10011` (peak) `&lang=en` | — | — |
| Monthly PSP file | `{CEA_SITE_BASE_URL}/api/dashboardReports/download-pdf/en/{uuid}` | ~2.5 months after the month, often revised | supported from 2020-04 |

The defaults are `https://npp.gov.in/public-reports/cea` (CEA's files mirrored on NPP) and `https://cea.nic.in`.

**Request behaviour:**
- Requests within a run are 0.5 s apart.
- **HTTP 404** means the report is not published. It is recorded as `MISSING_DAY` / `MISSING_MONTH`, not as a failure.
- **Retries:** timeouts and HTTP 408/425/429/5xx are retried by the shared HTTP client.
- **Publication time:** NPP's `Last-Modified` header is stored as `source_document.published_at`. The CEA PDFs send none.
- **Monthly report UUIDs change between page loads**, so they are not identifiers. Each run reads the listing first and stores it as an audit document holding no records.

## Field mapping

### `cea_coal_stock_daily` (stock in '000 tonnes, as published)

| Column | Source |
|---|---|
| `plant_id` | station name → `source_entity_mapping` (`CEA_COAL_STOCK_DAILY`) |
| `report_date` | `(AS ON DD-MM-YYYY)`, checked against the requested date |
| `actual_stock_000_tonnes`, `indigenous_stock_000_tonnes`, `imported_stock_000_tonnes` | Actual Stock: Total / Indigenous / Import |
| `daily_requirement_000_tonnes` | Daily Requirement @85 % PLF |
| `normative_stock_days` | Normative Stock Reqd. (Days) |
| `stock_vs_normative_pct` | % of Actual Stock vis-à-vis Normative (`39%` → 39.00) |
| `critical_stock_flag` | Critical `*` (stock < 25 % of normative) |
| `receipt_000_tonnes`, `consumption_000_tonnes` | Receipt / Consumption of the day |
| `remarks` | Reasons for critical coal stock / Remarks |
| `stock_days`, `actual_stock_tonnes` | **NULL**: not published (D3). Source-stated days are in `cea_dgr_plant_daily.coal_stock_days` |

**Not loaded** (kept in the raw file and the record's raw copy): capacity, PLF, normative stock, GENCO, mode of transport, category A–D.

**Parsing:**
- Merged header cells do not line up with data cells, and column positions changed between 2024 and 2026.
- A plant row is therefore read as the sequence of its non-empty cells, checked against a strict pattern: number, %, 6 numbers, %, optional `*`, 2 numbers, optional remarks.
- A row that does not match is a `PARSE_ERROR`, never a guess.

### `cea_dgr_unit_outage` (DGR sub-report 10)

| Column | Source |
|---|---|
| `plant_id`, `unit_id` | Power Station → mapping (`CEA_DGR_DAILY`); Unit No. → `generating_unit` (NULL if the unit is not in the master) |
| `unit_label`, `state_name` | Unit No. and State/System as published |
| `planned_mw`, `forced_major_mw`, `forced_minor_mw`, `other_mw` | the four maintenance columns (migration `0001_cea`) |
| `outage_capacity_mw` | sum of the four |
| `outage_start_at` | Date & Time of Maintenance (IST) |
| `expected_sync_at` | Expected/sync Date of Return (IST). A future date is expected; a past date within the report day is the actual sync. Stored as published |
| `outage_reason` | Reasons / Present Status |

**What the data means:**
- **Reserve shutdowns:** "RESERVE SHUT DOWN" and "RSD/LOW SCHEDULE" units are reported under Forced (Minor). They are economic shutdowns, not breakdowns.
- **Zero-MW rows:** a row with all four MW at 0 is either a unit that returned during the day or a very old entry.
- **Scope:** gas and hydro unit outages are not in sub-report 10 (D5).

**Wrapped cells.** A long reason or station name sometimes wraps onto a second row that repeats every other cell:
- "RESERVE SHUT DOWN /" + "STANDBY UNIT";
- "ADANI POWER LIMITED MUNDRA" + "TPP - I & II".

Consecutive rows identical in unit, state, all four MW, outage start and sync time are joined into one record. The joined name is then looked up ("ADANI POWER LIMITED MUNDRA TPP - I & II" is a station in the master). The report's own Total row counts such units twice. A row that differs in any of those cells is not joined, so genuine conflicts are still rejected.

### `cea_dgr_plant_daily` (DGR sub-report 2)

| Column | Source |
|---|---|
| `monitored_capacity_mw` | MONITORED CAP. (MW) |
| `todays_program_mu`, `todays_actual_mu` | TODAY'S PROGRAM / ACTUAL |
| `ytd_program_mu`, `ytd_actual_mu` | APRIL 1 TILL DATE: PROG. / ACTUAL |
| `coal_stock_days` | COAL STOCK IN DAYS (coal stations only) |
| `capacity_under_outage_mw` | CAP. UNDER OUTAGE (MW) |
| `remarks` | REMARKS |

Bhutan import stations (CHUKHA, KURICHU, MANGDECHHU, PUNATSANGCHHU-II, TALA) are skipped. Unit rows are used only for the unit master.

### `cea_power_supply_position` (monthly reports)

One row per area × month × period (`MONTH`, and `FYTD` = April to the month; April reports have `MONTH` only) × measure.

| Column | Energy report | Peak report |
|---|---|---|
| `requirement` | Energy Requirement (MU) | Peak Demand (MW) |
| `supplied` | Energy Supplied (MU) | Peak Met (MW) |
| `not_supplied` | Energy not Supplied (MU) | Demand not Met (MW) |
| `not_supplied_pct` | % | % |

**Areas:**

| `area_code` | `area_type` |
|---|---|
| ISO 3166-2:IN state code, e.g. `HR`, `CG`, `DH` | `STATE` |
| `DVC`, `JK_LA` ("UT of J&K and Ladakh"), `DD` / `DNH` (separate before the 2020 merger) | `SYSTEM` |
| `OTHERS_WR`, `OTHERS_NER` | `OTHER` |
| `NR`, `WR`, `SR`, `ER`, `NER` | `REGION` |
| `ALL_INDIA` | `COUNTRY` |

A row's `region_code` is the region whose total row follows it in the report.

**Other columns:**
- `report_status`: "Revised" / "Corrigendum" from the title.
- `footnote_flags`: markers such as `(#)` (stand-alone island systems) and `(**)` (previous month's data used).

**Report checks:**
- The title must read "Energy (or Peak) Power Supply Position Report" for the requested measure, and the month must match.
- Anything else is rejected. This rejects the pre-2020 tables and the many generation reports mislabelled as PSP in CEA's listing (2017 to early 2020).

## Reference data (D1)

`python -m scripts.ingest_cea reference [--date YYYY-MM-DD] [--dry-run]` seeds:

- **Regions:** 5 grid regions.
- **States:** 36 states/UTs with ISO 3166-2:IN codes (`src/pipelines/cea/geography.py`).
- **Plants:** one per Indian station in DGR sub-report 2 of the date.
  - `plant_code = 'CEA:<normalised name>'`.
  - Name, state, region and CEA type (`THERMAL`, `HYDRO`, `THER (GT)`, `THER (DG)`, `NUCLEAR`) are stored.
  - `fuel_type` and `installed_capacity_mw` stay NULL: the report gives a type and *monitored* capacity, not fuel or installed capacity.
- **Units:** one `generating_unit` per unit row (label and capacity).
- **Name mappings:** one `source_entity_mapping` row per station for `CEA_COAL_STOCK_DAILY` and `CEA_DGR_DAILY`, using the exact normalised name.

**Idempotency:** inserts use `ON CONFLICT DO NOTHING` / existence checks. Existing rows are never updated; rows that differ are reported. The source file is stored under `raw_data/cea/cea_reference/`.

**Coverage on 2026-11-11:** all 191 coal-report stations and all outage-report stations map exactly. Older reports use some names that are no longer current (e.g. the 2024-07 coal report has `METTUR TPS - II`). Such rows are rejected as `UNMAPPED_ENTITY`, never guessed. Seed again with an older `--date` before backfilling.

**Before a run:**
- Daily runs need plant mappings and monthly runs need regions and states; outside dry runs a missing reference is a configuration error (exit 2).
- A dry run seeds them inside its rolled-back transaction.

## Data quality

| Check | Result |
|---|---|
| Report not published (404) | `MISSING_DAY` / `MISSING_MONTH`: INFO for the last 2 days or 4 months, WARNING for older ones |
| HTML page instead of the file, unreadable workbook or PDF, wrong title, wrong date or month, missing header labels, two listing entries for one month | Run FAILED (`InvalidResponseError`); nothing of that document is loaded |
| Other HTTP errors after retries | Run FAILED (`CeaHttpError`) |
| Row that does not match the expected cells, bad number or timestamp, wrong count of numbers | `PARSE_ERROR`; the record is rejected, others load; run PARTIAL |
| Station / area name not in the reference | `UNMAPPED_ENTITY` (ERROR); rejected |
| Negative values | `OUT_OF_RANGE` (ERROR; WARNING for generation MU) |
| Coal Total ≠ Indigenous + Import (±0.15) or Σ plants ≠ Grand Total (±0.5) | `INCONSISTENT_TOTAL` WARNING |
| Outage MW > unit capacity, or start after the report day | `ABOVE_UNIT_CAPACITY` / `FUTURE_OUTAGE_START` WARNING |
| Σ unit outages ≠ report Total (±0.5 MW) | `INCONSISTENT_TOTAL` WARNING |
| PSP supplied > requirement, or requirement − supplied ≠ not supplied (±1) | WARNING |
| Repeated key in one document | identical → INFO, loaded once; conflicting → ERROR, not loaded |

Rejected records go to `pi.data_quality_issue` and a `<dataset>-rejected` raw file; nothing is silently dropped.

**Rounding:** values are quantized to the destination column's scale when parsed (e.g. daily requirement `10.283639999999998` → `10.284`), so an unchanged re-published file compares equal.

## Idempotency, revisions and reprocessing

- **Byte-identical files** are skipped by SHA-256 (`DUPLICATE_DOCUMENT`).
- **Changed records** (a revised monthly report, an updated daily file) are inserted as new versions tied to the new run; earlier rows are kept.
- **No database unique constraints** exist on the natural keys (the tables' constraints include `ingestion_run_id`). Uniqueness is enforced by the loader under an advisory lock.
- **`--reprocess`** re-parses files already ingested instead of skipping them, so a parser fix reaches stored files. Records already stored unchanged are skipped; only missing or changed records are inserted.
  - It uses an opt-in `reload_duplicate_documents` flag added to `BaseIngestionPipeline`. It is off by default, so no other pipeline's behaviour changes.

## Raw storage

```
RAW_DATA_DIR/cea/<dataset>/<coal|dgr10|dgr2|energy|peak|listing>/YYYY/MM/DD/<retrieved UTC>Z.<xls|pdf|html> (+ .meta.json)
```

Files are write-once. Document names follow these patterns:
- `cea_coal__2026-11-11.xls`
- `cea_dgr10__2026-11-11.xls`
- `cea_dgr2__2026-11-11.xls`
- `cea_psp_energy__2026-08__<uuid>.pdf`
- `cea_psp_energy__listing__2026-08__2026-08.html`

## Running

```bash
python -m scripts.ingest_cea reference --dry-run                       # once (and before backfills: --date)
python -m scripts.ingest_cea coal --date 2026-11-11 --dry-run
python -m scripts.ingest_cea outages --start-date 2026-11-05 --end-date 2026-11-11    # max 31 days
python -m scripts.ingest_cea plant-daily --date 2026-11-11
python -m scripts.ingest_cea psp-energy --start-month 2026-06 --end-month 2026-08     # max 12 months
python -m scripts.ingest_cea outages --start-date 2026-11-05 --end-date 2026-11-10 --reprocess
```

**Exit codes:** `0` SUCCESS, `1` PARTIAL or FAILED, `2` invalid arguments or missing reference.

**Allowed dates:**
- Daily reports: up to yesterday (IST).
- Monthly reports: up to the previous month.

## Live verification record

Run on 2026-11-13.

**Reference seeding:**
- Committed: 5 regions, 36 states, 537 plants, 684 units, 1,074 mappings; 5 Bhutan stations skipped.
- Re-run: everything unchanged.

**Dry runs** (2026-11-11 / 2026-08), all rolled back:

| Dataset | Read | Inserted | Rejected | Status |
|---|---|---|---|---|
| coal | 191 | 191 | 0 | SUCCESS |
| outages | 173 | 171 | 2 | PARTIAL |
| plant-daily | 537 | 537 | 0 | SUCCESS |
| psp-energy | 86 | 86 | 0 | SUCCESS |
| psp-peak | 88 | 88 | 0 | SUCCESS |

The 2 outage rejections were a reason wrapped onto a second row. The parser was fixed to join such rows, and the outage dry run was repeated: 172 records, 0 rejected.

**Live runs:**

| Run | Dataset | Period | Documents | Read | Inserted | Rejected | Status |
|---|---|---|---|---|---|---|---|
| 1091 | coal | 2026-11-11 | 1 | 191 | 191 | 0 | SUCCESS |
| 1092 | outages | 2026-11-11 | 1 | 172 | 172 | 0 | SUCCESS |
| 1093 | plant-daily | 2026-11-11 | 1 | 537 | 537 | 0 | SUCCESS |
| 1094 | psp-energy | 2026-08 | 2 | 86 | 86 | 0 | SUCCESS |
| 1095 | psp-peak | 2026-08 | 2 | 88 | 88 | 0 | SUCCESS |
| 1096 | coal | 2026-11-05..10 | 6 | 1,146 | 1,146 | 0 | SUCCESS |
| 1097 | outages | 2026-11-05..10 | 6 | 859 | 849 | 10 | PARTIAL (wrapped station names; see below) |
| 1098 | plant-daily | 2026-11-05..10 | 6 | 3,222 | 3,222 | 0 | SUCCESS |
| 1099 / 1100 | psp-energy / psp-peak | 2026-06..07 | 3 / 3 | 168 / 168 | 168 / 168 | 0 | SUCCESS |
| 1270 | outages `--reprocess` | 2026-11-05..10 | 6 | 854 | **5** (849 unchanged) | 0 | SUCCESS |
| 1271–1275 | all five, re-run | 11-05..11-11 / 06..08 | 31 duplicate | | **0** | 0 | SUCCESS |
| 1276–1280 | all five, `--reprocess --dry-run` | same | | 6,632 | **0** (all unchanged) | 0 | SUCCESS |

**Run 1097:**
- 10 rows were the two halves of 5 station names wrapped across rows. They were rejected as `UNMAPPED_ENTITY`, and the daily totals were flagged `INCONSISTENT_TOTAL` because the halves double-counted MW.
- The parser was fixed to join wrapped names. Reprocessing the stored files (run 1270) inserted exactly those 5 records, and the totals now match.
- In the monthly re-runs, the one processed document per run is the listing page, which differs on every load and holds no records.

**Checks in PostgreSQL after the runs:**

| Table | Rows | Detail |
|---|---|---|
| `cea_coal_stock_daily` | 1,337 | 191 plants per day |
| `cea_dgr_unit_outage` | 1,026 | Σ per day equals the report Total (e.g. 49,023.01 MW on 2026-11-11: planned 6,391, forced major 4,186.01, forced minor 38,446); every row has `unit_id` |
| `cea_dgr_plant_daily` | 3,759 | 537 per day; 190–191 with coal stock days |
| `cea_power_supply_position` | 510 | 42–44 areas × 2 periods × 3 months × 2 measures |

- 0 duplicate natural keys in all four tables.
- All 33 raw files match their stored SHA-256.
- Spot checks against the files:
  - PANIPAT TPS on 2026-11-11: stock 88.7 kt, 39 % of normative; DGR 7 days, 710 MW under outage.
  - August 2026: All India energy requirement 168,032 MU and supplied 167,829 MU; All India peak demand 258,276 MW; Haryana 8,604 MU.

## Known limitations

- **Use restriction:** see the note at the top.
- **Monthly report layout:** the parser supports the current layout only (2020-04 onwards). The 2013–2020 tables use another layout and terminology (requirement / availability / signed surplus-deficit). Many listing entries in 2017–2020 are actually generation reports.
- **Coal report history:** daily coal reports on NPP start in mid-2024.
- **Outage coverage:** sub-report 10 covers coal, lignite and nuclear only; gas and hydro outages are not loaded (D5).
- **Unconfirmed codes:** the meaning of the one-letter unit flags in sub-report 2 (`S`, `L`, `P`, `*`) is not confirmed, so they are not stored.
- **Station names change:** older reports need an older reference seed. Unmapped names are rejected, not guessed.
- **No publication time for monthly files:** CEA's PDF downloads send no `Last-Modified`.
- **Re-running doesn't apply parser fixes:** a parser fix reaches already-ingested files only through `--reprocess`.
- **No backfill run yet:** suggested chunks are in the assessment, §7.9.
