# CEA source assessment

Status: **implemented and live-verified on 2026-11-13.** Decisions D1–D6 were approved as recommended; see `docs/cea_pipeline.md`. This document is the assessment as written before implementation. Corrections found during implementation: the current monthly PSP layout goes back to 2020-04, not 2023; listing entries from 2017 to early 2020 are mislabelled generation reports; monthly file UUIDs change between page loads; DGR sub-report 10 wraps long reasons and station names onto a second row.

Legend: ✅ verified live · ⚠️ assumption / inference · ❓ pending confirmation

## 1. Summary

| # | Dataset | Best official source | Format | History (verified) | Frequency | Programmatic | Existing table | Schema change needed |
|---|---|---|---|---|---|---|---|---|
| 1 | State-wise energy requirement and availability | CEA monthly "Power Supply Position – Energy" | PDF only (text layer) | 2013-01 to 2026-08 | monthly, ~2.5-month lag, revised | ✅ static PDF links on an HTML listing; no JSON | **none fits** | **yes**: new table |
| 2 | State-wise peak demand and peak met | CEA monthly "Power Supply Position – Peak" | PDF only (text layer) | 2013-01 to 2026-08 | as above | ✅ as above | **none fits** | **yes**: same new table |
| 3 | Coal stock (tonnes and days) | CEA Daily Coal Stock Report, on NPP | **XLS** + PDF | 2024-06 to D-2 | daily, D+1 ~15:30 IST | ✅ predictable URL | `cea_coal_stock_daily` | no; plant master needed |
| 4 | Unit outages (capacity and reason) | CEA Daily Generation Report, sub-report 10, on NPP | **XLS** + PDF | 2018-04-01 to D-2 | daily, D+1 ~17:00 IST | ✅ predictable URL | `cea_dgr_unit_outage` | **recommended**: outage category columns; plant and unit masters needed |

**Main blockers, for the user to decide (§6):**
1. **No plant or unit master.** Every CEA table requires `plant_id NOT NULL`, but `pi.plant`, `pi.generating_unit`, `pi.state` and `pi.region` all have 0 rows.
2. **No table for the monthly power supply position.**
3. **Use restriction on the generation report.** Every DGR sub-report footer says "Data published in this report is for operational purpose only and not for commercial purpose."

## 2. Existing architecture

| Item | Finding |
|---|---|
| Source registration | `CEA` (source_id 1) is registered |
| Dataset registrations | `CEA_COAL_STOCK_DAILY` (175), `CEA_COAL_STOCK_MONTHLY` (176), `CEA_DGR_DAILY` (177) in `src/ingestion/catalog.py` |
| Catalogue details | Coal points at the **PDF**; the DGR outage sub-report is marked `PENDING_CONFIRMATION`. **No monthly power supply position dataset is registered.** |
| CEA code in this repo | none (no `src/pipelines/cea`, no script, no tests) |
| CEA tables (all 0 rows) | `cea_coal_stock_daily`, `cea_coal_stock_monthly`, `cea_dgr_plant_daily`, `cea_dgr_unit_outage` |
| Reference tables (all 0 rows) | `plant`, `generating_unit`, `state`, `region`, `entity`, `source_entity_mapping` |
| Alembic | `migrations/versions/` is empty: the schema predates Alembic, and no migration has ever been applied |
| Existing CEA code elsewhere | `IEX_Price_Forecasting/iex_data_feeds/iex_data_feeds/sources/cea_npp.py` (read-only, not modified) |

**What the forecasting repo's `cea_npp.py` does:**
- It fetches DGR sub-report 1 (all-India/regional totals only) and the daily coal stock XLS (Grand Total and category totals only).
- It parses with pandas into floats and writes parquet.
- It has no plant-level rows, no outage parsing, no PostgreSQL writes and no checksums.

**What is reusable from it:**
- The NPP `.xls` URL pattern.
- "404 or HTML body = not published".
- `Last-Modified` as the publication time.
- Header-label (not fixed-position) column location.

Its note that "cea.nic.in redirects report pages to a login wall" is **out of date**. The site has been rebuilt, and the report listings are now public (§3.1).

**Framework fit:** the existing `BaseIngestionPipeline` covers what CEA needs:
- raw-first storage with SHA-256;
- one transaction per document;
- `TableLoader` with natural key, `ConflictPolicy.VERSION`, `nullable_key_columns` and lineage injection for `ingestion_run_id`/`document_id`;
- `document_issues` and `SavepointDatabase` dry runs.

The CEA tables have `document_id` but no `retrieved_at`/`source_published_at` columns. The publication time therefore lives on `source_document`, which `_lineage` already handles.

## 3. Dataset assessment

### 3.1 Datasets 1 and 2: state-wise energy and peak power supply position (monthly)

| Aspect | Finding |
|---|---|
| Publisher | CEA (data furnished by state utilities) ✅ |
| Listing pages | Energy: `https://cea.nic.in/reports/dashboardView?headingId=107&subHeadingId=1003&subSubHeadingId=10010&lang=en`<br>Peak: same with `subSubHeadingId=10011` ✅ |
| File URL | `https://cea.nic.in/api/dashboardReports/download-pdf/en/{uuid}`. The UUID is only discoverable from the listing; there is no date-based URL ✅ |
| API | None. The listing is server-rendered HTML with one row per month (Report Name, Months `YYYY-MM`, PDF link). The Excel column is `--` for every row ✅ |
| Format | 1-page PDF with a text layer. `pdfplumber` extracts it cleanly; no OCR needed ✅ |
| History | 164 rows per report, 2013-01 to 2026-08 ✅ |
| Lag | August 2026 was the latest on 2026-11-13, a lag of about 2.5 months ✅ |
| Revisions | The title says "(Revised)", and 2023-04 also says "Corrigendum". The listing holds one row per month, so a revision presumably replaces the file ⚠️ |
| Areas | 36 states/UTs/systems, including `DVC`, `UT of J&K and Ladakh`, `Dadra & Nagar Haveli and Daman & Diu`, `Others`, and the stand-alone Lakshadweep and Andaman-Nicobar; 5 regions; All India ✅ |
| Units | Energy in **MU net**, peak in **MW net**, both rounded to whole units; Indian digit grouping (`2,57,783`) ✅ |

**Fields in the current format (2023 onwards).** Each area appears twice: for the month, and cumulative April to the month.

| Report | Fields |
|---|---|
| Energy | Energy Requirement (MU), Energy Supplied (MU), Energy not Supplied (MU), Energy not Supplied (%) ✅ |
| Peak | Peak Demand (MW), Peak Met (MW), Demand not Met (MW), Demand not Met (%) ✅ |

**Older format (e.g. 2013-04):**
- Columns are Requirement / Availability / Surplus/Deficit(−) MU, with a **signed** deficit; the current format uses positive "not supplied".
- The Hindi text uses a legacy font encoding and extracts as garbage; the English labels are fine ✅

The user's terms "availability" and "peak availability" are the old names for "Supplied" and "Peak Met".

**Anomalies and notes:**
- **The 2019-04 "Energy" entry is a different report** (region-wise monthly gross generation). The parser must check the title and reject mislabelled files ✅
- Footnotes printed on the report:
  - "Energy Supplied has been matched with Energy Requirement where Energy Requirement is less than Energy Supplied".
  - "(**) As data of July-2026 not received hence data of June-2026 has been considered" for Andaman-Nicobar.
  - Tripura excludes energy exported to Bangladesh.

  These notes should be preserved per row ✅
- **Not the same as Grid-India's daily report.** Grid-India's daily PSP (`GRID_INDIA_PSP_DAILY` → `grid_state_power_supply`) gives *daily* max demand met and energy met, operational and with no requirement or shortage. CEA's monthly figures are the official monthly requirement versus supply. They must not be mixed into the Grid-India table.

### 3.2 Dataset 3: daily coal stock

| Aspect | Finding |
|---|---|
| Publisher | CEA Fuel Management Division ✅ |
| URL | `https://npp.gov.in/public-reports/cea/daily/fuel/{DD-MM-YYYY}/dailyCoal1-{YYYY-MM-DD}.xls` (`.pdf` also exists) ✅ |
| Format | XLS (BIFF, one sheet `dailyCoalReport`, about 319 rows × 40 columns); needs `xlrd` ✅ |
| History | 2024-06-15 present; 2024-06-01 and earlier return 404 ✅. The CEA site's "Coal Reports" listing has only monthly Coal Statements (to 2024-02), with no older daily reports ✅ |
| Publication | Report "AS ON D" has `Last-Modified` D+1 10:01 UTC (15:31 IST). D+1 was not yet available at 09:40 IST on D+2 ✅ |
| Not published | HTTP 404 ✅ (the forecasting repo also saw HTML bodies served with 200) |
| Rows | About 191 plants, in categories A (domestic linkage), B (imported-coal design), C (not in operation), D (washery rejects). Also GENCO subtotals, category totals, `DCB`/`ICB`/`Grand Total` rows and a summary block ✅ |

**Columns (2026-11-11 report).** Located by header text, because positions shift: the 2024-07 file has 43 columns and no separate State/GENCO columns.

| Header | Unit / meaning |
|---|---|
| State, GENCO, Mode of Transport | text |
| Name of Thermal Power Station | text |
| Capacity (MW) | MW |
| Current Month PLF (Tentative) | % string, e.g. `52%` |
| Normative Stock Reqd. (Days) | days |
| Daily Requirement @85% PLF | '000 tonnes |
| Normative Stock Reqd. | '000 tonnes |
| Actual Stock: Indigenous / Import / Total | '000 tonnes |
| % of Actual Stock vis-à-vis Normative Stock | % string |
| Critical (*) | `*` when stock < 25 % of normative |
| Receipt of the day / Consumption of the day | '000 tonnes |
| Reasons for critical coal stock / Remarks | text |

**Stock days:**
- The current report has **no stock-in-days column** ✅
- It can be derived as Total ÷ Daily Requirement, but that is at an assumed **85 % PLF**, so it is a derivation, not a published fact ⚠️
- A published plant-level "COAL STOCK IN DAYS" appears in **DGR sub-report 2** ✅ (§3.3).

### 3.3 Dataset 4: generating unit outages

| Aspect | Finding |
|---|---|
| Publisher | CEA Operation Performance Monitoring Division (DGR) ✅ |
| URL | `https://npp.gov.in/public-reports/cea/daily/dgr/{DD-MM-YYYY}/dgr{N}-{YYYY-MM-DD}.xls` (`.pdf` also exists) ✅ |
| History | Sub-reports 2 and 10: 2018-04-01 present, 2018-01-01 and earlier 404 ✅ |
| Publication | `Last-Modified` D+1 11:29 UTC (~17:00 IST) for the 2026-11-11 report ✅ |
| Use restriction | Every sub-report: "Data published in this report is for operational purpose only and not for commercial purpose. Generation data/Outage data are compiled based on data submitted directly by the utilities." ✅ |

**Sub-reports found on 2026-11-11** (titles read from the files; `dgr7` is 404, and `dgr12`, `dgr14` and `dgr15` are empty sheets):

| No. | Content | Relevance |
|---|---|---|
| 1 | All-India/region generation overview | — |
| **2** | Region > state > sector > type > **station > unit** generation. Columns: monitored capacity, today's program/actual MU, YTD program/actual MU, **coal stock in days**, **capacity under outage MW**, outage date, expected/sync date, remarks; a one-letter unit flag (`S`, `L`, `P`, `*`) | → `cea_dgr_plant_daily`; plant and unit master source |
| 3 | All-India capacity availability by region (planned/forced/other MW) | aggregate |
| 4 | Region/utility capacity availability summary | aggregate |
| 5 | Region availability (thermal/nuclear/hydro) | aggregate |
| 6 | Hydro reservoir levels | — |
| 8, 9 | NTPC overview and stations | subset of 2 |
| **10** | **Daily maintenance report (coal, lignite and nuclear)**: State, Power Station, Unit No., **Planned / Forced (Major) / Forced (Minor) / Others MW**, date-time of maintenance, expected/sync date of return, **reason / present status** | **→ `cea_dgr_unit_outage`** |
| 11 | Same as 10, units ≥ 500 MW only | duplicate subset of 10 |
| 13 | Long-standing outages, mostly gas/liquid; no title row in the file | ❓ meaning to confirm |
| 16, 17 | Capacity utilisation; category/fuel-wise generation | aggregate |

**Sub-report 10 details (2026-11-11):**
- **Size:** 173 unit rows; total planned 6,391 MW, forced major 4,186 MW, forced minor 38,446 MW.
- **Timestamps:** strings, `DD/MM/YYYY HH24:MM` IST. Sub-report 2 uses Excel serial dates for the same values.
- **Rows with all four MW = 0** are of two kinds:
  1. units that came back during the day (a sync time is present);
  2. very old entries (e.g. DAE Rajasthan since 2004, Titagarh since 2016).
- **Return column:** "Expected/sync Date of Return" holds a *future expected* date or a *past actual sync*. The meaning depends on the value ⚠️
- **Reserve shutdown counted as outage:** "RESERVE SHUT DOWN" and "RSD/LOW SCHEDULE" appear under Forced (Minor). That is economic shutdown, not a technical failure, so analyses must not treat all forced MW as unavailability ⚠️
- **Format stability:** the 2018 and 2022 files have the same 10 logical columns at different positions. In 2018 the title was "(THERMAL AND NUCLEAR)" ✅
- **Scope:** gas and hydro unit outages are *not* in sub-report 10. They appear only as unit rows in sub-report 2 (outage MW and reason, no planned/forced split) and in sub-report 13 ✅

## 4. Database mapping

### 4.1 Datasets 1 and 2: no suitable table

| Candidate | Why not |
|---|---|
| `grid_state_power_supply` | Daily `report_date`; only `max_demand_met_mw` and `energy_met_mu`; no requirement, shortage, peak demand or cumulative period. It is the Grid-India table |
| `grid_daily_power_supply` | Regional, daily, energy met only |
| `npp_demand_generation` | Real-time snapshots |

A new table is required, so a migration is required (proposed in §5, not applied).

### 4.2 Dataset 3 → `pi.cea_coal_stock_daily`

| Column | Source | Notes |
|---|---|---|
| `plant_id` | station name → `source_entity_mapping` → `plant` | **blocked: plant master empty** |
| `report_date` | "AS ON DD-MM-YYYY" | checked against the URL date |
| `actual_stock_000_tonnes` | Actual Stock – Total | as published |
| `indigenous_stock_000_tonnes` | Actual Stock – Indigenous | |
| `imported_stock_000_tonnes` | Actual Stock – Import | |
| `daily_requirement_000_tonnes` | Daily Requirement @85 % PLF | |
| `normative_stock_days` | Normative Stock Reqd. (Days) | |
| `stock_vs_normative_pct` | % of Actual vs Normative | `39%` → 39.00 |
| `critical_stock_flag` | Critical `*` | true / false |
| `receipt_000_tonnes` | Receipt of the day | |
| `consumption_000_tonnes` | Consumption of the day | |
| `remarks` | Reasons / Remarks | |
| `actual_stock_tonnes` | — | **recommend NULL** (×1000 of the published value adds nothing) |
| `stock_days` | — | **decision D3**: NULL, or derived (stock ÷ requirement @85 % PLF) |
| `ingestion_run_id`, `document_id` | framework | |

**Not stored** (kept in the raw file):
- capacity and PLF %;
- normative stock in '000 t;
- GENCO, mode of transport and category A–D;
- subtotal rows (used for validation only).

Capacity and state belong on `plant`. No migration is needed for coal.

**Natural key:** `(plant_id, report_date)` with VERSION. This is compatible with the existing `UNIQUE (plant_id, report_date, ingestion_run_id)`.

### 4.3 Dataset 4 → `pi.cea_dgr_unit_outage` (sub-report 10)

| Column | Source | Notes |
|---|---|---|
| `plant_id` | Power Station → mapping | **blocked: plant master empty** |
| `unit_id` | Unit No. → `generating_unit` | **blocked: unit master empty**; nullable |
| `report_date` | title date | |
| `outage_capacity_mw` | Planned + Forced Major + Forced Minor + Others | sum; the split is lost without a migration |
| `outage_start_at` | Date & Time of Maintenance | IST → timestamptz |
| `expected_sync_at` | Expected/sync Date of Return | name kept; the value may be an actual sync (§3.3) |
| `outage_reason` | Reasons / Present Status | verbatim |
| `remarks` | — | |

**Missing from the current schema:**
- the **outage category split** (planned / forced major / forced minor / others);
- the published unit label, needed when `unit_id` cannot be mapped.

The table has no unique constraint; the natural key `(plant_id, unit_id, report_date)` is enforced by the pipeline, with `unit_id` null-safe.

**Sub-report 2 → `cea_dgr_plant_daily`** (optional, a natural companion):
- station rows → `monitored_capacity_mw`, `todays_program_mu`, `todays_actual_mu`, `ytd_program_mu`, `ytd_actual_mu`, **`coal_stock_days`**, `capacity_under_outage_mw`;
- this fills the source-stated "stock days" that the coal report lacks.

### 4.4 Reference data required by all three

| Table | Needed for | Proposed source |
|---|---|---|
| `region` | states, PSP | 5 regions (NR, WR, SR, ER, NER) |
| `state` | PSP, plant | states/UTs as named in PSP and DGR (≈36) |
| `plant` | coal, outages, DGR plant | DGR sub-report 2 station rows (name, state, region, type, monitored capacity) |
| `generating_unit` | outages | DGR sub-report 2 unit rows (unit no., capacity) |
| `source_entity_mapping` | name → id per dataset | coal and DGR spellings differ per report |

## 5. Proposed Alembic migration (not applied, for review)

This would be the first migration in `migrations/versions/`, and purely additive. No existing column is changed or dropped.

**1. New `pi.cea_power_supply_position`** (one row per area × month × period × measure):

| Column | Type | Notes |
|---|---|---|
| `id` | bigserial PK | |
| `report_month` | date NOT NULL | first day of the month |
| `period_scope` | varchar(10) NOT NULL | `MONTH` or `FYTD` (April to month); CHECK |
| `measure` | varchar(10) NOT NULL | `ENERGY` (MU) or `PEAK` (MW); CHECK |
| `area_code` | varchar(40) NOT NULL | normalised, e.g. `HARYANA`, `DVC`, `NR`, `ALL_INDIA` |
| `area_name` | text NOT NULL | as published |
| `area_type` | varchar(12) NOT NULL | `STATE` / `SYSTEM` / `REGION` / `COUNTRY` / `OTHER`; CHECK |
| `state_code` | varchar(10) NULL FK `state` | |
| `region_code` | varchar(10) NULL FK `region` | |
| `requirement` | numeric(18,3) | energy requirement MU / peak demand MW |
| `supplied` | numeric(18,3) | energy supplied MU / peak met MW (old "availability") |
| `not_supplied` | numeric(18,3) | positive shortfall (old reports: −1 × signed surplus/deficit, recorded) |
| `not_supplied_pct` | numeric(8,2) | |
| `unit` | varchar(4) NOT NULL | `MU` / `MW`; CHECK consistent with `measure` |
| `report_status` | text | e.g. `Revised`, `Corrigendum` |
| `footnote_flags` | text | e.g. `(#)`, `(**)` markers on the row |
| `ingestion_run_id` | bigint NOT NULL FK | |
| `document_id` | bigint FK | |

Index on `(area_code, report_month, period_scope, measure)`. Natural key with VERSION policy.

One table for both reports, because they have the same layout. A wide table would make two documents update halves of the same row, which breaks VERSION.

**2. `pi.cea_dgr_unit_outage`: add nullable columns**
- `planned_mw`, `forced_major_mw`, `forced_minor_mw`, `other_mw` (numeric(14,3));
- `unit_label` varchar(20) (published "Unit No.");
- `state_name` text (as published).

**3. No change** to the coal and DGR-plant tables.

The alternative to item 2 is no migration: store only the total in `outage_capacity_mw` and lose the planned/forced distinction. That distinction matters for price analysis, because forced outages are supply shocks and planned outages are known in advance.

## 6. Decisions needed before implementation

| # | Decision | Recommendation |
|---|---|---|
| **D1** | **Plant/unit master.** CEA rows cannot load without `plant` and `generating_unit` rows. Earlier guidance was "no dummy plant records". These would be *real* public CEA stations (≈190 coal, ≈500+ across DGR), not company plants | Approve a `reference` command (like IEX) that seeds region/state, then plant/unit from DGR sub-report 2: `plant_code = 'CEA:<normalised name>'`, `ON CONFLICT DO NOTHING`, dry-run first; plus `source_entity_mapping` rows per dataset. Unmapped names are rejected with `UNMAPPED_ENTITY`, never dropped |
| **D2** | Migration in §5 (new PSP table; outage split columns) | Approve both items; datasets 1–2 cannot load otherwise |
| **D3** | Coal `stock_days` | Leave NULL in `cea_coal_stock_daily`; take source-stated days from DGR sub-report 2 into `cea_dgr_plant_daily.coal_stock_days` |
| **D4** | DGR "not for commercial purpose" note (and the IEX precedent) | Confirm intended use is acceptable before regular ingestion |
| **D5** | Outage scope: sub-report 10 only (coal, lignite, nuclear, with category split), or also gas/hydro from sub-report 2 | Start with sub-report 10; add sub-report 2 later as the plant-daily dataset |
| **D6** | Parsing dependencies: `xlrd==2.0.1` (`.xls`) and `pdfplumber==0.11.4` (PDF), both tested only in a scratch venv | Add to `requirements.txt` |

## 7. Implementation plan (after approval)

Same structure as the NPP and IEX pipelines: `src/pipelines/cea/` with `api.py`, `parser_*.py`, `pipeline.py`, `runner.py`, `reference.py`, `errors.py`, plus `scripts/ingest_cea.py`.

### 7.1 Fetching

| Dataset | Requests per run |
|---|---|
| Coal | 1 GET per report date (XLS) |
| Outages | 1 GET per date (`dgr10` XLS); `dgr2` too if D5 is extended |
| PSP | 1 GET of each listing page, then 1 GET per selected month's PDF |

- **404 means not published.** It is reported as `MISSING_DAY` (INFO for D+1/today, WARNING for older dates), not a failure.
- **HTML served with 200** → `InvalidResponseError`.
- **Retries:** timeouts and 5xx go through the shared retry HTTP client.
- **Request pacing:** 0.5 s between requests; at most 31 days, or 12 months for PSP, per command.
- **Publication time:** `Last-Modified` → `RawDocument.published_at` (stored on `source_document`).

### 7.2 Raw storage

- **Layout:** `RAW_DATA_DIR/cea/<dataset>/<partition>/YYYY/MM/DD/<retrieved>Z.<xls|pdf>` (+ `.meta.json`), write-once.
- **Document names:**
  - `cea_dgr10__2026-11-11.xls`
  - `cea_coal_daily__2026-11-11.xls`
  - `cea_psp_energy__2026-08__<uuid>.pdf`
- The PSP listing HTML is also stored as a document, so the month → UUID mapping is auditable.

### 7.3 Parsing

- **Column location:** by header labels, never fixed positions, which shift between years. A missing expected header fails the document with `InvalidResponseError`.
- **Numbers:** `Decimal` throughout. Indian grouping (`2,57,783`) and `%` strings are normalised.
- **Timestamps:** IST strings → timestamptz. Sub-report 2's Excel serial dates are converted with the workbook datemode.
- **Row classification:** region/state headers, plants, units, subtotals and totals are classified explicitly. Subtotals are kept for validation, not loaded.
- **PSP specifics:**
  - The title must be "Energy/Peak Power Supply Position", otherwise `InvalidResponseError` (this catches the mislabelled 2019-04 file).
  - The month in the header must equal the listing month.
  - Area names wrap onto two lines and must be rejoined.
  - Old-format signed deficit → positive `not_supplied`.

### 7.4 Validation

| Check | Severity |
|---|---|
| Non-numeric or negative values | ERROR, record rejected |
| Outage MW > unit capacity (once units are seeded) | WARNING |
| Coal: Total ≠ Indigenous + Import (±0.1) | WARNING |
| Coal: Σ plants ≠ Grand Total (±0.5) | WARNING (document level) |
| PSP: supplied > requirement | WARNING (CEA caps supplied at requirement) |
| PSP: requirement − supplied ≠ not_supplied (±1, rounding) | WARNING |
| PSP: Σ states in region ≠ region (regional figures include ISTS entities; footnote ##) | INFO |
| Unmapped plant/state name | ERROR `UNMAPPED_ENTITY`; written to `data_quality_issue` and the rejects file |
| Duplicate key within one document | as IEX: identical → INFO, conflicting → ERROR |

### 7.5 Entity mapping

- **Lookup:** names are matched through `source_entity_mapping` (dataset_id, source_entity_name) → `plant_id` / `unit_id` / `state_code`.
- **Seeding:** the `reference` command seeds exact-name mappings from the same report used to create the plant.
- **Cross-report names:** coal vs DGR spellings are matched only by exact normalised name. Anything else goes to a review CSV for manual approval, with no fuzzy auto-matching.

### 7.6 Duplicate handling

- **Document level:** SHA-256 dedupe (`DUPLICATE_DOCUMENT`).
- **Record level:** natural keys with VERSION:

  | Dataset | Natural key |
  |---|---|
  | Coal | `(plant_id, report_date)` |
  | Outage | `(plant_id, unit_id, report_date)`, `unit_id` null-safe |
  | PSP | `(area_code, report_month, period_scope, measure)` |

- **Revisions:** a revised PSP or a re-uploaded DGR inserts a new version; earlier rows are kept.

### 7.7 Ingestion audit

Unchanged framework behaviour:
- one `ingestion_run` per command, with parameters;
- one `source_document` per file;
- `data_quality_issue` rows plus a rejects file;
- SUCCESS / PARTIAL / FAILED.

### 7.8 Testing

- Synthetic XLS/PDF fixtures built in tests: `xlwt` is not needed, because the parser can be fed the parsed cell grid. PDF tests use stored text extracted from structure-only samples.
- No real CEA data is committed, following the IEX precedent.
- Integration tests run on the real `pi` schema inside rolled-back transactions.

### 7.9 Verification and backfill

1. **First live test (after approval):** a dry run for one date; then live ingestion of one historical date and a 3–7-day range for coal and outages, plus 2 months of PSP; verify in PostgreSQL; then re-run to confirm no duplicates.
2. **Backfill (separate approval, not started):**

   | Dataset | Scope | Approach |
   |---|---|---|
   | Coal | 2024-06 onwards (~520 files) | 31-day chunks |
   | Outages | 2018-04 onwards (~3,150 files) | 31-day chunks, newest first, so recent data is usable early |
   | PSP | 2013-01 onwards (2 × 164 PDFs) | old-format parser variant needed before 2023 |

   Each chunk is idempotent and resumable, at about 1 request per second.
3. **No scheduling** is configured.

### 7.10 Catalogue updates (implementation phase)

- Add `CEA_PSP_ENERGY_MONTHLY` and `CEA_PSP_PEAK_MONTHLY` → `cea_power_supply_position`.
- Point `CEA_COAL_STOCK_DAILY` at the XLS.
- Replace the DGR `PENDING_CONFIRMATION` with sub-report 10 (outages) and 2 (plant daily).
- Regenerate `docs/dataset_table_mapping.md`.

## 8. Open questions (❓)

- The meaning of the one-letter unit flags in sub-report 2 (`S`, `L`, `P`, `*`). Short/long outage and planned are guesses ⚠️
- The exact content and title of sub-report 13.
- Whether CEA replaces the PSP PDF under the same UUID when it revises, or issues a new UUID. Both are handled by checksum and versioning.
- The exact first date of daily coal on NPP: between 2024-06-02 and 2024-06-15.
- NPP / CEA website terms of use were not reviewed beyond the notes printed in the reports.
