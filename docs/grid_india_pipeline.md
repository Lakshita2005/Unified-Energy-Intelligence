# Grid-India ingestion pipeline (Daily PSP workbook)

NLDC's Daily Power Supply Position (PSP) workbook, loaded into PostgreSQL schema `pi`. The source assessment and the decisions G1–G5 are in `docs/grid_india_source_assessment.md`.

> **Terms of use (G5)** of grid-india.in are not yet verified. Technical access to the API does not establish permission for commercial use. No schedule is configured.

## Status

| Item | State |
|---|---|
| Alembic `0002_grid_india` (columns, entity representation, availability timestamps) | ✅ applied 2026-11-13 after a rolled-back dry run (42/42 checks) |
| Section A → `grid_daily_power_supply` | ✅ milestone 1, live-verified |
| Sections C + J → `grid_state_power_supply` | ✅ milestone 1, live-verified |
| Section G → `grid_sourcewise_generation` | ✅ milestone 2, live-verified 2026-11-13 |
| Section F → `grid_generation_outage` | ✅ milestone 3, live-verified 2026-11-13 |
| Sections E, `TimeSeries`, `IR-Line`, `CrossBorder` | not yet implemented (next milestones, in that order) |
| Pre-2023 PDF reports | deferred (G4) |
| Tests | 48 unit + 24 integration (synthetic workbooks, mocked HTTP, real schema in rolled-back transactions) |
| Scheduling | not configured |

## How one workbook feeds several tables (G2)

```
listing (POST, neighbouring months) ──► raw JSON (write-once)
        │ select every .xls version for the requested days, oldest upload first
        ▼
download each workbook ONCE ──► raw .xls (write-once, SHA-256, listing entry + availability in .meta.json)
        │ the same in-memory documents are handed to each section
        ├──► SupplyPipeline  (dataset GRID_INDIA_PSP_DAILY,  own ingestion_run) ─► grid_daily_power_supply
        ├──► StatesPipeline  (dataset GRID_INDIA_PSP_STATES, own ingestion_run) ─► grid_state_power_supply
        ├──► FuelPipeline    (dataset GRID_INDIA_GENERATION, own ingestion_run) ─► grid_sourcewise_generation
        └──► OutagePipeline  (dataset GRID_INDIA_PSP_OUTAGE,  own ingestion_run) ─► grid_generation_outage
```

- **Section pipelines:** each one registers the workbook as a `source_document` of its own dataset, with the same checksum and the same raw file URI. Documents are de-duplicated per dataset, so each section loads independently. No section writes a second raw copy.
- **One run per section:** each section has its own `ingestion_run` with SUCCESS / PARTIAL / FAILED status. A failure in one section (e.g. section C missing from a workbook) fails only that section's run. The command exits 0 only if every section succeeded.
- **Fetch failures:** if the listing or a download fails, every requested section's run is recorded as FAILED with the error. Files downloaded before the failure stay in raw storage.
- **Re-runs:** every command fetches again and keeps the new copy, as in the other pipelines. Identical content is then skipped by checksum.

## Source

| Item | Value |
|---|---|
| Listing | `POST {GRID_INDIA_API_BASE_URL}/file` `{"_source":"GRDW","_type":"DAILY_PSP_REPORT","_fileDate":"<FY e.g. 2026-27>","_month":"MM"}` |
| File | `GET {GRID_INDIA_CDN_BASE_URL}/<FilePath>` (e.g. `files/grdw/2026/11/10.11.26_NLDC_PSP_401.xls`) |
| Report day | operating day D = the date in `Title_` = "Date of Reporting" − 1 (checked; a mismatch fails the document) |
| Published | D+1, usually 10:00–12:30 IST; a revision is an extra file the same day (e.g. `..._269_611.xls`) |
| History | `.xls` from 2023-01-01 (earlier reports are PDF only and out of scope) |

**Listing behaviour:**
- **Listing month:** the listing month is not strictly the report month (the August listing contains the 31 July report). The previous through the next month are queried, and files are selected by the date in `Title_`.
- **File type:** `MimeType` is inconsistent, so files are selected by extension.
- **Retries:** the listing POST and the downloads use the shared HTTP client's retries (POST explicitly opted in; it is a read-only query).
- **HTML pages:** an HTML page instead of a workbook fails the fetch.

## Availability timestamps (G3)

| Report date | `source_published_at` | `source_available_at` | `availability_basis` |
|---|---|---|---|
| from 2025-05-12 | listing `CreatedOn` (actual upload, IST) | = `source_published_at` | `PUBLISHED` |
| before 2025-05-12 | NULL | D+2 09:00 IST | `ESTIMATED_D_PLUS_2_0900` |

- **Earlier upload times:** before 2025-05-12, `CreatedOn` is a bulk re-upload date. It is kept in the raw `.meta.json` (listing entry) but never stored as a publication time. `source_document.published_at` is NULL for those files.
- **Implausible upload times:** a `CreatedOn` before the operating day ended is not used. The row is estimated and an `ESTIMATED_AVAILABILITY` warning is recorded.
- **Enforced in the database:** CHECK constraints from `0002_grid_india` reject an estimate that carries an actual time, or that is not exactly D+2 09:00 IST.
- **Look-ahead-free use:** select rows with `source_available_at <= cutoff`. Revisions are later versions with later availability. An unchanged re-publication keeps the earlier row, because availability columns are lineage and never make a row "changed".

## Field mapping

### Section A → `grid_daily_power_supply` (natural key `region_code, report_date`; NULL region = All India)

| Label (wording varies by year) | Column | Unit |
|---|---|---|
| Demand Met during Evening Peak (19:00 or 20:00, as stated in the label) | `evening_peak_demand_met_mw` | MW |
| Peak Shortage / "Shoratge … during Evening Peak" | `evening_peak_shortage_mw` | MW |
| Energy Met | `energy_met_mu` | MU |
| Hydro / Wind / Solar Gen | `hydro_gen_mu`, `wind_gen_mu`, `solar_gen_mu` | MU ("-" → NULL) |
| Energy Shortage | `energy_shortage_mu` | MU |
| Maximum Demand Met During the Day | `max_demand_met_mw` | MW |
| Time of Maximum Demand Met | `max_demand_met_time` | IST time |

**Regional sums:** for the additive rows (all except maximum demand and its time), the regional values are summed and compared with the published All-India value. A difference above 1 MW or 0.05 MU is a `TOTAL_MISMATCH` WARNING with both numbers. **The published All-India value is stored as published.**

**Evening-peak hour:** the hour of the evening peak changes between reports (19:00 or 20:00). It stays in the raw record.

### Sections C + J → `grid_state_power_supply` (natural key `area_code, report_date`)

| Column | Source |
|---|---|
| `max_demand_met_mw` | Max. Demand Met during the day (MW) |
| `shortage_at_max_demand_mw` | Shortage at/during Maximum Demand (MW) |
| `energy_met_mu` | Energy Met (MU) |
| `drawal_schedule_mu` | Drawal Schedule (MU) (negative for DVC) |
| `od_ud_mu` | OD(+)/UD(−) (MU) |
| `max_od_mw`, `max_ud_mw` | "Max OD(+)/UD(−) (MW)" `631/ -686` → 631, −686; older single "Max OD (MW)" → `max_od_mw` only |
| `energy_shortage_mu` | Energy Shortage (MU) |

**Areas** (`src/pipelines/grid_india/entities.py`, exact matching; unknown names are rejected as `UNMAPPED_ENTITY`):

| `area_type` | `area_code` | Examples |
|---|---|---|
| STATE | ISO 3166-2:IN (= `state_code`) | "UP" → UP, "MP" → MP, "Odisha" → OD, **"DNHDDPDCL" → DH** (published name kept) |
| SYSTEM | `DVC` (ER), `JK_LA` (NR) | "DVC", "J&K(UT) & Ladakh(UT)" |
| BULK_CONSUMER | `BULK_<region>` | "Bulk Consumer_WR ISTS" |
| ENTITY | `RAILWAYS_<region>`, `AMNSIL`, `BALCO`, `RIL_JAMNAGAR`, `WELSPUN`, `HZL_DARIPA` | Section J (sheet `MOP_E1`, 2026); in Section C in older workbooks |

**DNHDDPDCL:** the figures are the utility's demand. They exclude consumers connected directly to the national grid, which are reported in "Bulk Consumer_WR ISTS". In August 2026 its maximum demand (1,425 MW) equals CEA's peak met for DH exactly, as Gujarat, Goa and Maharashtra also match.

**Bulk consumers:** a `BULK_<region>` row is the sum of that region's Section J entities. Both are stored; do not add them together. The sum is checked, with a tolerance of 0.05 MU per entity for rounding. A larger gap is a `TOTAL_MISMATCH` WARNING.

**Region assignment:** the region comes from the reference data. The sheet's region label (printed on one row per block) is a cross-check, and a disagreement is a `REGION_LABEL_MISMATCH` WARNING. A missing expected state or system is a `MISSING_AREA` WARNING.

### Section G → `grid_sourcewise_generation` (natural key `region_code, fuel_type, report_date`)

**Heading:** "G. Sourcewise generation (Gross) (MU)" in sheet `MOP_E`. The header row is NR, WR, SR, ER, NER, All India, % Share.

**Layout check:** every workbook checked from 2023-01 to 2026-11 (68, including all 31 of August 2026) has the same heading, header and row labels. Only the row position changes, so the section and its header are found by label.

| Published label (`fuel_label`, stored verbatim) | `fuel_type` |
|---|---|
| Coal | `COAL` |
| Lignite | `LIGNITE` |
| Hydro | `HYDRO` |
| Nuclear | `NUCLEAR` |
| Gas, Naptha & Diesel | `GAS_NAPHTHA_DIESEL` |
| RES (Wind, Solar, Biomass & Others) | `RES` (not split into wind/solar; not assumed) |

- **Region values:** `gross_generation_mu` is gross generation in MU, with "-" stored as NULL. One row per region × fuel, 30 per workbook.
- **Not stored:** the "All India" column, the "Total" row and the "% Share" column are additive or derived. They are compared, not stored, and there is no All-India row. The two "Share of …" rows are recognised and ignored.
- **Totals compared** (difference > 0.05 MU → `TOTAL_MISMATCH` WARNING, values stored as published):
  - each fuel: the sum of regions vs the published All India value;
  - each region: the sum of fuels vs the published Total;
  - All India: the sum of the All-India fuel values vs the published All-India Total.

  The sum-of-fuels checks are skipped while any fuel row is unmapped.
- **Unknown labels:** any other label becomes records with `fuel_type` NULL, which are rejected as `UNMAPPED_FUEL` (ERROR, written to the rejects file); the run is PARTIAL. A missing known fuel is a `MISSING_FUEL` WARNING.

### Section F → `grid_generation_outage` (natural key `region_code, sector, report_date`)

**Heading:** "F. Generation Outage(MW)" in sheet `MOP_E`. The header row is NR, WR, SR, ER, NER, TOTAL, % Share.

**Layout check:** all 102 workbooks checked (2023-01 to 2026-11) have the same heading, header and rows. The section is found by label.

| Published row | `sector` |
|---|---|
| Central Sector | `CENTRAL` |
| State Sector | `STATE` |
| Total | validation only |

- **Region values:** `outage_capacity_mw` is in MW, with "-" stored as NULL. One row per region × sector, 10 per workbook.
- **Not available:** `plant_id`, `unit_id` and `outage_reason` stay NULL, because this section is aggregate. Unit-level outages come from CEA DGR (`cea_dgr_unit_outage`).
- **Not stored:** the TOTAL column and Total row are additive. They are compared, not stored, and there is no All-India row.
- **Totals compared** (difference > 1 MW → `TOTAL_MISMATCH` WARNING, values stored as published):
  - each sector: the sum of regions vs TOTAL;
  - each region: Central + State vs Total;
  - All India: the sector TOTALs vs the grand Total.
- **Unknown rows:** any other row label is rejected as `UNMAPPED_SECTOR` (ERROR, rejects file); the run is PARTIAL. A missing sector is a `MISSING_SECTOR` WARNING.

## Data quality

| Check | Result |
|---|---|
| Listing/CDN HTTP error after retries, HTML instead of a workbook | every section's run FAILED |
| Unreadable workbook, `MOP_E` missing, report date ≠ listing title | that document fails in each section's run |
| Section, header row or required label missing | that section's run FAILED; other sections unaffected |
| Unknown extra label in section A | `UNKNOWN_LABEL` WARNING (not loaded, not guessed) |
| Non-numeric value in a row | `PARSE_ERROR`; record rejected, run PARTIAL |
| Unknown area name | `UNMAPPED_ENTITY` ERROR; rejected and written to the rejects file |
| Negative demand / energy / shortage | `OUT_OF_RANGE` ERROR |
| Regional sums / bulk-consumer sums differ from published totals | `TOTAL_MISMATCH` WARNING (both values in the text) |
| Day with no workbook listed | `MISSING_DAY` in each section's run (INFO for yesterday, WARNING older) |

## Idempotency, revisions, reprocessing

- **Byte-identical workbooks** are skipped per dataset (`DUPLICATE_DOCUMENT`).
- **Changed records** become new versions, VERSION policy. Every workbook version of a day is loaded in upload order, so a same-day revision becomes the latest version with its own later availability.
- **`--reprocess`** re-parses stored workbooks. Records that this same document already loaded are counted as unchanged and not compared with the latest version, so replaying an original after its revision can't re-insert old values. Records the document never loaded (e.g. rejected before a fix) are inserted.

## Raw storage

```
RAW_DATA_DIR/grid_india/grid_india_psp_workbook/listing/YYYY/MM/DD/<retrieved>Z.json
RAW_DATA_DIR/grid_india/grid_india_psp_workbook/xls/YYYY/MM/DD/<retrieved>Z.xls  (+ .meta.json: listing entry, URL, availability)
RAW_DATA_DIR/grid_india/grid_india_psp_<section>-rejected/...                   (rejected records)
```

## Running

```bash
python -m scripts.ingest_grid_india psp --date 2026-11-10 --dry-run
python -m scripts.ingest_grid_india psp --start-date 2026-11-04 --end-date 2026-11-09          # max 31 days
python -m scripts.ingest_grid_india psp --date 2026-11-10 --sections states
python -m scripts.ingest_grid_india psp --date 2026-10-15 --reprocess
```

The region and state reference rows (seeded by `python -m scripts.ingest_cea reference`) must exist. A dry run seeds them inside its rollback.

**Exit codes:** `0` every section SUCCESS, `1` any section PARTIAL/FAILED, `2` invalid arguments or configuration.

## Live verification record (milestone 1, 2026-11-13)

| Runs | Days | Workbooks (downloaded once per command) | Supply inserted | States inserted | Status |
|---|---|---|---|---|---|
| 1681/1682 dry run | 2026-11-10 | 1 | 6 (rolled back) | 46 (rolled back) | SUCCESS |
| 1683/1684 | 2026-11-10 | 1 | 6 | 46 | SUCCESS |
| 1685/1686 | 2026-11-04..09 | 6 | 36 | 276 | SUCCESS |
| 1687/1688 | 2026-10-15 (original + same-day revision) | 2 | 6 (+6 unchanged) | 47 (46 + 1 revised version, 45 unchanged) | SUCCESS |
| 1689/1690 | 2025-05-09..13 (across the 2025-05-12 cut-over) | 5 | 30 | 195 | SUCCESS |
| 1691–1698 | the same four, re-run | 14 duplicate per section | **0** | **0** | SUCCESS |
| 1735–1740 | `--reprocess --dry-run` of all ranges | 14 | **0** (all unchanged) | **0** (all unchanged) | SUCCESS |

**Checks in PostgreSQL:**
- **Rows:** `grid_daily_power_supply` 78 rows, 6 per day (5 regions + All India). `grid_state_power_supply` 564 rows: 46 areas per day in 2026 and 39 in May 2025 (fewer entities then).
- **Duplicates and versions:** no record is stored twice. One key has two versions, the genuine 2026-10-15 revision for Telangana (max OD/UD 1713/−1964 at 10:47 → 1096/−1421 at 16:38).
- **Timestamps:** 2025-05-09..11 have `ESTIMATED_D_PLUS_2_0900` (available D+2 09:00, no publication time, `source_document.published_at` NULL). 2025-05-12 onwards have `PUBLISHED` (e.g. 2025-05-12 → 2025-05-13 17:12; 2026-11-10 → 2026-11-11 12:13).
- **Raw files:** 28 source documents (14 workbooks × 2 sections) point to 14 raw files, and all checksums match.
- **Source discrepancies, recorded as warnings with values stored as published:**
  - 2026-11-10: evening-peak demand, regions 187,221 vs All India 188,852 MW; solar 550.46 vs 552.59 MU.
  - 2026-11-09: wind 142.46 vs 142.00 MU.
- **Other tables:** all other pipelines' tables have unchanged row counts.

### Milestone 2: Section G (2026-11-13), `--sections fuel`

| Runs | Days | Workbooks | Inserted | Notes |
|---|---|---|---|---|
| 2150 dry run | 2026-11-10 | 1 | 30 (rolled back) | |
| 2151 | 2026-11-10 | 1 | 30 | |
| 2152 | 2026-11-04..09 | 6 | 180 | |
| 2153 | 2026-10-15 (original + revision) | 2 | 30 (+30 unchanged) | Section G was not changed by that revision, so no new versions |
| 2154 | 2025-05-09..13 (across the cut-over) | 5 | 150 | estimated availability up to 05-11, published from 05-12 |
| 2155 | 2023-01-15 (earliest XLS layout) | 1 | 30 | estimated availability |
| 2156 / 2157 | 2026-08-13 / 2026-08-15 | 1 / 1 | 30 / 30 | 6 / 3 `TOTAL_MISMATCH` warnings (source inconsistencies) |
| 2158–2164 | the same, re-run | 17 duplicate | **0** | |
| 2165–2168 | `--reprocess --dry-run` | 15 | **0** (all unchanged) | |

**Checks in PostgreSQL:**
- **Rows and mapping:** 480 rows, 30 per day (5 regions × 6 fuels), with no All-India rows and no duplicate keys. Each `fuel_type` has exactly one published `fuel_label`.
- **Document linkage:** every row's document belongs to dataset `GRID_INDIA_GENERATION` and to the row's own run, with the same report date. The 14 workbooks also loaded for section A have identical checksums in both datasets. All 17 checksums match the raw files.
- **Timestamps:** `source_document.published_at` is set exactly when the rows are `PUBLISHED`, and equals `source_published_at`.
- **Source inconsistencies,** stored as published:
  - 2026-08-13: every region's fuels differ from its Total by 0.13–0.48 MU, and the All-India Total by 0.09.
  - 2026-08-15: WR fuels 1,755.42 vs Total 1,743.72 MU (11.70); coal regions 3,489.25 vs All India 3,489.00; All-India Total off by 0.13.
- **Other tables:** besides `grid_sourcewise_generation`, only `ingestion_run`, `source_document` and `data_quality_issue` changed. All other tables have the same counts.

### Milestone 3: Section F (2026-11-13), `--sections outage`

| Runs | Days | Workbooks | Inserted | Notes |
|---|---|---|---|---|
| 3328 dry run | 2026-11-10 | 1 | 10 (rolled back) | |
| 3329 | 2026-11-10 | 1 | 10 | |
| 3330 | 2026-11-04..09 | 6 | 60 | |
| 3331 | 2026-10-15 (original + revision) | 2 | 10 (+10 unchanged) | Section F not changed by that revision |
| 3332 | 2025-05-09..13 | 5 | 50 | estimated up to 05-11, published from 05-12 |
| 3333 | 2023-01-15 | 1 | 10 | total 62,742.54 MW = published Total |
| 3334 / 3335 | 2026-08-13 / 15 | 1 / 1 | 10 / 10 | |
| 3336–3342 | re-run | 17 duplicate | **0** | |
| 3343–3346 | `--reprocess --dry-run` | 15 | **0** (all unchanged) | |

**Checks in PostgreSQL:**
- **Rows:** 160 rows, 10 per day; no All-India rows, no duplicate keys, and no plant, unit or reason values.
- **Document linkage:** documents belong to `GRID_INDIA_PSP_OUTAGE` and to the row's own run. All 17 checksums match, and they are identical to the Section G documents for the same workbooks.
- **Timestamps:** `source_document.published_at` is set exactly when rows are `PUBLISHED`.
- **Data quality:** no `TOTAL_MISMATCH` on any day.
- **Other tables:** only `grid_generation_outage`, `ingestion_run`, `source_document` and `data_quality_issue` changed.

## Known limitations

- **Terms of use:** not verified (G5).
- **Pre-2023 reports:** PDF only; deferred (G4).
- **Undocumented API:** the listing semantics (months, file names) were inferred from observation.
- **Remaining sections:** E, `TimeSeries`, `IR-Line` and `CrossBorder` are not yet loaded.
