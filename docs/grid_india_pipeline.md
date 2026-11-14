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
| Alembic `0003_grid_regional_exch_od_ud` (`grid_regional_exchange.od_ud_mu`) | ✅ applied 2026-11-13 after a rolled-back dry run (14/14 checks) |
| Section E → `grid_regional_exchange` | ✅ milestone 4, live-verified 2026-11-13 |
| Alembic `0004_grid_intl_region_label` (`grid_international_line_exchange.region_label`) | ✅ applied after a rolled-back dry run (14/14 checks) |
| Country reference rows BTN, NPL, BGD (`python -m scripts.ingest_grid_india reference`) | ✅ seeded 2026-11-13 |
| IR-Line inter-regional → `grid_interregional_line_exchange` | ✅ milestone 5, live-verified 2026-11-13 |
| IR-Line international → `grid_international_line_exchange` | ✅ milestone 5, live-verified 2026-11-13 |
| Alembic `0005_grid_crossborder` (direction `NET` allowed on `grid_crossborder_market_schedule`) | ✅ applied after a rolled-back dry run (15/15 checks) |
| CrossBorder → `grid_crossborder_market_schedule` | ✅ milestone 6, live-verified 2026-11-13 |
| Alembic `0006_grid_scada_instant_comment` (comment on `grid_15min_scada.interval_start`) | ✅ applied after a rolled-back dry run (11/11 checks) |
| TimeSeries → `grid_15min_scada` | ✅ milestone 7, live-verified 2026-11-14 |
| Pre-2023 PDF reports | deferred (G4) |
| Tests | 88 unit + 53 integration (synthetic workbooks, mocked HTTP, real schema in rolled-back transactions) |
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
        ├──► OutagePipeline  (dataset GRID_INDIA_PSP_OUTAGE,  own ingestion_run) ─► grid_generation_outage
        ├──► ExchangePipeline (dataset GRID_INDIA_EXCHANGE,  own ingestion_run) ─► grid_regional_exchange
        ├──► IrLinesPipeline   (dataset GRID_INDIA_PSP_IR_LINES,   own run) ─► grid_interregional_line_exchange
        ├──► IntlLinesPipeline (dataset GRID_INDIA_PSP_INTL_LINES, own run) ─► grid_international_line_exchange
        ├──► CrossBorderPipeline (dataset GRID_INDIA_PSP_CROSSBORDER, own run) ─► grid_crossborder_market_schedule
        └──► TimeSeriesPipeline (dataset GRID_INDIA_SCADA, own run) ─► grid_15min_scada
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

### Section E → `grid_regional_exchange` (natural key `region_code, report_date`)

**Heading:** "E. Import/Export by Regions (in MU) - Import(+ve)/Export(-ve); OD(+)/UD(-)" in sheet `MOP_E`. The header row is NR, WR, SR, ER, NER, TOTAL.

**Layout check:** the layout is identical in all 136 workbooks checked (54 distinct, 2023-01 to 2026-11). The section is found by label.

| Published row | Column | Notes |
|---|---|---|
| Schedule(MU) | `scheduled_exchange_mu` | signed, as published |
| Actual(MU) | `actual_exchange_mu` | signed, as published |
| O/D/U/D(MU) | `od_ud_mu` (migration `0003`) | published value, not derived. It differs from Actual − Schedule in 10 of 80 loaded rows, by up to 0.013 MU (rounding in the source) |
| heading convention | `sign_convention` | "Import(+ve)/Export(-ve); OD(+)/UD(-)". A heading without an import/export convention fails the section, so it is never assumed |
| — | `exchange_direction` | left NULL. The sign carries the direction, and schedule and actual can have opposite signs for the same row (e.g. ER on 2025-05-10, SR on 2026-08-15) |

- **Totals:** the TOTAL column is additive. It is not stored and is checked against the regional sums for all three rows (it always matched exactly in the audit). The scheduled TOTAL is within ±0.012 MU of zero.
- **O/D/U/D check:** published O/D/U/D is compared with Actual − Schedule per region and for TOTAL. A difference above 0.05 MU is an `OD_UD_MISMATCH` WARNING (none so far), and the published value is kept.
- **Unknown rows and columns:** an unknown row label is `UNKNOWN_LABEL` and an unknown header column is `UNKNOWN_COLUMN` (WARNING, not loaded). A missing O/D/U/D row is `MISSING_FIELD` (stored NULL, not derived). Missing Schedule or Actual rows, or a missing region header, fail the section.

### IR-Line (inter-regional part) → `grid_interregional_line_exchange` (natural key `from_region_code, to_region_code, line_name, report_date`)

**Sheet `IR-Line`.** Its "Date of Reporting" cell is the report date + 1 in every workbook. The heading convention "Import=(+ve) /Export =(-ve) for NET (MU)" is stored as `direction_convention`; a sheet without it fails the section. The layout is identical in all 54 distinct workbooks audited (2023-01-15 to 2026-11-12).

| Published | Column | Notes |
|---|---|---|
| block heading "Import/Export of X (With Y)" | `from_region_code` = X, `to_region_code` = Y | an unknown region in the heading is `UNMAPPED_REGION` (ERROR, rejected) |
| Voltage Level / Line Details / No. of Circuit | `voltage_level`, `line_name`, `circuits` | names exactly as published (66 names seen; the roster varies over time); `-` circuits → NULL |
| Max Import / Max Export (MW) | `max_import_mw`, `max_export_mw` | as published |
| Import / Export (MU) | `import_mu`, `export_mu` | negative values are `OUT_OF_RANGE` (ERROR) |
| NET (MU) | `net_exchange_mu` | published value, not derived; compared with Import − Export (`NET_MISMATCH` WARNING above 0.05 MU, never seen live) |

- **Subtotals:** each block's "X-Y" subtotal row is not stored; it is compared with the sum of the block's lines. A gap explained by leaving out exactly one line is `SUBTOTAL_EXCLUDES_LINE` INFO naming the line (ER-SR always leaves out `TALCHER-I/C`). Any other gap is `TOTAL_MISMATCH` WARNING.
- **Unknown rows** inside the part are `UNKNOWN_ROW` WARNING and not loaded.
- **Missing sheet:** a workbook without `IR-Line` fails only the two IR-Line sections; the other sections still load.

### IR-Line (international part) → `grid_international_line_exchange` (natural key `country_code, line_name, report_date`)

Below the heading "INTERNATIONAL EXCHANGES" with the sign convention "Import(+ve)/Export(-ve)" (stored as `sign_convention`; required, never assumed).

| Published | Column | Notes |
|---|---|---|
| Line Name | `line_name`; `country_code` from the **exact-name** reference `INTERNATIONAL_LINES` in `reference.py` (16 names) | only whitespace and case are normalised; no keywords, no row position. An unknown name is `UNMAPPED_COUNTRY` (ERROR, rejected) |
| State (BHUTAN / NEPAL / BANGLADESH) | cross-check only | printed on one row per group; where present and different from the reference, `COUNTRY_LABEL_CONFLICT` (ERROR, rejected) |
| Region | `region_code` (ER, NER, NR); the cleaned cell text in `region_label` (migration `0004`) | "ER\n(Isolated from Indian Grid)" (Godda) → `ER` + "ER (Isolated from Indian Grid)" |
| Max / Min / Avg (MW), Energy Exchange (MU) | `max_mw`, `min_mw`, `avg_mw`, `energy_exchange_mu` | signed, as published; `direction` left NULL |

- **Countries:** `pi.country` rows BTN, NPL, BGD (ISO alpha-3; MMR is added for CrossBorder) are seeded by `python -m scripts.ingest_grid_india reference`. A real run requesting `intl_lines` without them fails with a configuration error; a dry run seeds them inside its rollback.
- **Section D check:** per-country line sums are compared with MOP_E Section D "Actual (MU)" (Bhutan, Nepal, Bangladesh, and "Godda -> Bangladesh" for the isolated Godda line). A difference above 0.05 MU is `SECTION_D_MISMATCH` WARNING; the published line values are kept. The check is skipped while any line is unmapped.
- **MW not reported:** rows with Max = Min = Avg = 0 but non-zero energy (the Bhutan receipt rows) are loaded as published and noted as `MW_NOT_REPORTED` INFO.

### CrossBorder → `grid_crossborder_market_schedule` (natural key `country_code, contract_type, power_exchange, direction, report_date`; `power_exchange` NULL-safe)

**Sheet `CrossBorder`, "CROSS BORDER EXCHANGE SCHEDULE"** (scheduled energy, MU). First published for report date **2023-07-06**. Its "Date of Reporting" must equal MOP_E's (report date + 1) and the title must be present, or the section fails. The grid (33 × 13) was identical in all 95 distinct workbooks audited (2023-07-06 to 2026-11-12), but everything is found by label.

Three blocks, each with rows Bhutan, Nepal, Bangladesh, Myanmar and a Total row:

| Block heading | `direction` | `sign_convention` (heading text) | Values |
|---|---|---|---|
| "Export From India (in MU)" | `EXPORT` | as published | non-negative (`OUT_OF_RANGE` ERROR otherwise) |
| "Import by India(in MU)" | `IMPORT` | as published | non-negative |
| "Net from India(in MU) … -ve : Export / +ve : Import" | `NET` (migration `0005`) | as published | signed, published value, not derived; a heading without the convention fails the section |

Columns (`contract_type`, `power_exchange`). The categories changed on **2023-10-01**, and the two regimes keep their own codes (decision C3):

| Header from 2023-10-01 | Header 2023-07-06 to 2023-09-30 | `contract_type` | `power_exchange` |
|---|---|---|---|
| GNA / (ISGS/PPA) (Import block prints "(ISGA/PPA)") | (ISGS/LTA/MTOA) / PPA | `GNA` / `ISGS_LTA_MTOA_PPA` | NULL |
| T-GNA / BILATERAL / TOTAL | STOA / BILATERAL / TOTAL | `T_GNA_BILATERAL` / `STOA_BILATERAL` | NULL |
| COLLECTIVE / IDAM / IEX, PXIL, HPX | same | `COLLECTIVE_IDAM` | `IEX` / `PXIL` / `HPX` |
| COLLECTIVE / RTM / IEX, PXIL, HPX | same | `COLLECTIVE_RTM` | `IEX` / `PXIL` / `HPX` |
| TOTAL | same | not stored (row total, validated) | |

- **Header check:** the header must match one of these two label sets exactly. An unknown label, a new exchange, or a mix of labels fails the section; categories are never mapped by position or by date. "IDAM" is stored as published and not linked to `market_master` (the workbook does not define it).
- **Countries:** exact labels only (`CROSSBORDER_COUNTRIES` in `reference.py`): Bhutan → BTN, Nepal → NPL, Bangladesh → BGD, Myanmar → MMR (seeded by `reference`; Myanmar has been 0 in every workbook and is stored as published, decision C2). An unknown label is `UNMAPPED_COUNTRY` (ERROR, rejected); a country listed twice in a block is `DUPLICATE_ROW` (ERROR, all its rows rejected); a missing country is `MISSING_COUNTRY` WARNING. `interconnector_id` stays NULL (country-level data).
- **Totals (decision C4):** each row's TOTAL column and each block's Total row are additive. They are not stored and are compared with the sums (`TOTAL_MISMATCH` WARNING above 0.05 MU; never seen).
- **Net check:** Net is compared with Import − Export for every country, the Total row and every column. A difference above 0.05 MU is `NET_MISMATCH` WARNING and the published value is kept. Six cells on 2026-06-15 and 2026-08-30 differ by 0.02 (rounding) and pass.
- **Values:** a blank cell is stored as NULL with `MISSING_VALUE` WARNING (never derived); a non-numeric cell is `PARSE_ERROR` (that cell's record rejected). Neither has been seen.
- **Absent sheet (decision C5):** before 2023-07-06 the sheet is expected to be absent: the run is SUCCESS with 0 rows and `SECTION_NOT_PUBLISHED` INFO. From 2023-07-06 a missing sheet fails the section.
- **No cross-section check (decision C6):** Section D and the IR-Line international lines are *actual* flows; CrossBorder is *scheduled*. They differ in scope (e.g. Bhutan schedule about 25 MU/day below actual since 2025-10; Bangladesh excludes Godda), so no equality check is made.

### TimeSeries → `grid_15min_scada` (natural key `region_code` (NULL = All India), `interval_start`)

**Sheet `TimeSeries`, "15 Min (INSTANTANEOUS) ALL INDIA GRID FREQUENCY, GENERATION & DEMAND MET (SCADA DATA)"**, first published for report date **2024-11-04**. One row per 15-minute mark, 0:00 to 23:45 of the report day. The values are **instantaneous readings at the mark, not averages** (decision T2): they are stored with the mark in `interval_start`, whose column comment says so (migration `0006`). The title and the "Date of Reporting" (report date + 1) are required. Dataset `GRID_INDIA_SCADA` (the unused placeholder, repurposed, decision T1).

| Published (label, footnote markers removed) | Column | Notes |
|---|---|---|
| TIME | `interval_start` = report date + mark, IST (timezone-aware) | written as "H:MM", "H:MM:SS" (2024-11-04) or a day fraction stored as text (2024-11-05); anything that is not an exact 15-minute mark is a `PARSE_ERROR` |
| — | `region_code` NULL | All India (convention from migration 0002) |
| FREQUENCY (Hz) | `grid_frequency_hz` | |
| DEMAND MET | `demand_met_mw` | from 2026-05-06 a footnote says it includes storage load; earlier layouts do not say. Stored as published across layouts (T8) |
| STORAGE DEMAND (PSP&BESS) | `storage_demand_mw` | published from 2026-06-01; NULL before, never 0 (T4) |
| NUCLEAR, WIND, SOLAR, HYDRO, GAS, THERMAL, OTHERS | `nuclear_/wind_/solar_/hydro_/gas_/thermal_/other_generation_mw` | as published; small negative values (e.g. solar at night) are kept |
| STORAGE (PSP&BESS) | `storage_generation_mw` | published from 2026-05-06; NULL before (T4) |
| NET DEMAND MET | `net_demand_met_mw` | published; checked against Demand − Wind − Solar |
| TOTAL GENERATION | `total_generation_mw` | published; checked against the sum of sources |
| NET TRANSNATIONAL EXCHANGE | `net_transnational_exchange_mw` | import positive; the header (five wordings seen) must state it, or the section fails |

**Layouts** (header labels must match one of them exactly, otherwise the section fails): 13 columns (2024-11-04 to 2026-05-05), 15 columns with storage generation (2026-05-06 to 2026-05-31), 16 columns with storage demand (from 2026-06-01).

**Checks** (values are always stored as published):

| Check | Issue |
|---|---|
| Sheet absent before 2024-11-04 | `SECTION_NOT_PUBLISHED` INFO, SUCCESS with 0 rows (T6); absent from that date → section FAILED |
| Sheet with headers but no rows (2025-07-15) | `NO_INTERVAL_DATA` WARNING, SUCCESS with 0 rows (T6) |
| Mark not published (2025-05-11 14:45) | `MISSING_INTERVAL` WARNING; never filled |
| Same mark twice | `DUPLICATE_INTERVAL` ERROR; both rows rejected |
| Blank value | stored NULL, `MISSING_VALUE` WARNING; never derived |
| Net Demand Met ≠ Demand − Wind − Solar (> 1 MW) | `NET_DEMAND_MISMATCH` WARNING (never seen) |
| Total ≠ sum of sources, but = sum without storage (the 15-column layout, despite its formula label) | `TOTAL_EXCLUDES_STORAGE` INFO, one per document (T5) |
| Total ≠ sum of sources otherwise (2024-11-22 18:00–18:30) | `TOTAL_MISMATCH` WARNING |
| Frequency outside 49–51 Hz (0 Hz on telemetry loss), or every source 0 while demand is not | `TELEMETRY_SUSPECT` WARNING; zeros stored as published, never NULL (T3) |
| Day's highest demand above Section A "Maximum Demand Met During the Day" (1-minute SCADA) + 1 MW | `MAX_DEMAND_ABOVE_SECTION_A` WARNING (T7; the only cross-check; Section G and NPP are not comparable) |

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
python -m scripts.ingest_grid_india reference                                  # once, idempotent
python -m scripts.ingest_grid_india psp --date 2026-11-10 --sections ir_lines,intl_lines
python -m scripts.ingest_grid_india psp --date 2026-11-10 --sections crossborder
python -m scripts.ingest_grid_india psp --date 2026-11-10 --sections timeseries
python -m scripts.ingest_grid_india psp --date 2026-10-15 --reprocess
```

The region and state reference rows (seeded by `python -m scripts.ingest_cea reference`) must exist, and for `intl_lines` and `crossborder` the country rows (`python -m scripts.ingest_grid_india reference [--dry-run]`). A dry run seeds missing rows inside its rollback.

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

### Milestone 4: Section E (2026-11-13), `--sections exchange`

**Migration:** `0003` was first dry-run under the approved ID `0003_grid_regional_exchange_od_ud`. That failed safely, because the 33-character ID exceeds `alembic_version.version_num varchar(32)`, and the transaction rolled back. With approval, the ID was shortened to `0003_grid_regional_exch_od_ud` and the content kept identical. The dry run then passed 14/14 checks: one nullable `numeric(18,4)` column added, the downgrade guard works, and the schema and row counts were identical after rollback. Applied 2026-11-13.

| Runs | Days | Workbooks | Inserted | Notes |
|---|---|---|---|---|
| 4093 dry run | 2026-11-10 | 1 | 5 (rolled back) | |
| 4094 | 2026-11-10 | 1 | 5 | |
| 4095 | 2026-11-04..09 | 6 | 30 | |
| 4096 | 2026-10-15 (original + revision) | 2 | 5 (+5 unchanged) | Section E not changed by that revision |
| 4097 | 2025-05-09..13 | 5 | 25 | estimated up to 05-11, published from 05-12 |
| 4098 | 2023-01-15 | 1 | 5 | |
| 4099 / 4100 | 2026-08-13 / 15 | 1 / 1 | 5 / 5 | no Section E discrepancy on these days |
| 4101–4107 | re-run | 17 duplicate | **0** | |
| 4108–4112 | `--reprocess --dry-run` | 16 | **0** recreated | the 08-13..15 range also covered 08-14, never loaded, so its 5 rows were new (rolled back) |

**Checks in PostgreSQL:**
- **Rows:** 80 rows, 5 per day. There are no TOTAL rows, no duplicate keys, `exchange_direction` is always NULL, and there is one sign convention.
- **O/D/U/D:** stored for every row; 10 rows differ from Actual − Schedule by ≤ 0.013 MU, as published.
- **Document linkage:** documents belong to `GRID_INDIA_EXCHANGE` and to the row's own run. All 17 checksums match, and they are identical to the Section F documents for the same workbooks.
- **Timestamps:** `source_document.published_at` is set exactly when rows are `PUBLISHED`.
- **Data quality:** no warnings.
- **Other tables:** only `grid_regional_exchange`, `ingestion_run`, `source_document` and `data_quality_issue` changed.

### Milestone 5: IR-Line (2026-11-13), `--sections ir_lines,intl_lines`

**Migration and reference:** `0004_grid_intl_region_label` applied after a rolled-back dry run (14/14). Countries seeded with `reference` (dry run, then committed; a second run reported all three unchanged).

| Runs (IR / intl) | Report dates | Documents | Inserted IR / intl | Notes |
|---|---|---|---|---|
| 5881 / 5882 | 2026-11-04..10 | 7 | 441 / 84 | 7 subtotal INFO / 26 MW INFO + 3 Section D WARNING |
| 5883 / 5884 | 2026-10-15 (original + revision) | 2 | 63 (+63 unchanged) / 12 (+12) | IR-Line not changed by that revision |
| 5885 / 5886 | 2023-01-15 | 1 | 62 / 10 | estimated availability |
| 5887–5890 | 2026-08-13 / 15 | 1 / 1 | 63 / 12 each | |
| 5891 / 5892 | 2025-05-09..13 | 5 | 320 / 60 | across the G3 cutoff |

- **Rows:** 1012 inter-regional rows (62–64 per day, 7 region pairs, no NULL regions, NET = Import − Export on every row, one direction convention) and 190 international rows (10–12 per day, all three countries every day, one sign convention, `direction` always NULL).
- **Region labels:** Godda (BGD) is stored as `region_code` ER with `region_label` "ER (Isolated from Indian Grid)" (15 rows); all other labels equal the region code.
- **Section D:** three WARNINGs, the same as the audit: Nepal 2026-11-07 (2.920 MU) and 2026-11-08 (2.520 MU), Bhutan 2026-11-09 (1.040 MU). Values stored as published.
- **Subtotals:** the only subtotal note is ER-SR excluding `TALCHER-I/C` (one per day). No `TOTAL_MISMATCH`, `NET_MISMATCH`, `UNKNOWN_ROW` or rejected rows.
- **Re-run:** runs 5893–5896 inserted 0 (`DUPLICATE_DOCUMENT` INFO). `--reprocess --dry-run` on 2026-11-04..10 and 2026-10-15 inserted 0 (all unchanged).
- **Document linkage:** documents belong to the section's dataset and the row's own run; 16 distinct checksums per dataset (17 documents including the revision), the SHA-256 of the shared raw workbook.
- **Timestamps:** `PUBLISHED` with the listing upload time from 2025-05-12; `ESTIMATED_D_PLUS_2_0900` before.
- **Other tables:** only `country` (+3), `grid_interregional_line_exchange` (+1012), `grid_international_line_exchange` (+190), `ingestion_run` (+16), `source_document` (+34) and `data_quality_issue` (+81) changed. Health check: HEALTHY.

### Milestone 6: CrossBorder (2026-11-13), `--sections crossborder`

**Migration and reference:** `0005_grid_crossborder` (direction CHECK allows `NET`) applied after a rolled-back dry run (15/15). Dataset `GRID_INDIA_PSP_CROSSBORDER` registered (id 12022). Myanmar (MMR) seeded with `reference` (dry run, then committed).

| Run | Report dates | Documents | Inserted | Notes |
|---|---|---|---|---|
| 6883 | 2023-07-05 | 1 | 0 | `SECTION_NOT_PUBLISHED` INFO (sheet not yet published) |
| 6884 / 6885 | 2023-07-06 / 2023-09-30 | 1 / 1 | 96 / 96 | pre-GNA categories (`ISGS_LTA_MTOA_PPA`, `STOA_BILATERAL`) |
| 6886 | 2023-10-01 | 1 | 96 | first GNA / T-GNA workbook |
| 6891 | 2025-05-11..12 | 2 | 192 | across the G3 cutoff |
| 6887 / 6890 | 2026-03-30 / 2026-10-15 (original + revision each) | 2 / 2 | 96 (+96 unchanged) each | revisions did not change CrossBorder |
| 6888 / 6889 | 2026-06-15 / 2026-08-30 | 1 / 1 | 96 / 96 | Net rounding cells (0.02): no warning, published values stored |
| 6892 | 2026-11-04..10 | 7 | 672 | |

- **Rows:** 1536 (96 per day: 3 blocks × 4 countries × 8 columns), one version per key, no NULL values, `interconnector_id` NULL. Export/Import 0 to 39.60 MU; Net -24.47 to 39.60. One sign convention per block. Myanmar 384 rows, all 0.
- **Spot check 2026-11-10:** Bangladesh GNA export 24.47 (Net -24.47), T-GNA bilateral export 0.06; Nepal RTM IEX import 10.57, GNA import 0.87, as in the sheet.
- **Data quality:** no warnings or rejections; the only stored Net vs Import − Export difference is Nepal RTM IEX on 2026-08-30 (-1.22 vs -1.20), within tolerance.
- **Re-run:** runs 6893–6895 inserted 0 (`DUPLICATE_DOCUMENT` INFO). `--reprocess --dry-run` on 2026-11-04..10, 2026-03-30 and 2026-10-15 inserted 0 (all unchanged).
- **Document linkage:** rows link to 16 `GRID_INDIA_PSP_CROSSBORDER` documents of their own runs (SHA-256, `published_at` set exactly when `PUBLISHED`); the 19 documents (including revisions and 2023-07-05) share checksums with the IR-Line documents of the same workbooks.
- **Timestamps:** `PUBLISHED` with the listing upload time from 2025-05-12; `ESTIMATED_D_PLUS_2_0900` before.
- **Other tables:** only `country` (+1), `grid_crossborder_market_schedule` (+1536), `ingestion_run` (+13), `source_document` (+19), `data_quality_issue` (+11) and `source_dataset` (+1) changed. Health check: HEALTHY.

### Milestone 7: TimeSeries (2026-11-14), `--sections timeseries`

**Migration and dataset:** `0006_grid_scada_instant_comment` (column comment on `interval_start` only) applied after a rolled-back dry run (11/11). The `GRID_INDIA_SCADA` catalogue entry (id 197, no previous runs or documents) was updated in place and the mapping document regenerated.

| Run | Report dates | Documents | Inserted | Notes |
|---|---|---|---|---|
| 9663 | 2024-11-03 | 1 | 0 | `SECTION_NOT_PUBLISHED` INFO |
| 9664 / 9665 | 2024-11-04 / 05 | 1 / 1 | 96 / 96 | "H:MM:SS" times / day fractions stored as text |
| 9666 | 2024-11-22 | 1 | 96 | 3 × `TOTAL_MISMATCH` WARNING (18:00–18:30) |
| 9667 | 2025-03-15 | 1 | 96 | 4 × `TELEMETRY_SUSPECT` WARNING (13:15, 13:30: frequency 0, every source 0) |
| 9668 | 2025-07-15 | 1 | 0 | `NO_INTERVAL_DATA` WARNING |
| 9671 | 2025-05-11..12 | 2 | 191 | `MISSING_INTERVAL` 2025-05-11 14:45; across the G3 cutoff |
| 9672 | 2026-05-05..06 | 2 | 192 | 13 → 15 columns; `TOTAL_EXCLUDES_STORAGE` INFO on 2026-05-06 (95 marks) |
| 9673 | 2026-05-31..06-01 | 2 | 192 | 15 → 16 columns |
| 9669 / 9670 | 2026-03-30 / 2026-10-15 (original + revision each) | 2 / 2 | 96 (+96 unchanged) each | revisions did not change TimeSeries |
| 9674 | 2026-11-04..10 | 7 | 672 | |

- **Rows:** 1823 (19 days × 96, less the missing 14:45), one version per key, all marks on the quarter hour, `region_code` NULL, no NULL demand. Storage generation present from 2026-05-06 and storage demand from 2026-06-01, NULL before.
- **Spot check 2026-11-10 00:00:** 50.02 Hz, demand 153,240, storage demand 53, nuclear 7,222, wind 7,159, hydro 9,625, thermal 127,305, storage 1,342, others 3,390, net demand 146,081, total 157,505, exchange −472 MW, as in the sheet.
- **Re-run:** runs 9675–9677 inserted 0 (`DUPLICATE_DOCUMENT` INFO). `--reprocess --dry-run` on 2026-11-04..10, 2026-03-30 and 2025-05-11..12 inserted 0 (all unchanged).
- **Document linkage:** rows link to 19 `GRID_INDIA_SCADA` documents of their own runs (SHA-256, `published_at` set exactly when `PUBLISHED`); checksums are shared with the other sections' documents of the same workbooks.
- **Timestamps:** `PUBLISHED` with the listing upload time from 2025-05-12; `ESTIMATED_D_PLUS_2_0900` before.
- **Other tables:** only `grid_15min_scada` (+1823), `ingestion_run` (+15), `source_document` (+23) and `data_quality_issue` (+21) changed. Health check: HEALTHY.
- **Test fixture:** the synthetic CrossBorder values were changed to 3-decimal, non-zero values. After the live CrossBorder load, the old zero cells coincided with real rows in the database the integration tests run against.

## Known limitations

- **Terms of use:** not verified (G5).
- **Pre-2023 reports:** PDF only; deferred (G4).
- **Undocumented API:** the listing semantics (months, file names) were inferred from observation.
- **All workbook sections are loaded** (A, C, J, G, F, E, IR-Line, CrossBorder, TimeSeries). Sections B (frequency profile), D (transnational actuals, used only as a check), H and I are not stored.
- **TimeSeries:** all-India only (regional 15-minute SCADA was not found publicly); values are instantaneous, not averages; telemetry-loss zeros are stored as published and flagged.
- **CrossBorder header changes:** a new contract category, exchange or country label fails the section or is rejected until the reference / known layouts are extended deliberately; nothing is mapped by position.
- **Misfiled listing entry:** `24.02.26_NLDC_PSP_409.xls` (report date 2026-02-24) is listed only under FY 2024-25 month 02, so the neighbouring-month listing does not see it. It is byte-identical to `24.02.26_NLDC_PSP_573.xls`, so no data is lost; it is the only misfiled entry in 2023-01..2026-11 (affects every section; not changed).
- **International line roster:** a new or renamed international line is rejected (`UNMAPPED_COUNTRY`) until its exact name is added to `INTERNATIONAL_LINES`; it is never guessed.
