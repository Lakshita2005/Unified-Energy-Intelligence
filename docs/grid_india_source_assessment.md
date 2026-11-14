# Grid-India source assessment

**Status:** G1–G5 decided; migration `0002_grid_india` applied and milestone 1 (Sections A, C, J) implemented and live-verified on 2026-11-13. See `docs/grid_india_pipeline.md`. The text below is the assessment as written before implementation.

**Evidence:** sources were checked live on 2026-11-13 from this machine:
- API listings and period queries;
- 9 sample workbooks downloaded to the scratchpad, not to `raw_data/`.

Earlier findings are reused where they are recorded in the read-only forecasting repository and are marked with their source.

**Legend:** ✅ verified live today · 📄 verified earlier (forecasting repo, cited) · ⚠️ assumption / inference · ❓ unresolved

## 1. Summary

The one usable Grid-India source is NLDC's **Daily PSP report**. Its Excel workbook covers five of the six requested datasets in one file per day.

| # | Dataset | Source | Format | History | Usable for |
|---|---|---|---|---|---|
| 1 | Power Supply Position (all-India and regional) | Daily PSP, sheet `MOP_E` §A | XLS (PDF before 2023) | XLS from 2023-01-01; PDF back to FY 2010-11 | ✅ history and forecasting |
| 2 | Fuel-wise generation | Daily PSP §G (daily MU by fuel and region); `TimeSeries` (15-min MW, all-India) | XLS | §G from 2023 (XLS); 15-min from 2024-11 | ✅ |
| 3 | State-wise demand and availability | Daily PSP §C (+ `MOP_E1` §J bulk consumers) | XLS | from 2023-01 (XLS) | ✅ |
| 4 | Generating unit outages | Daily PSP §F only: **aggregate MW by sector × region**. No unit-level public file from NLDC | XLS | from 2023-01 | ⚠️ aggregate only; unit level comes from CEA DGR (already loaded) |
| 5 | Inter-regional exchange | Daily PSP §E (by region), `IR-Line` (by line), §D / `CrossBorder` (international) | XLS | `IR-Line` 2023-01; `CrossBorder` from 2023-07-06 (corrected by the CrossBorder audit) | ✅ |
| 6 | 15-minute SCADA | Daily PSP `TimeSeries`: **all-India** 15-min instantaneous frequency, demand and generation by source | XLS | from 2024-11-02..08 | ✅ all-India only; ❓ regional SCADA not found |

**Key changes against what was known:**
- **The official API is reachable from this machine.** The forecasting repo's cloud box was geo-blocked (`SOURCES.md` #5–7) and its PSP feed now runs on the office PC OWS2.
- **The workbook is far richer than the existing `pi.grid_*` tables.** Most tables need extra columns.
- **Two tables can't hold entities in the report.** `grid_state_power_supply` (`state_code NOT NULL`) can't hold DVC, J&K + Ladakh or bulk consumers, and `grid_sourcewise_generation` / `grid_regional_exchange` (`region_code NOT NULL`) can't hold the All-India row.
- **Migration needed:** an Alembic migration is required before loading (§5).

## 2. What was already known (not re-researched)

From `IEX_Price_Forecasting` (read-only, not modified):

| Finding | Source |
|---|---|
| File API: `POST https://webapi.grid-india.in/api/v1/file` with `{"_source":"GRDW","_type":"DAILY_PSP_REPORT","_fileDate":"<FY 2026-27>","_month":"MM"}`; files at `https://webcdn.grid-india.in/<FilePath>` | 📄 `iex_data_feeds/sources/grid_india_psp.py`, `SOURCES.md` #5–6 |
| `CreatedOn` in the listing = upload time. From op day 2025-05-12 the first upload is on D+1 at ~10:00–10:35 IST, and 99.4 % are public by D+2 09:00 | 📄 `reports/requests_log.md` (2026-10-20 audit of 3,989 files) |
| Before 2025-05-12, `CreatedOn` is a **bulk re-upload date** (2025-04-11/15, 2026-01/02), not the original publication time | 📄 same |
| Reports exist for 1,783 of 1,786 days from 2021-11-03; XLS from 2023-01-01, PDF before | 📄 same |
| The collector parsed only `MOP_E` §A, §B and the NR states; the `TimeSeries` sheet was not parsed | 📄 same, and code |
| The PDF and XLS of the same report disagree on derived 15-minute columns | 📄 same |
| Older PDFs need poppler `pdftotext`; pdfplumber produced implausible rows that were quarantined | 📄 `HANDOVER_OWS.md` §5 |
| Week-ahead all-India demand forecast: `_type: DEMAND_FORECAST`, PDF, published Thu/Fri ~17:30 | 📄 `SOURCES.md` #8 |
| Internet Archive fallback copies of 2019–22 PDFs are often truncated at 1 MiB | 📄 `SOURCES.md` #10 |

**In this repo:**
- **Catalogue:** `GRID_INDIA_PSP_DAILY` points at the old `report.grid-india.in` path (marked current path `PENDING_CONFIRMATION`). `GRID_INDIA_SCADA`, `GRID_INDIA_EXCHANGE` and `GRID_INDIA_GENERATION` are all `PENDING_CONFIRMATION`.
- **Code and data:** no Grid-India code exists, and all 9 `grid_*` tables have 0 rows.
- **Reference data:** `region` (5) and `state` (36) were seeded by the CEA pipeline. `country` and `interconnector` are empty.

## 3. Source and access (verified today)

| Item | Finding |
|---|---|
| API | `POST https://webapi.grid-india.in/api/v1/file` returns HTTP 200 in about 0.5 s with no authentication ✅ |
| Listing fields | `Title_` (e.g. `12.11.26_NLDC_PSP`), `FilePath`, `MimeType`, `SizeInKb`, `CreatedOn` / `ModifiedOn` (`DD-MM-YYYY HH:MM` IST), `Field2` (upload date) ✅ |
| History per type | `POST .../api/v1/file/get-period` `{"_source":"GRDW","_fileType":T}` lists the financial years ✅ |
| Page → file type | `POST .../api/v1/page` `{"_source":"GRDW","_relPath":"/en/reports/daily-psp-report"}` returns `FileType` ✅ |
| Files | `GET https://webcdn.grid-india.in/<FilePath>`: HTTP 200, no `Last-Modified` header ✅ |
| Website | `https://grid-india.in` (a JavaScript app that calls the same API) ✅ |
| Terms of use | not reviewed ❓ |

**Report types relevant here** (resolved from the site's page metadata):

| Page | `FileType` | Format | Financial years | Use |
|---|---|---|---|---|
| Daily PSP Report | `DAILY_PSP_REPORT` | XLS + PDF (PDF only before 2023) | 2010-11 → 2026-27 (15) | **primary** |
| Daily VRE Report | `DAILY_VRE_REPORT` (`*_NLDC_REMC_REPORT.pdf`) | PDF only | 2024-25 → 2026-27 | optional (RE detail); D+1 ~13:00 |
| Daily Frequency Profile | `DAILY_FREQUENCY_PROFILE` | not sampled | 2014-15 → | not needed (frequency is in `TimeSeries` / §B) |
| Weekly PSP Report | `REPORTS_WEEKLY_REPORT` | PDF | 2012-13 → | not needed (summary of daily) |
| Monthly PSP Report | `REPORTS_MONTHLY_REPORT` | PDF (e.g. `Monthly_Report_Sep_2026`, published 2026-10-23) | 2012-13 → | not needed |
| Tripping Report | `F_PG00443` | PDF | 2025-26 only: **1 file** (an incident report) | not a dataset |
| Week-ahead demand forecast | `DEMAND_FORECAST` | PDF | — | optional, later |
| Monthly ATC inter-regional | `ATC_INTER_REGIONAL` | not sampled | — | out of scope |

**Daily PSP files and versions** ✅:
- **Files per report:** one XLS and one PDF per report day. File names carry a random suffix (e.g. `12.11.26_NLDC_PSP_247.xls`), so the name cannot be predicted and the listing must be read first.
- **Revisions:** a revision is an additional file on the same day. For example, `15.10.26_NLDC_PSP_269.xls` was uploaded 16-10 10:47 and `15.10.26_NLDC_PSP_269_611.xls` at 16-10 16:38.
- **File type:** `MimeType` is inconsistent (`application/vnd.ms-excel` or `xls`), so filter on the file extension.
- **Paths:** older files live under `files/grdw/uploads/daily-reports/psp-reports/2024-2025/15.10.24_NLDC_PSP.xls`.
- **Coverage:** every sampled month (2022-12, 2023-01, 2023-06, 2024-10, 2024-11, 2024-12, 2025-06, 2026-10, 2026-11) has a report for every day.
- **Today's example:** the report for 2026-11-12 was uploaded on 2026-11-13 at 10:40 IST.

## 4. Dataset field inventory (Daily PSP workbook)

**Workbook sheets by date** ✅:

| Report date | Sheets |
|---|---|
| 2023-01-15 | `MOP_E`, `IR-Line` |
| 2023-07-06 | + `CrossBorder` (first report date with the sheet; categories STOA / (ISGS/LTA/MTOA) PPA until 2023-09-30, GNA / T-GNA from 2023-10-01) |
| 2024-11-01 | `MOP_E`, `IR-Line`, `CrossBorder` (no `TimeSeries` yet) |
| 2024-11-04 | + `TimeSeries` (13 columns; first report date with the sheet, corrected by the TimeSeries audit) |
| 2026-11-12 | `MOP_E`, `MOP_E1`, `IR-Line`, `CrossBorder`, `TimeSeries` (16 columns, with storage) |

Section letters and labels in `MOP_E` are the same in 2023 and 2026, but row positions shift. The parser must therefore locate sections and rows by label, as the CEA parsers do.

**Common to all sections:**
- The report for day D has "Date of Reporting: D+1"; the operating day is D.
- Region columns are NR, WR, SR, ER, NER and TOTAL (or All India).
- Sign convention: import is positive, export negative.

### 4.1 §A Power supply position, all-India and regional → dataset 1

Values are given per region plus TOTAL.

| Row label | Unit | Example (2026-11-12, TOTAL) |
|---|---|---|
| Demand Met during Evening Peak hrs (at 20:00 hrs; from RLDCs) | MW | 201,247 |
| Shortage during Evening Peak (label misspelt "Shoratge") | MW | 0 |
| Energy Met | MU | 4,444.64 |
| Hydro Gen | MU | 286.382 |
| Wind Gen | MU | 150.35 (ER/NER "-") |
| Solar Gen* | MU | 627.75 |
| Energy Shortage | MU | 0.2 |
| Maximum Demand Met During the Day (from NLDC SCADA) | MW | 215,199 |
| Time of Maximum Demand Met | HH:MM | 09:28 |

**Related sections:**
- **§B frequency profile** (All India): FVI and % of time in each band (< 49.7, 49.7–49.8, 49.8–49.9, < 49.9, 49.9–50.05, > 50.05). There is no destination table.
- **§H / §I:** demand diversity factors, and solar-hour / non-solar-hour all-India peak with time and shortage. There is no destination table.

### 4.2 §C / §J State-wise demand and availability → dataset 3

One row per state or entity, with its region.

| Column | Unit |
|---|---|
| Max. Demand Met during the day | MW |
| Shortage during Maximum Demand | MW |
| Energy Met | MU |
| Drawal Schedule | MU |
| OD(+)/UD(−) | MU |
| Max OD(+)/UD(−) | MW, published as text "631/ -686" (two values) |
| Energy Shortage | MU |

**Entity names in the report:**

| Kind | Names |
|---|---|
| States as abbreviated | Punjab, Haryana, Rajasthan, Delhi, UP, Uttarakhand, HP, Chandigarh, Chhattisgarh, Gujarat, MP, Maharashtra, Goa, Andhra Pradesh, Telangana, Karnataka, Kerala, Tamil Nadu, Puducherry, Bihar, Jharkhand, Odisha, West Bengal, Sikkim, Arunachal Pradesh, Assam, Manipur, Meghalaya, Mizoram, Nagaland, Tripura |
| Not a state, or combined | `J&K(UT) & Ladakh(UT)`, `DVC`, `DNHDDPDCL` (the DNH-DD discom ≈ state DH ⚠️) |
| Bulk consumers | `Bulk Consumer_<region> ISTS` |
| §J (`MOP_E1`) entities | Railways_NR/ER ISTS, Hindustan Zinc, AMNSIL, BALCO, RIL Jamnagar, Welspun, … |

Lakshadweep and Andaman & Nicobar are not in the report.

### 4.3 §G Source-wise generation (gross) → dataset 2 (daily)

- **Fuels**, in MU per region and All India, with % share: Coal, Lignite, Hydro, Nuclear, "Gas, Naptha & Diesel", "RES (Wind, Solar, Biomass & Others)", Total.
- **Derived shares:** RES share and non-fossil share. These are derived and not needed.
- **Note in `MOP_E1`:** "All generation MU figures are gross".

### 4.4 §F Generation outage → dataset 4 (aggregate only)

- **Content:** outage MW for Central Sector, State Sector and Total per region, plus % share. In 2026-11-12, for example, the All-India total is 59,655.14 MW.
- **Not in the report:** the plant, the unit, the reason and the outage type.
- **What exists at unit level:** no unit-level outage file is published by NLDC. The Tripping Report holds one incident PDF, and the RLDC sites (NR, WR, SR, ER reachable, NER timed out) were not investigated ❓.
- **Where unit outages come from:** CEA DGR sub-report 10 (`cea_dgr_unit_outage`, already loaded).

### 4.5 Exchanges → dataset 5

| Section / sheet | Content | Unit |
|---|---|---|
| §E Import/Export by Regions | Schedule, Actual, O/D/U/D per region and total | MU |
| `IR-Line` | Region-pair blocks ("Import/Export of ER (With NR)", ER–WR, ER–SR, ER–NER, NER–NR, WR–NR, WR–SR), each line row: Voltage Level, Line Details, No. of Circuit, Max Import (MW), Max Export (MW), Import (MU), Export (MU), NET (MU); a subtotal row per pair (e.g. `ER-NR 3.85 81.5 -77.65`) | MW / MU |
| `IR-Line` "International exchanges" | Country (Bhutan, Nepal, Bangladesh), Region, Line Name, Max / Min / Avg (MW), Energy Exchange (MU) | MW / MU |
| §D Transnational Exchanges | Bhutan, Nepal, Bangladesh, Godda→Bangladesh: Actual (MU), Day Peak (MW) | MU / MW |
| `CrossBorder` | Export / Import / Net by country (Bhutan, Nepal, Bangladesh, Myanmar) × contract: GNA (ISGS/PPA), T-GNA Bilateral, Collective IDAM (IEX / PXIL / HPX), RTM (IEX / PXIL / HPX), Total | MU (schedules) |

**Direction conventions:**
- In `IR-Line`, the NET sign is relative to the first-named region (import positive).
- In the international section, import is positive.
- The sign convention must be stored with each row.

### 4.6 `TimeSeries` → datasets 6 and 2 (15-minute)

**Layout:** 96 rows, "TIME" 0:00 … 23:45, labelled "15 Min (INSTANTANEOUS) ALL INDIA". These are instantaneous values at each 15-minute mark, **not** 15-minute averages ⚠️.

| Column (2026 layout) | Unit |
|---|---|
| FREQUENCY | Hz |
| DEMAND MET¹ (A): demand on the transmission system, excluding captive and rooftop, including storage load | MW |
| STORAGE DEMAND (PSP & BESS) (B) | MW |
| NUCLEAR (C), WIND (D), SOLAR (E), HYDRO² (F), GAS (G), THERMAL (H), STORAGE (PSP & BESS) (I), OTHERS³ (J) | MW |
| NET DEMAND MET (K) = A − D − E | MW |
| TOTAL GENERATION (L) = Σ(C:J) | MW |
| NET TRANSNATIONAL EXCHANGE (M), import positive | MW |

**Layout change:** the November 2024 layout had 13 columns and no storage columns. The parser must map columns by header text.

**Limitations:**
- All-India only; no regional series.
- The PDF and XLS disagree on derived columns 📄, so use the XLS.

### 4.7 What "15-minute SCADA" means here

- **Published:** the only 15-minute series published by NLDC is the all-India `TimeSeries` above ✅.
- **Not found:** regional or state 15-minute SCADA was not found on grid-india.in. The RLDC websites and WBES (`newwbes.grid-india.in`, HTTP 200; schedules, not SCADA) were not assessed ❓.
- **Effect on the table:** `grid_15min_scada` has `region_code`, so all-India rows would have it NULL.

## 5. PostgreSQL mapping

**Shared rules:**
- **Lineage:** none of the `grid_*` tables has `document_id`, only `ingestion_run_id`. Adding `document_id` is recommended for parity with the CEA tables.
- **Uniqueness:** no `grid_*` table has a unique constraint, so natural keys would be enforced by the loader (as for IEX and NPP).
- **Values:** numbers are stored as published, quantized to the column scale.
- **Dates:** `report_date` is the operating day D, not the reporting date.

| Dataset → table | Fits today? | Source → column | Missing (migration) | Entity mapping |
|---|---|---|---|---|
| §A → `grid_daily_power_supply` | **partly**: only `energy_met_mu` | Energy Met → `energy_met_mu` (MU, numeric 18,4); region column → `region_code`; TOTAL → `region_code NULL` (All India, null-safe key as NPP) | `evening_peak_demand_met_mw`, `evening_peak_shortage_mw`, `energy_shortage_mu`, `max_demand_met_mw`, `max_demand_met_time` (time), `hydro_gen_mu`, `wind_gen_mu`, `solar_gen_mu`, `document_id` | NR/WR/SR/ER/NER → existing `region` |
| §C/§J → `grid_state_power_supply` | **no**: `state_code NOT NULL` FK excludes DVC, `J&K(UT) & Ladakh(UT)`, bulk consumers and §J entities | Max demand → `max_demand_met_mw`; Energy Met → `energy_met_mu` | `area_code`, `area_name`, `area_type` (as in `cea_power_supply_position`), `region_code`, **`state_code` made nullable**, `shortage_at_max_demand_mw`, `drawal_schedule_mu`, `od_ud_mu`, `max_od_mw`, `max_ud_mw` (split "631/ -686"), `energy_shortage_mu`, `document_id` | Alias table: UP→UP, HP→HP, MP→MP, Odisha→OD, DNHDDPDCL→DH ⚠️, J&K+Ladakh→`JK_LA` (system), DVC, `BULK_<region>`, §J entities by name |
| §G → `grid_sourcewise_generation` | **yes** for regions | region × fuel → (`region_code`, `fuel_type` verbatim e.g. "Gas, Naptha & Diesel", `gross_generation_mu`) | All-India row needs `region_code` nullable or is skipped (Σ regions, validated against the published total). Optional `document_id` | fuel labels stored verbatim, no assumption that "RES" = wind + solar |
| §F → `grid_generation_outage` | **partly**: no sector column; plant/unit not available | region × sector → `region_code`, `outage_capacity_mw` | `sector` (CENTRAL / STATE); total row → region NULL or skipped. Optional `document_id` | — |
| §E → `grid_regional_exchange` | **yes** | Schedule → `scheduled_exchange_mu`; Actual → `actual_exchange_mu`; `sign_convention` = 'import +ve'; O/D/U/D = actual − schedule (derived, not stored) | TOTAL row (region NOT NULL) skipped or column made nullable | regions |
| `IR-Line` → `grid_interregional_line_exchange` | **partly** | block "ER (With NR)" → `from_region_code` ER, `to_region_code` NR; Line Details → `line_name`; NET → `net_exchange_mu`; `direction_convention` = 'import to from_region +ve' | `voltage_level`, `circuits`, `max_import_mw`, `max_export_mw`, `import_mu`, `export_mu`, `document_id`; region-pair subtotal rows validated, not stored | line names verbatim (no line master) |
| `IR-Line` international → `grid_international_line_exchange` | **partly** | country → `country_code`; Line Name → `line_name`; Energy Exchange → `energy_exchange_mu`; `sign_convention` 'import +ve' | `region_code`, `max_mw`, `min_mw`, `avg_mw`, `document_id`; `interconnector_id` left NULL (no master) | **seed `country`** (BT, NP, BD, MM): reference data, not schema |
| §D → `grid_international_line_exchange` (country level) or skip | ⚠️ | Actual MU / Day Peak MW per country | would need a line marker ("ALL") or a separate table: **recommend skip** (Σ of lines) | country |
| `CrossBorder` → `grid_crossborder_market_schedule` | **yes** | country → `country_code`; contract (GNA, T-GNA BILATERAL, IDAM, RTM) → `contract_type`; IEX/PXIL/HPX → `power_exchange`; MU → `scheduled_energy_mu`; Export / Import → `direction` | Optional `document_id`; Net and Total rows are derived (validate, don't store) | country seed |
| `TimeSeries` → `grid_15min_scada` | **partly**: frequency, demand, solar, wind only | TIME + D → `interval_start` (IST → timestamptz; instantaneous at that mark); FREQUENCY → `grid_frequency_hz`; DEMAND MET → `demand_met_mw`; SOLAR / WIND → `solar_/wind_generation_mw`; `region_code NULL` = All India | `nuclear_mw`, `hydro_mw`, `gas_mw`, `thermal_mw`, `storage_generation_mw`, `storage_demand_mw`, `others_mw`, `net_demand_met_mw`, `total_generation_mw`, `net_transnational_exchange_mw`, `document_id` | — |

**Not mapped:** §B frequency profile and §H/§I diversity and peak. No table exists, and they aren't needed for the listed datasets ⚠️.

**Migration needed:** **yes**, one additive Alembic migration (`0002_grid_india`).
- Add nullable columns as listed.
- Make `grid_state_power_supply.state_code` nullable.
- Optionally make `region_code` nullable in `grid_sourcewise_generation`, `grid_regional_exchange` and `grid_generation_outage` for All-India rows.
- All tables are empty, so no data is affected.

`country` rows would be seeded by a reference command, like CEA's.

## 6. Implementation plan (after approval)

The structure follows CEA: `src/pipelines/grid_india/` (`api`, `parse_psp`, `pipeline`, `runner`, `reference`), `scripts/ingest_grid_india.py`, and the same shared framework.

1. **Fetch.** For each financial year and month in the requested range:
   - POST the listing for `DAILY_PSP_REPORT` and select the `.xls` files whose `Title_` date is in range, in `CreatedOn` order.
   - GET each file from `webcdn`. Before 2023 there is PDF only; see step 10.
   - Store the listing JSON as an audit document, as with the CEA monthly listings.
   - Pause 0.5 s between requests.
2. **Raw storage.** Write-once files under `raw_data/grid_india/grid_india_psp_daily/<xls|listing>/…`, with SHA-256 recorded in `source_document`. Document name = `FilePath` basename, which is unique per version. `published_at` comes from `CreatedOn` (IST).
3. **One workbook, several tables.** The framework loads one table per pipeline, so there is one pipeline class per destination table, each parsing its own sections of the same file:
   - daily supply (§A);
   - states (§C/§J);
   - fuel mix (§G);
   - outage (§F);
   - regional exchange (§E);
   - IR lines and international lines (`IR-Line`);
   - cross-border schedule (`CrossBorder`);
   - 15-minute (`TimeSeries`).

   A runner fetches the files once and hands the same raw documents to each pipeline. ⚠️ This needs either a shared-fetch hook or running each pipeline on stored files (the CEA `--reprocess` path works for this).
4. **Parsing.** Use `xlrd`; locate sections by letter and label, and columns by header text. Then:
   - "-" → NULL;
   - "631/ -686" → two values;
   - `HH:MM` → time;
   - the report date is read from "Date of Reporting" − 1 day and checked against `Title_`.
5. **Validation:**
   - regions sum to TOTAL (§A, §E, §F, §G);
   - IR-Line lines sum to the pair subtotal;
   - `TimeSeries` has exactly 96 rows, TOTAL GENERATION = Σ(C:J), and NET DEMAND = A − D − E;
   - frequency is within 49–51 Hz;
   - values are non-negative where they must be;
   - names are known. Unknown → `UNMAPPED_ENTITY`, rejected and recorded, never dropped.
6. **Entity mapping:**
   - regions use the existing table;
   - states use an alias table extending `src/pipelines/cea/geography.py` (read-only reuse) with "UP", "HP", "MP", "J&K(UT) & Ladakh(UT)", "DNHDDPDCL" ⚠️, "DVC" and "Bulk Consumer_*";
   - countries are seeded.
7. **Duplicates and revisions:**
   - **Document level:** the checksum skips identical files.
   - **Record level:** natural keys with the VERSION policy.
     - Daily tables: (`region_code`/`area_code`, `report_date`[, `fuel_type` / `sector`]).
     - IR lines: (`from_region`, `to_region`, `line_name`, `report_date`).
     - 15-minute: (`region_code`, `interval_start`).
   - **Revisions:** a revised file uploaded the same afternoon becomes a new version, and the earlier rows are kept.
8. **Audit:** one `ingestion_run` per command, `data_quality_issue` rows plus a rejects file, and SUCCESS / PARTIAL / FAILED status.
9. **Failures:**
   - HTTP errors, HTML instead of a file, a missing sheet or label, or a wrong date → the document fails.
   - A report day missing from the listing → `MISSING_DAY` (INFO for D+1, WARNING for older days).
10. **Backfill (separate approval):**
    - XLS 2023-01 → present: about 1,400 files, in 1-month chunks.
    - `TimeSeries` exists only from 2024-11.
    - PDFs 2010–2022 are a separate decision. They need poppler `pdftotext` 📄, and parsing is less reliable.

## 7. Technical risks and unresolved questions

| # | Item | Status |
|---|---|---|
| 1 | Grid-India website terms of use not reviewed | ❓ |
| 2 | Publication time before 2025-05-12 is unknown (bulk re-uploads) 📄. For leak-free forecasting, history needs a rule (the forecasting repo uses D+2 09:00) | ⚠️ decision |
| 3 | Unit-level generator outages are not published by NLDC; RLDC sites not assessed | ❓ |
| 4 | Regional or state 15-minute SCADA not found publicly; RLDC sites and WBES not assessed | ❓ |
| 5 | `TimeSeries` values are instantaneous at each 15-minute mark, not interval averages (from the sheet title) | ⚠️ |
| 6 | `DNHDDPDCL` (a discom) mapped to state DH; "J&K(UT) & Ladakh(UT)" is combined | ⚠️ |
| 7 | Layouts change: `TimeSeries` gained storage columns, `MOP_E1` and `CrossBorder` were added, row positions shift. Parse by label and fail loudly on unknown headers | risk |
| 8 | API access from other hosts: the cloud box was geo-blocked 📄; works from this machine ✅ | risk if run elsewhere |
| 9 | File names are not predictable, so the listing must be read first; the API is undocumented | risk |
| 10 | The PDF era (2010–2022) needs a different parser and has known extraction problems 📄 | risk |
| 11 | One workbook feeds about 9 tables: the framework is one-table-per-pipeline, so a shared fetch or a reprocess approach is needed | design |
| 12 | The weekly and monthly PSP reports and the VRE report are PDF only | not proposed |

## 8. Recommended implementation order

1. **Migration `0002_grid_india`**, then seed the country reference.
2. **Daily PSP §A and §C/§J** (supply and states): the main demand and availability signal. XLS from 2023.
3. **§G fuel mix, §F outage aggregate and §E regional exchange:** same file, simple tables.
4. **`TimeSeries`** 15-minute all-India (from 2024-11).
5. **`IR-Line`, international lines and `CrossBorder`.**
6. **Later, if wanted:** the PDF backfill for 2010–2022, the week-ahead demand forecast, and an RLDC investigation for unit outages or regional SCADA.

**Decisions needed before implementation:**
- **G1:** approve the migration, including making `state_code` nullable plus `area_code`, and making `region_code` nullable for All-India rows.
- **G2:** the shared-fetch design for one workbook → many tables.
- **G3:** the publication-time rule for pre-2025-05 history.
- **G4:** whether PDF-era data is in scope.
- **G5:** confirm the terms of use.
