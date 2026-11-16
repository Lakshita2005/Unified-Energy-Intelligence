# Dataset to PostgreSQL table mapping

Generated from `src/ingestion/catalog.py` by `python -m scripts.register_sources --write-mapping`. Do not edit by hand; change the catalogue and regenerate.

Values marked `PENDING_CONFIRMATION` have not been verified against the publisher and must be confirmed before a pipeline relies on them. All tables are in the `pi` schema.

## Sources

| Code | Name | URL |
|---|---|---|
| `CEA` | Central Electricity Authority | https://cea.nic.in/ |
| `NPP` | National Power Portal | https://npp.gov.in/ |
| `GRID_INDIA` | Grid-India | https://grid-india.in/ |
| `OPEN_METEO` | Open-Meteo | https://open-meteo.com/ |
| `IEX` | Indian Energy Exchange | https://www.iexindia.com/ |
| `RPC` | Regional Power Committees (NRPC, WRPC, SRPC, ERPC, NERPC) | — (no single site) |
| `WBES` | Grid-India WBES (Web Based Energy Scheduling) | https://newwbes.grid-india.in/ |
| `IMD` | India Meteorological Department | https://mausam.imd.gov.in/ |

## Registered datasets

| Dataset code | Source | Destination table(s) | Granularity | Collection | Timezone |
|---|---|---|---|---|---|
| `CEA_COAL_STOCK_DAILY` | `CEA` | `cea_coal_stock_daily` | daily | daily | Asia/Kolkata |
| `CEA_COAL_STOCK_MONTHLY` | `CEA` | `cea_coal_stock_monthly` | monthly | monthly | Asia/Kolkata |
| `CEA_DGR_DAILY` | `CEA` | `cea_dgr_plant_daily`<br>`cea_dgr_unit_outage` | daily | daily | Asia/Kolkata |
| `CEA_PSP_ENERGY_MONTHLY` | `CEA` | `cea_power_supply_position` | monthly | monthly | Asia/Kolkata |
| `CEA_PSP_PEAK_MONTHLY` | `CEA` | `cea_power_supply_position` | monthly | monthly | Asia/Kolkata |
| `NPP_DEMAND` | `NPP` | `npp_demand_generation` | irregular ~2-4 min snapshots | intraday, continuous | Asia/Kolkata |
| `NPP_GENERATION` | `NPP` | `npp_demand_generation` | irregular ~2-4 min snapshots | intraday, continuous | Asia/Kolkata |
| `GRID_INDIA_PSP_DAILY` | `GRID_INDIA` | `grid_daily_power_supply` | daily | daily | Asia/Kolkata |
| `GRID_INDIA_PSP_STATES` | `GRID_INDIA` | `grid_state_power_supply` | daily | daily | Asia/Kolkata |
| `GRID_INDIA_SCADA` | `GRID_INDIA` | `grid_15min_scada` | 15min | daily | Asia/Kolkata |
| `GRID_INDIA_EXCHANGE` | `GRID_INDIA` | `grid_regional_exchange` | daily | daily | Asia/Kolkata |
| `GRID_INDIA_GENERATION` | `GRID_INDIA` | `grid_sourcewise_generation` | daily | daily | Asia/Kolkata |
| `GRID_INDIA_PSP_IR_LINES` | `GRID_INDIA` | `grid_interregional_line_exchange` | daily | daily | Asia/Kolkata |
| `GRID_INDIA_PSP_INTL_LINES` | `GRID_INDIA` | `grid_international_line_exchange` | daily | daily | Asia/Kolkata |
| `GRID_INDIA_PSP_CROSSBORDER` | `GRID_INDIA` | `grid_crossborder_market_schedule` | daily | daily | Asia/Kolkata |
| `GRID_INDIA_PSP_OUTAGE` | `GRID_INDIA` | `grid_generation_outage` | daily | daily | Asia/Kolkata |
| `IEX_DAM` | `IEX` | `iex_market_price` | 15min | daily | Asia/Kolkata |
| `IEX_GDAM` | `IEX` | `iex_market_price` | 15min | daily | Asia/Kolkata |
| `IEX_RTM` | `IEX` | `iex_market_price` | 15min | intraday (half-hourly sessions) | Asia/Kolkata |
| `WBES_PX_REGION` | `WBES` | `wbes_px_region_schedule` | 15min | intraday | Asia/Kolkata |
| `WBES_PX_LINK` | `WBES` | `wbes_px_link_schedule` | 15min | intraday | Asia/Kolkata |
| `WBES_PX_ENTITY` | `WBES` | `wbes_px_entity_schedule` | 15min | intraday | Asia/Kolkata |
| `OPEN_METEO_FORECAST` | `OPEN_METEO` | `open_meteo_weather` | hourly | daily | UTC |
| `OPEN_METEO_HISTORICAL` | `OPEN_METEO` | `open_meteo_weather` | hourly | daily | UTC |
| `DSM_SCHEDULE_INTERSTATE` | `GRID_INDIA` | `dsm_plant_schedule_actual` | 15min | PENDING_CONFIRMATION | Asia/Kolkata |
| `DSM_SETTLEMENT_REGIONAL` | `RPC` | `dsm_settlement` | weekly | weekly | Asia/Kolkata |
| `IMD_AWS_OBS` | `IMD` | `imd_station`<br>`imd_station_observation` | station observation | intraday | Asia/Kolkata |
| `IMD_CITY_FORECAST` | `IMD` | `imd_city_forecast` | daily | daily | Asia/Kolkata |

### Dataset details

#### `CEA_COAL_STOCK_DAILY` — CEA Daily Coal Stock Report (thermal power stations)

- **Source:** `CEA`
- **Endpoint / report:** CEA Fuel Management Division Daily Coal Stock Report (XLS, published on NPP): https://npp.gov.in/public-reports/cea/daily/fuel/{DD-MM-YYYY}/dailyCoal1-{YYYY-MM-DD}.xls (also .pdf). Report 'AS ON' D is published about D+1 15:30 IST; available from 2024-06.
- **Destination:** `cea_coal_stock_daily`
- **Notes:** One row per plant per report date, stock in '000 tonnes as published. Plant names are mapped via source_entity_mapping. stock_days is left NULL (not published; see CEA_DGR_DAILY sub-report 2).

#### `CEA_COAL_STOCK_MONTHLY` — CEA Monthly Coal Statement of Thermal Power Stations

- **Source:** `CEA`
- **Endpoint / report:** CEA Monthly Coal Statement (PDF, published on NPP): https://npp.gov.in/public-reports/cea/monthly/fuel/{YYYY}/{MON}/monthCoal1-{YYYY}-{Mon}.pdf
- **Destination:** `cea_coal_stock_monthly`
- **Notes:** Older reports (2018-2023) are on cea.nic.in under wp-content/uploads/coal/.

#### `CEA_DGR_DAILY` — CEA Daily Generation Report (DGR)

- **Source:** `CEA`
- **Endpoint / report:** CEA OPM Division Daily Generation Report (XLS on NPP, also .pdf): https://npp.gov.in/public-reports/cea/daily/dgr/{DD-MM-YYYY}/dgr{N}-{YYYY-MM-DD}.xls. Sub-report 2 (region/state/sector/type/station/unit-wise generation, coal stock days, capacity under outage) -> cea_dgr_plant_daily; sub-report 10 (daily maintenance report, coal/lignite/nuclear units: planned/forced/other MW, outage time, expected/sync time, reason) -> cea_dgr_unit_outage. Report D is published about D+1 17:00 IST; available from 2018-04.
- **Destination:** `cea_dgr_plant_daily`, `cea_dgr_unit_outage`
- **Notes:** Stations of 25 MW and above. Report footer: 'for operational purpose only and not for commercial purpose'. Gas/hydro unit outages are not in sub-report 10.

#### `CEA_PSP_ENERGY_MONTHLY` — CEA monthly Energy Power Supply Position (state-wise)

- **Source:** `CEA`
- **Endpoint / report:** CEA 'Power Supply Position - Energy' monthly PDF, listed at https://cea.nic.in/reports/dashboardView?headingId=107&subHeadingId=1003&subSubHeadingId=10010&lang=en; file: https://cea.nic.in/api/dashboardReports/download-pdf/en/{uuid}
- **Destination:** `cea_power_supply_position`
- **Notes:** Energy requirement / supplied / not supplied (MU net) for the month and April-to-month, per state/system, region and All India. Published about 2.5 months after the month; revised reports replace earlier ones. Supported from 2020-04 (current layout); older listing entries are an older layout or mislabelled generation reports and are rejected. File UUIDs change between page loads.

#### `CEA_PSP_PEAK_MONTHLY` — CEA monthly Peak Power Supply Position (state-wise)

- **Source:** `CEA`
- **Endpoint / report:** CEA 'Power Supply Position - Peak' monthly PDF, listed at https://cea.nic.in/reports/dashboardView?headingId=107&subHeadingId=1003&subSubHeadingId=10011&lang=en; file: https://cea.nic.in/api/dashboardReports/download-pdf/en/{uuid}
- **Destination:** `cea_power_supply_position`
- **Notes:** Peak demand / peak met / demand not met (MW net) for the month and April-to-month. Same layout and timing as CEA_PSP_ENERGY_MONTHLY.

#### `NPP_DEMAND` — NPP All-India real-time demand met (source: MERIT India)

- **Source:** `NPP`
- **Endpoint / report:** Undocumented NPP dashboard JSON: https://npp.gov.in/dashBoard/demandmet1chartdata?date={YYYY-MM-DD} (IST day). Fields updated_on (epoch ms), name_of_data ('DEMAND MET'), value_of_data (MW). History from about 2025-10.
- **Destination:** `npp_demand_generation`
- **Notes:** All-India only: state_code and region_code are NULL. Snapshot times are stored as published (not rounded to 15-minute blocks).

#### `NPP_GENERATION` — NPP All-India real-time generation by source (source: MERIT India)

- **Source:** `NPP`
- **Endpoint / report:** Undocumented NPP dashboard JSON: https://npp.gov.in/dashBoard/demandmet2chartdata?date={YYYY-MM-DD} (IST day). Same fields as demandmet1chartdata; series THERMAL, HYDRO, GAS, NUCLEAR, SOLAR and WIND GENERATION (MW). History from about 2025-10.
- **Destination:** `npp_demand_generation`
- **Notes:** All-India only (NULL state_code/region_code). Series names are stored verbatim; the composition of THERMAL GENERATION (coal/lignite only or more) is not documented and must not be assumed.

#### `GRID_INDIA_PSP_DAILY` — Grid-India (NLDC) Daily PSP report - Section A (all-India/regional)

- **Source:** `GRID_INDIA`
- **Endpoint / report:** NLDC Daily PSP workbook (XLS from 2023-01; PDF before). Listing: POST https://webapi.grid-india.in/api/v1/file {_source: GRDW, _type: DAILY_PSP_REPORT, _fileDate: <FY e.g. 2026-27>, _month: MM}; file: https://webcdn.grid-india.in/<FilePath>. Section A, sheet MOP_E: evening-peak demand met and shortage, energy met, hydro/wind/solar generation, energy shortage, maximum demand met and its time per region and All India.
- **Destination:** `grid_daily_power_supply`
- **Notes:** Report for operating day D is uploaded on D+1 (~10:00-11:00 IST); revisions are extra files. All India = region_code NULL. Availability: actual upload time (CreatedOn) for report dates from 2025-05-12, else estimated D+2 09:00 IST.

#### `GRID_INDIA_PSP_STATES` — Grid-India (NLDC) Daily PSP report - Sections C and J (states)

- **Source:** `GRID_INDIA`
- **Endpoint / report:** Same NLDC Daily PSP workbook as GRID_INDIA_PSP_DAILY (listing POST https://webapi.grid-india.in/api/v1/file, file https://webcdn.grid-india.in/<FilePath>). Section C (sheet MOP_E, 'Power Supply Position in States') and Section J (sheet MOP_E1, ISTS bulk consumers, from 2026): maximum demand met, shortage at maximum demand, energy met, drawal schedule, over/under-drawal (MU), maximum over/under-drawal (MW), energy shortage.
- **Destination:** `grid_state_power_supply`
- **Notes:** One row per state, system (DVC, J&K+Ladakh), regional bulk consumer aggregate and Section J entity. DNHDDPDCL is stored as state DH with its published name (utility demand, excluding separately reported bulk consumers).

#### `GRID_INDIA_SCADA` — Grid-India (NLDC) Daily PSP report - TimeSeries (all-India 15-minute SCADA)

- **Source:** `GRID_INDIA`
- **Endpoint / report:** Same NLDC Daily PSP workbook as GRID_INDIA_PSP_DAILY (listing POST https://webapi.grid-india.in/api/v1/file, file https://webcdn.grid-india.in/<FilePath>). Sheet 'TimeSeries', '15 Min (INSTANTANEOUS) ALL INDIA GRID FREQUENCY, GENERATION & DEMAND MET (SCADA DATA)' (from report date 2024-11-04): frequency, demand met, generation by source, storage (from 2026-05/06), net demand met, total generation and net transnational exchange (import +ve) at each 15-minute mark 0:00-23:45.
- **Destination:** `grid_15min_scada`
- **Notes:** Instantaneous values at each mark (not averages; column comment, migration 0006). All-India only (region_code NULL). Storage generation from 2026-05-06, storage demand from 2026-06-01 (NULL before). Values stored as published, including telemetry-loss zeros (flagged).

#### `GRID_INDIA_EXCHANGE` — Grid-India (NLDC) Daily PSP report - Section E (regional exchange)

- **Source:** `GRID_INDIA`
- **Endpoint / report:** Same NLDC Daily PSP workbook as GRID_INDIA_PSP_DAILY (listing POST https://webapi.grid-india.in/api/v1/file, file https://webcdn.grid-india.in/<FilePath>). Section E, sheet MOP_E, 'Import/Export by Regions (in MU) - Import(+ve)/Export(-ve); OD(+)/UD(-)': Schedule, Actual and O/D/U/D per region (NR, WR, SR, ER, NER), with TOTAL.
- **Destination:** `grid_regional_exchange`
- **Notes:** Signed MU values as published (import +ve, export -ve; over-drawal +ve, under-drawal -ve). The published O/D/U/D is stored separately (od_ud_mu, migration 0003) because it is not always Actual - Schedule. exchange_direction is left NULL; the TOTAL column is additive and validated, not stored.

#### `GRID_INDIA_GENERATION` — Grid-India (NLDC) Daily PSP report - Section G (source-wise generation)

- **Source:** `GRID_INDIA`
- **Endpoint / report:** Same NLDC Daily PSP workbook as GRID_INDIA_PSP_DAILY (listing POST https://webapi.grid-india.in/api/v1/file, file https://webcdn.grid-india.in/<FilePath>). Section G, sheet MOP_E, 'Sourcewise generation (Gross) (MU)': Coal, Lignite, Hydro, Nuclear, 'Gas, Naptha & Diesel', 'RES (Wind, Solar, Biomass & Others)' per region (NR, WR, SR, ER, NER), with All India and Total for validation.
- **Destination:** `grid_sourcewise_generation`
- **Notes:** Gross generation in MU. One row per region x fuel; the All-India column and the Total row are additive and are validated, not stored. fuel_type is a normalised code, fuel_label the published label.

#### `GRID_INDIA_PSP_IR_LINES` — Grid-India (NLDC) Daily PSP report - IR-Line (inter-regional lines)

- **Source:** `GRID_INDIA`
- **Endpoint / report:** Same NLDC Daily PSP workbook as GRID_INDIA_PSP_DAILY (listing POST https://webapi.grid-india.in/api/v1/file, file https://webcdn.grid-india.in/<FilePath>). Sheet 'IR-Line', 'INTER-REGIONAL EXCHANGES': per line in blocks 'Import/Export of <region> (With <region>)': voltage level, circuits, max import/export (MW), import/export (MU), NET (MU).
- **Destination:** `grid_interregional_line_exchange`
- **Notes:** from_region_code = the first-named region of the block; NET import +ve relative to it (sheet title). Import/export are published as separate non-negative values. Block subtotal rows are validated, not stored (the ER-SR subtotal excludes TALCHER-I/C).

#### `GRID_INDIA_PSP_INTL_LINES` — Grid-India (NLDC) Daily PSP report - IR-Line (international lines)

- **Source:** `GRID_INDIA`
- **Endpoint / report:** Same NLDC Daily PSP workbook as GRID_INDIA_PSP_DAILY (listing POST https://webapi.grid-india.in/api/v1/file, file https://webcdn.grid-india.in/<FilePath>). Sheet 'IR-Line', 'INTERNATIONAL EXCHANGES' (Import(+ve)/Export(-ve)): per line: country, region, max/min/avg (MW), energy exchange (MU).
- **Destination:** `grid_international_line_exchange`
- **Notes:** Country from an exact line-name reference (ISO alpha-3 BTN/NPL/BGD); the published Region cell is kept in region_label (migration 0004). Totals per country are compared with Section D.

#### `GRID_INDIA_PSP_CROSSBORDER` — Grid-India (NLDC) Daily PSP report - CrossBorder (exchange schedule)

- **Source:** `GRID_INDIA`
- **Endpoint / report:** Same NLDC Daily PSP workbook as GRID_INDIA_PSP_DAILY (listing POST https://webapi.grid-india.in/api/v1/file, file https://webcdn.grid-india.in/<FilePath>). Sheet 'CrossBorder', 'CROSS BORDER EXCHANGE SCHEDULE' (from report date 2023-07-06): Export From India, Import by India and Net from India (-ve export / +ve import), in MU, per country (Bhutan, Nepal, Bangladesh, Myanmar) and contract: GNA (ISGS/PPA), T-GNA Bilateral, Collective IDAM and RTM per exchange (IEX, PXIL, HPX); before 2023-10-01 (ISGS/LTA/MTOA) PPA and STOA.
- **Destination:** `grid_crossborder_market_schedule`
- **Notes:** Scheduled energy. Net rows stored as direction NET (migration 0005); row TOTAL column and Total rows validated, not stored. Pre-2023-10 contract categories keep their own codes.

#### `GRID_INDIA_PSP_OUTAGE` — Grid-India (NLDC) Daily PSP report - Section F (generation outage)

- **Source:** `GRID_INDIA`
- **Endpoint / report:** Same NLDC Daily PSP workbook as GRID_INDIA_PSP_DAILY (listing POST https://webapi.grid-india.in/api/v1/file, file https://webcdn.grid-india.in/<FilePath>). Section F, sheet MOP_E, 'Generation Outage(MW)': Central Sector and State Sector outage capacity (MW) per region (NR, WR, SR, ER, NER), with TOTAL and Total.
- **Destination:** `grid_generation_outage`
- **Notes:** Aggregate outage MW by sector and region only (no plant, unit or reason). The TOTAL column and Total row are additive and validated, not stored. Unit-level outages come from CEA DGR (cea_dgr_unit_outage).

#### `IEX_DAM` — IEX Day-Ahead Market (DAM) prices

- **Source:** `IEX`
- **Endpoint / report:** IEX API: https://www.iexindia.com/api/v1/dam/market-snapshot?interval=ONE_FOURTH_HOUR&fromDate={DD-MM-YYYY}&toDate={DD-MM-YYYY} (MCP) and /api/v1/dam/area-price?...&bidArea={A1..W3} (bid-area prices). Page: https://www.iexindia.com/market-data/day-ahead-market/market-snapshot
- **Destination:** `iex_market_price`
- **Notes:** Time blocks 1-96 (block 1 = 00:00-00:15 IST). MCP stored as area MKT plus 13 bid areas. 15-minute data from at least 2015-01-01. Requires market_master/market_area reference rows.

#### `IEX_GDAM` — IEX Green Day-Ahead Market (G-DAM) prices

- **Source:** `IEX`
- **Endpoint / report:** IEX API: https://www.iexindia.com/api/v1/gdam/market-snapshot?interval=ONE_FOURTH_HOUR&fromDate={DD-MM-YYYY}&toDate={DD-MM-YYYY} (unconstrained MCP) and /api/v1/gdam/area-price?...&bidArea={A1..W3}. Page: https://www.iexindia.com/market-data/green-day-ahead-market/market-snapshot
- **Destination:** `iex_market_price`
- **Notes:** 15-minute data from 2021-10-27. Price field is unconstrained_m_c_p; the volume breakdown fields changed with delivery date 2026-07-08 (solar/non-solar/hydro -> hydro/wind/other RE/DRE).

#### `IEX_RTM` — IEX Real-Time Market (RTM) prices

- **Source:** `IEX`
- **Endpoint / report:** IEX API: https://www.iexindia.com/api/v1/rtm/market-snapshot?interval=ONE_FOURTH_HOUR&fromDate={DD-MM-YYYY}&toDate={DD-MM-YYYY}. Page: https://www.iexindia.com/market-data/real-time-market/market-snapshot
- **Destination:** `iex_market_price`
- **Notes:** 15-minute data from 2020-06-01. MCP only (area MKT): the rtm/area-price endpoint returned no rows for any bid area. The current day fills in as sessions clear.

#### `WBES_PX_REGION` — WBES PX Dashboard - region-wise injection / drawal

- **Source:** `WBES`
- **Endpoint / report:** WBES PX Dashboard (login-gated, undocumented JSON API at https://newwbes-api.grid-india.in): POST /powerexchange/api/PowerExchange/GetPowerExchangeByDetails per delivery date x power exchange (GetPxTypeList) x market (GetPxTransTypeList), RevisionNo -1 (all revisions). One response feeds WBES_PX_REGION, WBES_PX_LINK and WBES_PX_ENTITY.
- **Destination:** `wbes_px_region_schedule`
- **Notes:** Units and region-id mapping unverified (values stored as published; region_code NULL until verified). Revision number, PowerExId and CreatedOn are lineage; new versions only when values change.

#### `WBES_PX_LINK` — WBES PX Dashboard - link-wise net amount

- **Source:** `WBES`
- **Endpoint / report:** WBES PX Dashboard (login-gated, undocumented JSON API at https://newwbes-api.grid-india.in): POST /powerexchange/api/PowerExchange/GetPowerExchangeByDetails per delivery date x power exchange (GetPxTypeList) x market (GetPxTransTypeList), RevisionNo -1 (all revisions). One response feeds WBES_PX_REGION, WBES_PX_LINK and WBES_PX_ENTITY.
- **Destination:** `wbes_px_link_schedule`
- **Notes:** Units and NetAmount sign convention unverified; values stored as published.

#### `WBES_PX_ENTITY` — WBES PX Dashboard - entity-wise injection / drawal

- **Source:** `WBES`
- **Endpoint / report:** WBES PX Dashboard (login-gated, undocumented JSON API at https://newwbes-api.grid-india.in): POST /powerexchange/api/PowerExchange/GetPowerExchangeByDetails per delivery date x power exchange (GetPxTypeList) x market (GetPxTransTypeList), RevisionNo -1 (all revisions). One response feeds WBES_PX_REGION, WBES_PX_LINK and WBES_PX_ENTITY.
- **Destination:** `wbes_px_entity_schedule`
- **Notes:** Units and entity-key uniqueness unverified (no unique constraint; duplicate keys within a revision are rejected and reported).

#### `OPEN_METEO_FORECAST` — Open-Meteo weather forecast

- **Source:** `OPEN_METEO`
- **Endpoint / report:** Open-Meteo Forecast API: https://api.open-meteo.com/v1/forecast
- **Destination:** `open_meteo_weather`
- **Notes:** Loaded with is_forecast = true and forecast_issued_at set. API returns GMT unless a timezone parameter is sent. Locations come from weather_location.

#### `OPEN_METEO_HISTORICAL` — Open-Meteo historical weather (reanalysis)

- **Source:** `OPEN_METEO`
- **Endpoint / report:** Open-Meteo Historical Weather API: https://archive-api.open-meteo.com/v1/archive
- **Destination:** `open_meteo_weather`
- **Notes:** Loaded with is_forecast = false. ERA5 / ERA5-Land / ECMWF IFS reanalysis.

#### `DSM_SCHEDULE_INTERSTATE` — DSM inter-state schedules issued by RLDCs

- **Source:** `GRID_INDIA`
- **Endpoint / report:** PENDING_CONFIRMATION: publisher confirmed as the RLDCs (Grid-India) for inter-state scheduling; access method (e.g. WBES) and format not verified
- **Destination:** `dsm_plant_schedule_actual`
- **Notes:** Fills the schedule_* columns. 15-minute DSM time blocks (1-96). Intra-state schedules (SLDCs) are not covered.

#### `DSM_SETTLEMENT_REGIONAL` — Regional weekly Deviation Settlement Account statements

- **Source:** `RPC`
- **Endpoint / report:** Weekly DSM statements issued by each RPC Secretariat (CERC DSM Regulations 2024). Verified listing: ERPC https://erpc.gov.in/dsm-rras-fras-and-sced-accts; NRPC/WRPC/SRPC/NERPC locations PENDING_CONFIRMATION. PDF formats vary by RPC; revised statements are issued for past weeks
- **Destination:** `dsm_settlement`
- **Notes:** Settlement period start/end per statement. Revisions must be loaded as new versions, not overwrites.

#### `IMD_AWS_OBS` — IMD AWS/ARG station observations

- **Source:** `IMD`
- **Endpoint / report:** IMD API (https://api.imd.gov.in/api/v1): GET /aws_data (optionally ?id=<call sign> or ?sid=<state id>) for observations and GET /aws_data_mapping for stations. Access requires IMD approval and IP whitelisting.
- **Destination:** `imd_station`, `imd_station_observation`
- **Notes:** Response container, units, DATE/TIME time zone and identifier uniqueness are unverified; values are stored as published in neutral *_value columns. The mapping endpoint's fields are not documented.

#### `IMD_CITY_FORECAST` — IMD 7-day city weather forecast

- **Source:** `IMD`
- **Endpoint / report:** IMD API (https://api.imd.gov.in/api/v1): GET /cityforecast (optionally ?id=<station code>). Access requires IMD approval and IP whitelisting.
- **Destination:** `imd_city_forecast`
- **Notes:** One vintage per published Date (issue time not published). Only forecast fields are loaded; the observed and astronomical fields of the response are kept in raw storage.

## Pending datasets (not registered)

These are intentionally absent from `pi.source_dataset` until their publisher is confirmed.

| Proposed code | Dataset | Candidate publisher(s) | Destination table(s) | Why pending |
|---|---|---|---|---|
| `DSM_ACTUAL_METER` | DSM actual injection/drawal (SEM/ABT meter data) | GRID_INDIA (RLDC) for inter-state, or the relevant SLDC for intra-state | `dsm_plant_schedule_actual` | Publisher and access method not confirmed. Meter-based actuals must stay distinct from SCADA operational actuals (GRID_INDIA_SCADA, regional level only). |
| `DSM_PPA_CONTRACT` | Power purchase agreements | INTERNAL_CONTRACTS (not created) | `dsm_ppa_contract` | Register an internal source only once access to signed PPA documents or contract records is confirmed. No public endpoint exists. |
| `DSM_SCHEDULE_INTRASTATE` | Intra-state DSM schedules | Relevant SLDC(s) | `dsm_plant_schedule_actual` | No SLDC sources registered; publishers and formats vary by state. |
| `DSM_SETTLEMENT_INTRASTATE` | Intra-state DSM settlement | Relevant SLDC(s) | `dsm_settlement` | No SLDC sources registered; publishers and formats vary by state. |

## Tables used by every dataset

- `ingestion_run`
- `source_document`
- `data_quality_issue`

Reference tables needed before loading: `market_master`, `market_area` (IEX); `plant`, `generating_unit`, `source_entity_mapping` (CEA, DSM); `state`, `region` (NPP, Grid-India); `weather_location` (Open-Meteo); `interconnector`, `country` (Grid-India exchange).
