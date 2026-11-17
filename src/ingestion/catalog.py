"""Catalogue of Price Intelligence data sources and datasets.

This module is the single source of truth for ``pi.data_source`` and
``pi.source_dataset``. ``scripts/register_sources.py`` upserts it into the
database and generates ``docs/dataset_table_mapping.md`` from it. Source
pipelines should reference these definitions (e.g. ``SOURCES["IEX"]``) so
their registration always matches.

Verification policy: URLs, report references, granularities and frequencies
are recorded only where verified (2026-11-12) against the publisher's site
or official documents. Anything unverified is recorded as
``PENDING_CONFIRMATION`` rather than guessed. Datasets whose publisher is not
confirmed are listed in :data:`PENDING_DATASETS` and are **not** registered.

Codes are stable identifiers: never rename one that has been registered.
"""

from __future__ import annotations

from dataclasses import dataclass

from src.ingestion.models import DatasetDefinition, SourceDefinition

PENDING = "PENDING_CONFIRMATION"
IST = "Asia/Kolkata"
UTC = "UTC"

SOURCES: dict[str, SourceDefinition] = {
    source.code: source
    for source in (
        # The first five match the rows already seeded in pi.data_source.
        SourceDefinition("CEA", "Central Electricity Authority", "https://cea.nic.in/"),
        SourceDefinition("NPP", "National Power Portal", "https://npp.gov.in/"),
        SourceDefinition("GRID_INDIA", "Grid-India", "https://grid-india.in/"),
        SourceDefinition("OPEN_METEO", "Open-Meteo", "https://open-meteo.com/"),
        SourceDefinition("IEX", "Indian Energy Exchange", "https://www.iexindia.com/"),
        # Publishers of weekly Deviation Settlement Account statements. There is
        # no single website: each RPC (NRPC, WRPC, SRPC, ERPC, NERPC) has its own.
        SourceDefinition("RPC", "Regional Power Committees (NRPC, WRPC, SRPC, ERPC, NERPC)", None),
        # Grid-India's Web Based Energy Scheduling portal: a separate system from grid-india.in, with its own
        # login (user + OTP + CAPTCHA) and terms (decision W3).
        SourceDefinition("WBES", "Grid-India WBES (Web Based Energy Scheduling)", "https://newwbes.grid-india.in/"),
        # India Meteorological Department API platform (access by IMD approval and IP whitelisting).
        SourceDefinition("IMD", "India Meteorological Department", "https://mausam.imd.gov.in/"),
        # ISRO Space Applications Centre's MOSDAC energy forecast API (solar / wind per lat-lon). Its data
        # guidelines forbid resale/redistribution of downloaded products; business use is by agreement.
        SourceDefinition("MOSDAC", "MOSDAC (ISRO Space Applications Centre)", "https://www.mosdac.gov.in/"),
    )
}


@dataclass(frozen=True)
class CatalogDataset:
    """A dataset to register, with its source and destination tables."""

    source_code: str
    definition: DatasetDefinition
    destination_tables: tuple[str, ...]
    notes: str = ""


@dataclass(frozen=True)
class PendingDataset:
    """A dataset deliberately NOT registered until its publisher is confirmed."""

    proposed_code: str
    name: str
    candidate_publishers: str
    destination_tables: tuple[str, ...]
    reason: str


def _dataset(
    source_code: str,
    code: str,
    name: str,
    endpoint_or_report: str,
    native_granularity: str,
    expected_frequency: str,
    destination_tables: tuple[str, ...],
    *,
    timezone: str = IST,
    notes: str = "",
) -> CatalogDataset:
    return CatalogDataset(
        source_code,
        DatasetDefinition(code, name, endpoint_or_report, native_granularity, expected_frequency, timezone),
        destination_tables,
        notes,
    )


DATASETS: tuple[CatalogDataset, ...] = (
    # ------------------------------------------------------------------ CEA
    # Daily reports are mirrored on NPP as .xls (loaded) and .pdf; verified live
    # 2026-11-13 (docs/cea_source_assessment.md). The monthly power supply
    # position reports are PDFs listed on the rebuilt cea.nic.in.
    _dataset(
        "CEA", "CEA_COAL_STOCK_DAILY", "CEA Daily Coal Stock Report (thermal power stations)",
        "CEA Fuel Management Division Daily Coal Stock Report (XLS, published on NPP): "
        "https://npp.gov.in/public-reports/cea/daily/fuel/{DD-MM-YYYY}/dailyCoal1-{YYYY-MM-DD}.xls "
        "(also .pdf). Report 'AS ON' D is published about D+1 15:30 IST; available from 2024-06.",
        "daily", "daily", ("cea_coal_stock_daily",),
        notes="One row per plant per report date, stock in '000 tonnes as published. Plant names are mapped "
              "via source_entity_mapping. stock_days is left NULL (not published; see CEA_DGR_DAILY sub-report 2).",
    ),
    _dataset(
        "CEA", "CEA_COAL_STOCK_MONTHLY", "CEA Monthly Coal Statement of Thermal Power Stations",
        "CEA Monthly Coal Statement (PDF, published on NPP): "
        "https://npp.gov.in/public-reports/cea/monthly/fuel/{YYYY}/{MON}/monthCoal1-{YYYY}-{Mon}.pdf",
        "monthly", "monthly", ("cea_coal_stock_monthly",),
        notes="Older reports (2018-2023) are on cea.nic.in under wp-content/uploads/coal/.",
    ),
    _dataset(
        "CEA", "CEA_DGR_DAILY", "CEA Daily Generation Report (DGR)",
        "CEA OPM Division Daily Generation Report (XLS on NPP, also .pdf): "
        "https://npp.gov.in/public-reports/cea/daily/dgr/{DD-MM-YYYY}/dgr{N}-{YYYY-MM-DD}.xls. Sub-report 2 "
        "(region/state/sector/type/station/unit-wise generation, coal stock days, capacity under outage) -> "
        "cea_dgr_plant_daily; sub-report 10 (daily maintenance report, coal/lignite/nuclear units: planned/forced/"
        "other MW, outage time, expected/sync time, reason) -> cea_dgr_unit_outage. Report D is published about "
        "D+1 17:00 IST; available from 2018-04.",
        "daily", "daily", ("cea_dgr_plant_daily", "cea_dgr_unit_outage"),
        notes="Stations of 25 MW and above. Report footer: 'for operational purpose only and not for commercial "
              "purpose'. Gas/hydro unit outages are not in sub-report 10.",
    ),
    _dataset(
        "CEA", "CEA_PSP_ENERGY_MONTHLY", "CEA monthly Energy Power Supply Position (state-wise)",
        "CEA 'Power Supply Position - Energy' monthly PDF, listed at https://cea.nic.in/reports/dashboardView?"
        "headingId=107&subHeadingId=1003&subSubHeadingId=10010&lang=en; file: "
        "https://cea.nic.in/api/dashboardReports/download-pdf/en/{uuid}",
        "monthly", "monthly", ("cea_power_supply_position",),
        notes="Energy requirement / supplied / not supplied (MU net) for the month and April-to-month, per "
              "state/system, region and All India. Published about 2.5 months after the month; revised reports "
              "replace earlier ones. Supported from 2020-04 (current layout); older listing entries are an older "
              "layout or mislabelled generation reports and are rejected. File UUIDs change between page loads.",
    ),
    _dataset(
        "CEA", "CEA_PSP_PEAK_MONTHLY", "CEA monthly Peak Power Supply Position (state-wise)",
        "CEA 'Power Supply Position - Peak' monthly PDF, listed at https://cea.nic.in/reports/dashboardView?"
        "headingId=107&subHeadingId=1003&subSubHeadingId=10011&lang=en; file: "
        "https://cea.nic.in/api/dashboardReports/download-pdf/en/{uuid}",
        "monthly", "monthly", ("cea_power_supply_position",),
        notes="Peak demand / peak met / demand not met (MW net) for the month and April-to-month. Same layout and "
              "timing as CEA_PSP_ENERGY_MONTHLY.",
    ),
    # ------------------------------------------------------------------ NPP
    # Both endpoints are undocumented JSON behind the NPP "Real Time Generation
    # and Demand Data (Source-MERIT INDIA)" dashboard; verified live 2026-11-12.
    _dataset(
        "NPP", "NPP_DEMAND", "NPP All-India real-time demand met (source: MERIT India)",
        "Undocumented NPP dashboard JSON: https://npp.gov.in/dashBoard/demandmet1chartdata?date={YYYY-MM-DD} "
        "(IST day). Fields updated_on (epoch ms), name_of_data ('DEMAND MET'), value_of_data (MW). "
        "History from about 2025-10.",
        "irregular ~2-4 min snapshots", "intraday, continuous", ("npp_demand_generation",),
        notes="All-India only: state_code and region_code are NULL. Snapshot times are stored as published "
              "(not rounded to 15-minute blocks).",
    ),
    _dataset(
        "NPP", "NPP_GENERATION", "NPP All-India real-time generation by source (source: MERIT India)",
        "Undocumented NPP dashboard JSON: https://npp.gov.in/dashBoard/demandmet2chartdata?date={YYYY-MM-DD} "
        "(IST day). Same fields as demandmet1chartdata; series THERMAL, HYDRO, GAS, NUCLEAR, SOLAR and "
        "WIND GENERATION (MW). History from about 2025-10.",
        "irregular ~2-4 min snapshots", "intraday, continuous", ("npp_demand_generation",),
        notes="All-India only (NULL state_code/region_code). Series names are stored verbatim; the composition "
              "of THERMAL GENERATION (coal/lignite only or more) is not documented and must not be assumed.",
    ),
    # ----------------------------------------------------------- Grid-India
    _dataset(
        # The NLDC Daily PSP workbook is downloaded once and parsed by one dataset per destination table
        # (documents are de-duplicated per dataset). Verified live 2026-11-13; see
        # docs/grid_india_source_assessment.md and docs/grid_india_pipeline.md.
        "GRID_INDIA", "GRID_INDIA_PSP_DAILY", "Grid-India (NLDC) Daily PSP report - Section A (all-India/regional)",
        "NLDC Daily PSP workbook (XLS from 2023-01; PDF before). Listing: POST https://webapi.grid-india.in/api/v1/file "
        "{_source: GRDW, _type: DAILY_PSP_REPORT, _fileDate: <FY e.g. 2026-27>, _month: MM}; file: "
        "https://webcdn.grid-india.in/<FilePath>. Section A, sheet MOP_E: evening-peak demand met and shortage, "
        "energy met, hydro/wind/solar generation, energy shortage, maximum demand met and its time per region and "
        "All India.",
        "daily", "daily", ("grid_daily_power_supply",),
        notes="Report for operating day D is uploaded on D+1 (~10:00-11:00 IST); revisions are extra files. "
              "All India = region_code NULL. Availability: actual upload time (CreatedOn) for report dates from "
              "2025-05-12, else estimated D+2 09:00 IST.",
    ),
    _dataset(
        "GRID_INDIA", "GRID_INDIA_PSP_STATES", "Grid-India (NLDC) Daily PSP report - Sections C and J (states)",
        "Same NLDC Daily PSP workbook as GRID_INDIA_PSP_DAILY (listing POST https://webapi.grid-india.in/api/v1/file, "
        "file https://webcdn.grid-india.in/<FilePath>). Section C (sheet MOP_E, 'Power Supply Position in States') and "
        "Section J (sheet MOP_E1, ISTS bulk consumers, from 2026): maximum demand met, shortage at maximum demand, "
        "energy met, drawal schedule, over/under-drawal (MU), maximum over/under-drawal (MW), energy shortage.",
        "daily", "daily", ("grid_state_power_supply",),
        notes="One row per state, system (DVC, J&K+Ladakh), regional bulk consumer aggregate and Section J entity. "
              "DNHDDPDCL is stored as state DH with its published name (utility demand, excluding separately "
              "reported bulk consumers).",
    ),
    _dataset(
        # Was a pending placeholder (no runs or documents); now the Daily PSP 'TimeSeries' sheet (decision T1).
        # Regional 15-minute SCADA was not found publicly; this is the all-India series only.
        "GRID_INDIA", "GRID_INDIA_SCADA", "Grid-India (NLDC) Daily PSP report - TimeSeries (all-India 15-minute SCADA)",
        "Same NLDC Daily PSP workbook as GRID_INDIA_PSP_DAILY (listing POST https://webapi.grid-india.in/api/v1/file, "
        "file https://webcdn.grid-india.in/<FilePath>). Sheet 'TimeSeries', '15 Min (INSTANTANEOUS) ALL INDIA GRID "
        "FREQUENCY, GENERATION & DEMAND MET (SCADA DATA)' (from report date 2024-11-04): frequency, demand met, "
        "generation by source, storage (from 2026-05/06), net demand met, total generation and net transnational "
        "exchange (import +ve) at each 15-minute mark 0:00-23:45.",
        "15min", "daily", ("grid_15min_scada",),
        notes="Instantaneous values at each mark (not averages; column comment, migration 0006). All-India only "
              "(region_code NULL). Storage generation from 2026-05-06, storage demand from 2026-06-01 (NULL before). "
              "Values stored as published, including telemetry-loss zeros (flagged).",
    ),
    _dataset(
        # Was a pending placeholder (no runs or documents); now Section E only. IR-Line and CrossBorder will get
        # their own datasets, since documents are de-duplicated per dataset.
        "GRID_INDIA", "GRID_INDIA_EXCHANGE", "Grid-India (NLDC) Daily PSP report - Section E (regional exchange)",
        "Same NLDC Daily PSP workbook as GRID_INDIA_PSP_DAILY (listing POST https://webapi.grid-india.in/api/v1/file, "
        "file https://webcdn.grid-india.in/<FilePath>). Section E, sheet MOP_E, 'Import/Export by Regions (in MU) - "
        "Import(+ve)/Export(-ve); OD(+)/UD(-)': Schedule, Actual and O/D/U/D per region (NR, WR, SR, ER, NER), "
        "with TOTAL.",
        "daily", "daily", ("grid_regional_exchange",),
        notes="Signed MU values as published (import +ve, export -ve; over-drawal +ve, under-drawal -ve). The published "
              "O/D/U/D is stored separately (od_ud_mu, migration 0003) because it is not always Actual - Schedule. "
              "exchange_direction is left NULL; the TOTAL column is additive and validated, not stored.",
    ),
    _dataset(
        # Was a pending placeholder (no runs or documents); now Section G only. Section F (generation outage
        # totals) will get its own dataset, since documents are de-duplicated per dataset.
        "GRID_INDIA", "GRID_INDIA_GENERATION", "Grid-India (NLDC) Daily PSP report - Section G (source-wise generation)",
        "Same NLDC Daily PSP workbook as GRID_INDIA_PSP_DAILY (listing POST https://webapi.grid-india.in/api/v1/file, "
        "file https://webcdn.grid-india.in/<FilePath>). Section G, sheet MOP_E, 'Sourcewise generation (Gross) (MU)': "
        "Coal, Lignite, Hydro, Nuclear, 'Gas, Naptha & Diesel', 'RES (Wind, Solar, Biomass & Others)' per region "
        "(NR, WR, SR, ER, NER), with All India and Total for validation.",
        "daily", "daily", ("grid_sourcewise_generation",),
        notes="Gross generation in MU. One row per region x fuel; the All-India column and the Total row are "
              "additive and are validated, not stored. fuel_type is a normalised code, fuel_label the published label.",
    ),
    _dataset(
        "GRID_INDIA", "GRID_INDIA_PSP_IR_LINES", "Grid-India (NLDC) Daily PSP report - IR-Line (inter-regional lines)",
        "Same NLDC Daily PSP workbook as GRID_INDIA_PSP_DAILY (listing POST https://webapi.grid-india.in/api/v1/file, "
        "file https://webcdn.grid-india.in/<FilePath>). Sheet 'IR-Line', 'INTER-REGIONAL EXCHANGES': per line in "
        "blocks 'Import/Export of <region> (With <region>)': voltage level, circuits, max import/export (MW), "
        "import/export (MU), NET (MU).",
        "daily", "daily", ("grid_interregional_line_exchange",),
        notes="from_region_code = the first-named region of the block; NET import +ve relative to it (sheet title). "
              "Import/export are published as separate non-negative values. Block subtotal rows are validated, not "
              "stored (the ER-SR subtotal excludes TALCHER-I/C).",
    ),
    _dataset(
        "GRID_INDIA", "GRID_INDIA_PSP_INTL_LINES", "Grid-India (NLDC) Daily PSP report - IR-Line (international lines)",
        "Same NLDC Daily PSP workbook as GRID_INDIA_PSP_DAILY (listing POST https://webapi.grid-india.in/api/v1/file, "
        "file https://webcdn.grid-india.in/<FilePath>). Sheet 'IR-Line', 'INTERNATIONAL EXCHANGES' (Import(+ve)/"
        "Export(-ve)): per line: country, region, max/min/avg (MW), energy exchange (MU).",
        "daily", "daily", ("grid_international_line_exchange",),
        notes="Country from an exact line-name reference (ISO alpha-3 BTN/NPL/BGD); the published Region cell is kept "
              "in region_label (migration 0004). Totals per country are compared with Section D.",
    ),
    _dataset(
        "GRID_INDIA", "GRID_INDIA_PSP_CROSSBORDER", "Grid-India (NLDC) Daily PSP report - CrossBorder (exchange schedule)",
        "Same NLDC Daily PSP workbook as GRID_INDIA_PSP_DAILY (listing POST https://webapi.grid-india.in/api/v1/file, "
        "file https://webcdn.grid-india.in/<FilePath>). Sheet 'CrossBorder', 'CROSS BORDER EXCHANGE SCHEDULE' (from "
        "report date 2023-07-06): Export From India, Import by India and Net from India (-ve export / +ve import), "
        "in MU, per country (Bhutan, Nepal, Bangladesh, Myanmar) and contract: GNA (ISGS/PPA), T-GNA Bilateral, "
        "Collective IDAM and RTM per exchange (IEX, PXIL, HPX); before 2023-10-01 (ISGS/LTA/MTOA) PPA and STOA.",
        "daily", "daily", ("grid_crossborder_market_schedule",),
        notes="Scheduled energy. Net rows stored as direction NET (migration 0005); row TOTAL column and Total rows "
              "validated, not stored. Pre-2023-10 contract categories keep their own codes.",
    ),
    _dataset(
        "GRID_INDIA", "GRID_INDIA_PSP_OUTAGE", "Grid-India (NLDC) Daily PSP report - Section F (generation outage)",
        "Same NLDC Daily PSP workbook as GRID_INDIA_PSP_DAILY (listing POST https://webapi.grid-india.in/api/v1/file, "
        "file https://webcdn.grid-india.in/<FilePath>). Section F, sheet MOP_E, 'Generation Outage(MW)': Central "
        "Sector and State Sector outage capacity (MW) per region (NR, WR, SR, ER, NER), with TOTAL and Total.",
        "daily", "daily", ("grid_generation_outage",),
        notes="Aggregate outage MW by sector and region only (no plant, unit or reason). The TOTAL column and Total row "
              "are additive and validated, not stored. Unit-level outages come from CEA DGR (cea_dgr_unit_outage).",
    ),
    # ------------------------------------------------------------------ IEX
    # Undocumented JSON API behind the iexindia.com market data pages; verified
    # live 2026-11-12. Website terms restrict use to personal, non-commercial
    # purposes unless IEX authorises otherwise (docs/iex_source_assessment.md).
    _dataset(
        "IEX", "IEX_DAM", "IEX Day-Ahead Market (DAM) prices",
        "IEX API: https://www.iexindia.com/api/v1/dam/market-snapshot?interval=ONE_FOURTH_HOUR"
        "&fromDate={DD-MM-YYYY}&toDate={DD-MM-YYYY} (MCP) and /api/v1/dam/area-price?...&bidArea={A1..W3} "
        "(bid-area prices). Page: https://www.iexindia.com/market-data/day-ahead-market/market-snapshot",
        "15min", "daily", ("iex_market_price",),
        notes="Time blocks 1-96 (block 1 = 00:00-00:15 IST). MCP stored as area MKT plus 13 bid areas. "
              "15-minute data from at least 2015-01-01. Requires market_master/market_area reference rows.",
    ),
    _dataset(
        "IEX", "IEX_GDAM", "IEX Green Day-Ahead Market (G-DAM) prices",
        "IEX API: https://www.iexindia.com/api/v1/gdam/market-snapshot?interval=ONE_FOURTH_HOUR"
        "&fromDate={DD-MM-YYYY}&toDate={DD-MM-YYYY} (unconstrained MCP) and /api/v1/gdam/area-price?...&bidArea="
        "{A1..W3}. Page: https://www.iexindia.com/market-data/green-day-ahead-market/market-snapshot",
        "15min", "daily", ("iex_market_price",),
        notes="15-minute data from 2021-10-27. Price field is unconstrained_m_c_p; the volume breakdown fields "
              "changed with delivery date 2026-07-08 (solar/non-solar/hydro -> hydro/wind/other RE/DRE).",
    ),
    _dataset(
        "IEX", "IEX_RTM", "IEX Real-Time Market (RTM) prices",
        "IEX API: https://www.iexindia.com/api/v1/rtm/market-snapshot?interval=ONE_FOURTH_HOUR"
        "&fromDate={DD-MM-YYYY}&toDate={DD-MM-YYYY}. Page: "
        "https://www.iexindia.com/market-data/real-time-market/market-snapshot",
        "15min", "intraday (half-hourly sessions)", ("iex_market_price",),
        notes="15-minute data from 2020-06-01. MCP only (area MKT): the rtm/area-price endpoint returned no rows "
              "for any bid area. The current day fills in as sessions clear.",
    ),
    # ------------------------------------------------------------------ WBES
    _dataset(
        "WBES", "WBES_PX_REGION", "WBES PX Dashboard - region-wise injection / drawal",
        "WBES PX Dashboard (login-gated, undocumented JSON API at https://newwbes-api.grid-india.in): POST /powerexchange/api/PowerExchange/GetPowerExchangeByDetails per delivery date x power exchange (GetPxTypeList) x market (GetPxTransTypeList), RevisionNo -1 (all revisions). One response feeds WBES_PX_REGION, WBES_PX_LINK and WBES_PX_ENTITY.",
        "15min", "intraday", ("wbes_px_region_schedule",),
        notes="Units and region-id mapping unverified (values stored as published; region_code NULL until verified). "
              "Revision number, PowerExId and CreatedOn are lineage; new versions only when values change.",
    ),
    _dataset(
        "WBES", "WBES_PX_LINK", "WBES PX Dashboard - link-wise net amount",
        "WBES PX Dashboard (login-gated, undocumented JSON API at https://newwbes-api.grid-india.in): POST /powerexchange/api/PowerExchange/GetPowerExchangeByDetails per delivery date x power exchange (GetPxTypeList) x market (GetPxTransTypeList), RevisionNo -1 (all revisions). One response feeds WBES_PX_REGION, WBES_PX_LINK and WBES_PX_ENTITY.",
        "15min", "intraday", ("wbes_px_link_schedule",),
        notes="Units and NetAmount sign convention unverified; values stored as published.",
    ),
    _dataset(
        "WBES", "WBES_PX_ENTITY", "WBES PX Dashboard - entity-wise injection / drawal",
        "WBES PX Dashboard (login-gated, undocumented JSON API at https://newwbes-api.grid-india.in): POST /powerexchange/api/PowerExchange/GetPowerExchangeByDetails per delivery date x power exchange (GetPxTypeList) x market (GetPxTransTypeList), RevisionNo -1 (all revisions). One response feeds WBES_PX_REGION, WBES_PX_LINK and WBES_PX_ENTITY.",
        "15min", "intraday", ("wbes_px_entity_schedule",),
        notes="Units and entity-key uniqueness unverified (no unique constraint; duplicate keys within a revision "
              "are rejected and reported).",
    ),
    # ----------------------------------------------------------- Open-Meteo
    _dataset(
        "OPEN_METEO", "OPEN_METEO_FORECAST", "Open-Meteo weather forecast",
        "Open-Meteo Forecast API: https://api.open-meteo.com/v1/forecast",
        "hourly", "daily", ("open_meteo_weather",), timezone=UTC,
        notes="Loaded with is_forecast = true and forecast_issued_at set. API returns GMT unless a timezone "
              "parameter is sent. Locations come from weather_location.",
    ),
    _dataset(
        "OPEN_METEO", "OPEN_METEO_HISTORICAL", "Open-Meteo historical weather (reanalysis)",
        "Open-Meteo Historical Weather API: https://archive-api.open-meteo.com/v1/archive",
        "hourly", "daily", ("open_meteo_weather",), timezone=UTC,
        notes="Loaded with is_forecast = false. ERA5 / ERA5-Land / ECMWF IFS reanalysis.",
    ),
    # ------------------------------------------------------------------ DSM
    _dataset(
        "GRID_INDIA", "DSM_SCHEDULE_INTERSTATE", "DSM inter-state schedules issued by RLDCs",
        f"{PENDING}: publisher confirmed as the RLDCs (Grid-India) for inter-state scheduling; "
        "access method (e.g. WBES) and format not verified",
        "15min", PENDING, ("dsm_plant_schedule_actual",),
        notes="Fills the schedule_* columns. 15-minute DSM time blocks (1-96). Intra-state schedules "
              "(SLDCs) are not covered.",
    ),
    _dataset(
        "RPC", "DSM_SETTLEMENT_REGIONAL", "Regional weekly Deviation Settlement Account statements",
        "Weekly DSM statements issued by each RPC Secretariat (CERC DSM Regulations 2024). Verified listing: "
        f"ERPC https://erpc.gov.in/dsm-rras-fras-and-sced-accts; NRPC/WRPC/SRPC/NERPC locations {PENDING}. "
        "PDF formats vary by RPC; revised statements are issued for past weeks",
        "weekly", "weekly", ("dsm_settlement",),
        notes="Settlement period start/end per statement. Revisions must be loaded as new versions, "
              "not overwrites.",
    ),
    # ------------------------------------------------------------------ IMD
    # Tables from Alembic 0008_imd (applied 2026-11-16). Live IMD access is not yet authorised.
    _dataset(
        "IMD", "IMD_AWS_OBS", "IMD AWS/ARG station observations",
        "IMD API (https://api.imd.gov.in/api/v1): GET /aws_data (optionally ?id=<call sign> or ?sid=<state id>) "
        "for observations and GET /aws_data_mapping for stations. Access requires IMD approval and IP "
        "whitelisting.",
        "station observation", "intraday", ("imd_station", "imd_station_observation"),
        notes="Response container, units, DATE/TIME time zone and identifier uniqueness are unverified; values "
              "are stored as published in neutral *_value columns. The mapping endpoint's fields are not documented.",
    ),
    _dataset(
        "IMD", "IMD_CITY_FORECAST", "IMD 7-day city weather forecast",
        "IMD API (https://api.imd.gov.in/api/v1): GET /cityforecast (optionally ?id=<station code>). Access requires "
        "IMD approval and IP whitelisting.",
        "daily", "daily", ("imd_city_forecast",),
        notes="One vintage per published Date (issue time not published). Only forecast fields are loaded; the "
              "observed and astronomical fields of the response are kept in raw storage.",
    ),
    # ------------------------------------------------------------------ MOSDAC
    # Tables from Alembic 0009_mosdac (applied 2026-11-17). Not a confirmed replacement for IMD.
    _dataset(
        "MOSDAC", "MOSDAC_SOLAR_FORECAST", "MOSDAC solar energy forecast (air temperature, solar flux) per location",
        "MOSDAC energy API: GET https://mosdac.gov.in/apienergy/solar?lat=<latitude>&lon=<longitude>. Live "
        "2026-11-17: a JSON list of {time 'DD-MM-YY HHMM' (IST), temp, flux} with string values; 289 points, "
        "15-minute spacing, 72 hours.",
        "15min", "irregular (issue time not published)", ("mosdac_solar_forecast",),
        notes="temp = air temperature (degC) and flux = solar flux (W/m2) per the supplied endpoint description; "
              "flux is NOT GHI and no DNI/DHI/GTI is derived. The live series ended before retrieval (stale).",
    ),
    _dataset(
        "MOSDAC", "MOSDAC_WIND_FORECAST", "MOSDAC wind energy forecast (speed, direction) per location",
        "MOSDAC energy API: GET https://mosdac.gov.in/apienergy/wind?lat=<latitude>&lon=<longitude>. Expected "
        "{time 'DD-MM-YY HHMM' (IST), speed, direction}; returned HTTP 500 on its only live test (2026-11-17).",
        "15min", "irregular (issue time not published)", ("mosdac_wind_forecast",),
        notes="Speed and direction units are NOT verified; values are stored as published in neutral *_value "
              "columns.",
    ),
)

# The IMD entries of DATASETS (used by the IMD pipelines).
IMD_DATASETS: tuple[CatalogDataset, ...] = tuple(entry for entry in DATASETS if entry.source_code == "IMD")

# The MOSDAC source and entries of DATASETS (used by the MOSDAC pipelines).
MOSDAC_SOURCE: SourceDefinition = SOURCES["MOSDAC"]
MOSDAC_DATASETS: tuple[CatalogDataset, ...] = tuple(entry for entry in DATASETS if entry.source_code == "MOSDAC")

PENDING_DATASETS: tuple[PendingDataset, ...] = (
    PendingDataset(
        "DSM_ACTUAL_METER", "DSM actual injection/drawal (SEM/ABT meter data)",
        "GRID_INDIA (RLDC) for inter-state, or the relevant SLDC for intra-state",
        ("dsm_plant_schedule_actual",),
        "Publisher and access method not confirmed. Meter-based actuals must stay distinct from SCADA "
        "operational actuals (GRID_INDIA_SCADA, regional level only).",
    ),
    PendingDataset(
        "DSM_PPA_CONTRACT", "Power purchase agreements",
        "INTERNAL_CONTRACTS (not created)", ("dsm_ppa_contract",),
        "Register an internal source only once access to signed PPA documents or contract records is confirmed. "
        "No public endpoint exists.",
    ),
    PendingDataset(
        "DSM_SCHEDULE_INTRASTATE", "Intra-state DSM schedules", "Relevant SLDC(s)",
        ("dsm_plant_schedule_actual",), "No SLDC sources registered; publishers and formats vary by state.",
    ),
    PendingDataset(
        "DSM_SETTLEMENT_INTRASTATE", "Intra-state DSM settlement", "Relevant SLDC(s)",
        ("dsm_settlement",), "No SLDC sources registered; publishers and formats vary by state.",
    ),
)

# Shared lineage/reference tables used by every dataset.
COMMON_TABLES: tuple[str, ...] = ("ingestion_run", "source_document", "data_quality_issue")
