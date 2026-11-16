"""Expected tables in the Price Intelligence ``pi`` schema.

This is the contract the database health check verifies against. The ``pi``
schema is managed outside this application (it has no Alembic history), and
the application never creates, alters or drops these tables: if one is
missing, the health check reports it so the schema owner can fix the database.

The list was taken from ``price_intelligence_dev`` on 2026-11-12. Review it
against the agreed SQL schema, and update it deliberately when that changes.
"""

from __future__ import annotations

EXPECTED_TABLES: frozenset[str] = frozenset(
    {
        # Reference / master data
        "country",
        "state",
        "region",
        "entity",
        "market_area",
        "market_master",
        "interconnector",
        "plant",
        "generating_unit",
        "pooling_station",
        "plant_qca_assignment",
        "weather_location",
        # Source catalogue and ingestion lineage
        "data_source",
        "source_dataset",
        "source_document",
        "source_entity_mapping",
        "dataset_tag",
        "ingestion_run",
        "data_quality_issue",
        # CEA reports
        "cea_coal_stock_daily",
        "cea_coal_stock_monthly",
        "cea_dgr_plant_daily",
        "cea_dgr_unit_outage",
        "cea_power_supply_position",  # Alembic 0001_cea
        # Deviation settlement mechanism (DSM)
        "dsm_plant_schedule_actual",
        "dsm_ppa_contract",
        "dsm_settlement",
        # Grid operations
        "grid_15min_scada",
        "grid_crossborder_market_schedule",
        "grid_daily_power_supply",
        "grid_generation_outage",
        "grid_international_line_exchange",
        "grid_interregional_line_exchange",
        "grid_regional_exchange",
        "grid_sourcewise_generation",
        "grid_state_power_supply",
        "npp_demand_generation",
        # Markets and weather
        "iex_market_price",
        "open_meteo_weather",
        # IMD AWS observations and city forecasts (Alembic 0008_imd)
        "imd_city_forecast",
        "imd_station",
        "imd_station_observation",
        # WBES PX Dashboard (Alembic 0007_wbes_px)
        "wbes_px_entity_schedule",
        "wbes_px_link_schedule",
        "wbes_px_region_schedule",
        # Features and forecasting
        "feature_definition",
        "feature_source_map",
        "feature_tag",
        "feature_tag_map",
        "forecasting_feature_value",
        "forecast_model",
        "forecast_run",
        "forecast_result",
    }
)
