"""IMD: AWS stations, AWS observations and city forecasts.

Proposed 2026-11-15 (IMD Phase 2). Additive: three new tables and their
lookup indexes; nothing existing is changed.

* ``imd_station`` (dataset ``IMD_AWS_OBS``, from ``/api/v1/aws_data_mapping``):
  AWS/ARG station attributes as published, versioned. Station identifiers
  are not assumed unique (no unique constraint); the loader's natural key is
  ``(network, imd_id, call_sign)``.
* ``imd_station_observation`` (dataset ``IMD_AWS_OBS``, from ``/api/v1/aws_data``):
  one row per station and published observation date/time. The published
  DATE and TIME are stored as such; ``observed_at`` stays NULL until the
  time zone is verified. ``imd_station_id`` links to the station row matched
  exactly on ``(imd_id, call_sign)`` (NULL for an unknown station, which is
  kept and reported).
* ``imd_city_forecast`` (dataset ``IMD_CITY_FORECAST``, from ``/api/v1/cityforecast``):
  one row per city station, issue date (the published ``Date``) and forecast
  date. Every issue date is a separate vintage; a changed re-fetch of the same
  issue becomes a new version. Older vintages are never overwritten.

Units, timestamps and identifiers are unverified until authorised live
testing, so measurement columns are neutral (``*_value``) and unconstrained
``numeric`` (published digits kept exactly).

The upgrade refuses if a table already exists; the downgrade refuses if any
of the three tables holds rows.

Revision ID: 0008_imd
Revises: 0007_wbes_px
Create Date: 2026-11-15
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import context, op

from config.settings import get_settings

revision = "0008_imd"
down_revision = "0007_wbes_px"
branch_labels = None
depends_on = None

SCHEMA = get_settings().db_schema
TABLES = ("imd_station", "imd_station_observation", "imd_city_forecast")
UNIT_NOTE = "unit not yet verified; value as published"


def _lineage() -> list:
    return [
        sa.Column("retrieved_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source_published_at", sa.DateTime(timezone=True),
                  comment="Publication time stated by IMD; NULL (not published by the API)"),
        sa.Column("source_available_at", sa.DateTime(timezone=True), nullable=False,
                  comment="When the row may be used without look-ahead"),
        sa.Column("availability_basis", sa.String(30), nullable=False,
                  comment="RETRIEVED_AT (= retrieved_at) or SOURCE_PUBLISHED_AT (= source_published_at)"),
        sa.Column("ingestion_run_id", sa.BigInteger, sa.ForeignKey(f"{SCHEMA}.ingestion_run.ingestion_run_id"),
                  nullable=False),
        sa.Column("document_id", sa.BigInteger, sa.ForeignKey(f"{SCHEMA}.source_document.document_id")),
        sa.CheckConstraint("availability_basis IN ('RETRIEVED_AT', 'SOURCE_PUBLISHED_AT')", name="availability_basis"),
        sa.CheckConstraint(
            "(availability_basis = 'RETRIEVED_AT' AND source_available_at = retrieved_at) OR "
            "(availability_basis = 'SOURCE_PUBLISHED_AT' AND source_published_at IS NOT NULL "
            "AND source_available_at = source_published_at)", name="availability_consistent"),
    ]


def upgrade() -> None:
    if not context.is_offline_mode():
        existing = [t for t in TABLES if sa.inspect(op.get_bind()).has_table(t, schema=SCHEMA)]
        if existing:
            raise RuntimeError(f"Refusing to upgrade: tables already exist: {existing}")

    op.create_table(
        "imd_station",
        sa.Column("imd_station_id", sa.BigInteger, sa.Identity(always=True), primary_key=True),
        sa.Column("network", sa.String(20), nullable=False, comment="Station network, e.g. AWS_ARG"),
        sa.Column("imd_id", sa.Text, nullable=False, comment="Published station ID (e.g. B48970CA); uniqueness unverified"),
        sa.Column("call_sign", sa.Text, nullable=False, comment="Published CALL_SIGN (e.g. NDL); uniqueness unverified"),
        sa.Column("station_name", sa.Text, comment="Published STATION"),
        sa.Column("district_name", sa.Text, comment="Published DISTRICT"),
        sa.Column("state_name", sa.Text, comment="Published STATE"),
        sa.Column("state_code", sa.String(10), sa.ForeignKey(f"{SCHEMA}.state.state_code"),
                  comment="PI state from the exact published STATE name; NULL if not mapped"),
        sa.Column("latitude", sa.Numeric, comment="Published Latitude (degrees, as published)"),
        sa.Column("longitude", sa.Numeric, comment="Published Longitude (degrees, as published)"),
        *_lineage(),
        sa.CheckConstraint("latitude IS NULL OR latitude BETWEEN -90 AND 90", name="latitude"),
        sa.CheckConstraint("longitude IS NULL OR longitude BETWEEN -180 AND 180", name="longitude"),
        schema=SCHEMA,
        comment="IMD AWS/ARG stations as published by /api/v1/aws_data_mapping (dataset IMD_AWS_OBS), versioned",
    )
    op.create_table(
        "imd_station_observation",
        sa.Column("id", sa.BigInteger, sa.Identity(always=True), primary_key=True),
        sa.Column("network", sa.String(20), nullable=False),
        sa.Column("imd_id", sa.Text, nullable=False, comment="Published station ID"),
        sa.Column("call_sign", sa.Text, nullable=False, comment="Published CALL_SIGN"),
        sa.Column("imd_station_id", sa.BigInteger, sa.ForeignKey(f"{SCHEMA}.imd_station.imd_station_id"),
                  comment="Station row matched exactly on (imd_id, call_sign); NULL for an unknown station"),
        sa.Column("observation_date", sa.Date, nullable=False, comment="Published DATE"),
        sa.Column("observation_time", sa.Time, nullable=False, comment="Published TIME (time zone unverified)"),
        sa.Column("observed_at", sa.DateTime(timezone=True),
                  comment="DATE+TIME in the configured time zone; NULL while the time zone is unverified"),
        sa.Column("station_name", sa.Text, comment="Published STATION (context)"),
        sa.Column("district_name", sa.Text, comment="Published DISTRICT (context)"),
        sa.Column("state_name", sa.Text, comment="Published STATE (context)"),
        sa.Column("state_code", sa.String(10), sa.ForeignKey(f"{SCHEMA}.state.state_code")),
        sa.Column("latitude", sa.Numeric, comment="Published Latitude (context)"),
        sa.Column("longitude", sa.Numeric, comment="Published Longitude (context)"),
        sa.Column("curr_temp_value", sa.Numeric, comment=f"CURR_TEMP; {UNIT_NOTE}"),
        sa.Column("dew_point_temp_value", sa.Numeric, comment=f"DEW_POINT_TEMP; {UNIT_NOTE}"),
        sa.Column("rh_value", sa.Numeric, comment=f"RH; {UNIT_NOTE}"),
        sa.Column("wind_direction_value", sa.Numeric, comment=f"WIND_DIRECTION; {UNIT_NOTE}"),
        sa.Column("wind_speed_value", sa.Numeric, comment=f"WIND_SPEED; {UNIT_NOTE}"),
        sa.Column("mslp_value", sa.Numeric, comment=f"MSLP; {UNIT_NOTE}"),
        sa.Column("min_temp_value", sa.Numeric, comment=f"MIN_TEMP; {UNIT_NOTE}"),
        sa.Column("max_temp_value", sa.Numeric, comment=f"MAX_TEMP; {UNIT_NOTE}"),
        sa.Column("feels_like_value", sa.Numeric, comment=f"'Feel Like'; {UNIT_NOTE}"),
        sa.Column("weather_code", sa.Numeric, comment="WEATHER_CODE as published"),
        sa.Column("nebulosity_value", sa.Numeric, comment="NEBULOSITY as published"),
        *_lineage(),
        schema=SCHEMA,
        comment="IMD AWS/ARG observations from /api/v1/aws_data (dataset IMD_AWS_OBS); revisions are new versions",
    )
    op.create_table(
        "imd_city_forecast",
        sa.Column("id", sa.BigInteger, sa.Identity(always=True), primary_key=True),
        sa.Column("station_code", sa.Text, nullable=False, comment="Published Station_Code"),
        sa.Column("station_name", sa.Text, comment="Published Station_Name"),
        sa.Column("issue_date", sa.Date, nullable=False,
                  comment="Published Date of the response (forecast vintage); issue time not published"),
        sa.Column("forecast_date", sa.Date, nullable=False, comment="Target date = issue_date + lead_day - 1"),
        sa.Column("lead_day", sa.SmallInteger, nullable=False, comment="1 (Todays_Forecast_*) .. 7 (Day_7_*)"),
        sa.Column("max_temp_value", sa.Numeric, comment="Forecast maximum temperature; documented as degC, unverified"),
        sa.Column("min_temp_value", sa.Numeric, comment="Forecast minimum temperature; documented as degC, unverified"),
        sa.Column("forecast_text", sa.Text, comment="Published weather forecast text/code for the day"),
        *_lineage(),
        sa.CheckConstraint("lead_day BETWEEN 1 AND 7", name="lead_day"),
        sa.CheckConstraint("forecast_date = issue_date + (lead_day - 1)", name="forecast_date"),
        schema=SCHEMA,
        comment="IMD 7-day city forecasts from /api/v1/cityforecast (dataset IMD_CITY_FORECAST); one vintage per issue",
    )
    # Non-unique lookup indexes on the loader's natural keys (identifier uniqueness is unverified).
    op.create_index("ix_imd_station_key", "imd_station", ["network", "imd_id", "call_sign"], schema=SCHEMA)
    op.create_index("ix_imd_station_state", "imd_station", ["state_code"], schema=SCHEMA)
    op.create_index("ix_imd_station_observation_key", "imd_station_observation",
                    ["network", "imd_id", "call_sign", "observation_date", "observation_time"], schema=SCHEMA)
    op.create_index("ix_imd_station_observation_station", "imd_station_observation", ["imd_station_id"],
                    schema=SCHEMA)
    op.create_index("ix_imd_city_forecast_key", "imd_city_forecast",
                    ["station_code", "issue_date", "forecast_date"], schema=SCHEMA)
    op.create_index("ix_imd_city_forecast_target", "imd_city_forecast", ["forecast_date", "station_code"],
                    schema=SCHEMA)
    for table in TABLES:
        op.create_index(f"ix_{table}_document", table, ["document_id"], schema=SCHEMA)


def downgrade() -> None:
    if not context.is_offline_mode():
        bind = op.get_bind()
        counts = {t: bind.execute(sa.text(f"SELECT count(*) FROM {SCHEMA}.{t}")).scalar_one() for t in TABLES}
        non_empty = {t: n for t, n in counts.items() if n}
        if non_empty:
            raise RuntimeError(f"Refusing to downgrade: IMD tables hold rows {non_empty}.")
    for table in reversed(TABLES):
        op.drop_table(table, schema=SCHEMA)
