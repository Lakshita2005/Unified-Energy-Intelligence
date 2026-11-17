"""MOSDAC: solar and wind energy forecasts per plant location.

Proposed 2026-11-17. Additive: two new tables and their lookup indexes; nothing
existing is changed (Open-Meteo and IMD tables, including pi.weather_location,
are untouched).

* ``mosdac_solar_forecast`` (dataset ``MOSDAC_SOLAR_FORECAST``, from
  ``GET https://mosdac.gov.in/apienergy/solar?lat=&lon=``): air temperature and
  solar flux per location and 15-minute valid time.
* ``mosdac_wind_forecast`` (dataset ``MOSDAC_WIND_FORECAST``, from
  ``GET https://mosdac.gov.in/apienergy/wind?lat=&lon=``): wind speed and
  direction as published (units not verified, so neutral ``*_value`` columns).

Location: ``location_code`` with the request coordinates (from
config/mosdac_locations.toml). Not linked to pi.weather_location, because
Open-Meteo's all-locations runs include every registered weather location.

Vintages: MOSDAC publishes no issue time. ``series_start_at`` (the earliest
valid time in a response) identifies a forecast series; the loader's natural
key is ``(location_code, series_start_at, valid_at)`` with the VERSION policy,
so a revised re-fetch adds new versions and earlier rows are never
overwritten. There is no unique constraint (versions share a natural key).

The upgrade refuses if a table already exists; the downgrade refuses if either
table holds rows.

Revision ID: 0009_mosdac
Revises: 0008_imd
Create Date: 2026-11-17
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import context, op

from config.settings import get_settings

revision = "0009_mosdac"
down_revision = "0008_imd"
branch_labels = None
depends_on = None

SCHEMA = get_settings().db_schema
TABLES = ("mosdac_solar_forecast", "mosdac_wind_forecast")
UNIT_NOTE = "unit not yet verified; value as published"


def _common() -> list:
    return [
        sa.Column("id", sa.BigInteger, sa.Identity(always=True), primary_key=True),
        sa.Column("location_code", sa.String(40), nullable=False,
                  comment="Location code from config/mosdac_locations.toml (e.g. ESEPL)"),
        sa.Column("latitude", sa.Numeric(10, 7), nullable=False, comment="Latitude sent as lat= (WGS84 degrees)"),
        sa.Column("longitude", sa.Numeric(10, 7), nullable=False, comment="Longitude sent as lon= (WGS84 degrees)"),
        sa.Column("series_start_at", sa.DateTime(timezone=True), nullable=False,
                  comment="Earliest valid time in the response: identifies the forecast series (issue time not "
                          "published)"),
        sa.Column("valid_at", sa.DateTime(timezone=True), nullable=False,
                  comment="Forecast valid time: published 'time' (DD-MM-YY HHMM, IST)"),
        sa.Column("source_time_text", sa.String(20), nullable=False, comment="Published 'time' text, unchanged"),
    ]


def _lineage() -> list:
    return [
        sa.Column("retrieved_at", sa.DateTime(timezone=True), nullable=False,
                  comment="When the response was retrieved (the forecast vintage as seen by PI)"),
        sa.Column("source_published_at", sa.DateTime(timezone=True),
                  comment="Publication/issue time stated by MOSDAC; NULL (not published by the API)"),
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
        sa.CheckConstraint("valid_at >= series_start_at", name="valid_after_series_start"),
        sa.CheckConstraint("latitude BETWEEN -90 AND 90", name="latitude"),
        sa.CheckConstraint("longitude BETWEEN -180 AND 180", name="longitude"),
    ]


def upgrade() -> None:
    if not context.is_offline_mode():
        existing = [t for t in TABLES if sa.inspect(op.get_bind()).has_table(t, schema=SCHEMA)]
        if existing:
            raise RuntimeError(f"Refusing to upgrade: tables already exist: {existing}")

    op.create_table(
        "mosdac_solar_forecast",
        *_common(),
        sa.Column("air_temperature_c", sa.Numeric,
                  comment="Published 'temp': air temperature, degC (per the MOSDAC endpoint description)"),
        sa.Column("solar_flux_w_m2", sa.Numeric,
                  comment="Published 'flux': solar flux, W/m2 (per the endpoint description). Not GHI; no DNI/DHI/GTI"),
        *_lineage(),
        schema=SCHEMA,
        comment="MOSDAC solar energy forecasts (dataset MOSDAC_SOLAR_FORECAST); every series and revision kept",
    )
    op.create_table(
        "mosdac_wind_forecast",
        *_common(),
        sa.Column("wind_speed_value", sa.Numeric, comment=f"Published 'speed'; {UNIT_NOTE}"),
        sa.Column("wind_direction_value", sa.Numeric, comment=f"Published 'direction'; {UNIT_NOTE}"),
        *_lineage(),
        schema=SCHEMA,
        comment="MOSDAC wind energy forecasts (dataset MOSDAC_WIND_FORECAST); every series and revision kept",
    )
    # Non-unique lookup indexes (versions share a natural key, so no unique constraint).
    for table in TABLES:
        op.create_index(f"ix_{table}_key", table, ["location_code", "series_start_at", "valid_at"], schema=SCHEMA)
        op.create_index(f"ix_{table}_target", table, ["valid_at", "location_code"], schema=SCHEMA)
        op.create_index(f"ix_{table}_document", table, ["document_id"], schema=SCHEMA)


def downgrade() -> None:
    if not context.is_offline_mode():
        bind = op.get_bind()
        counts = {t: bind.execute(sa.text(f"SELECT count(*) FROM {SCHEMA}.{t}")).scalar_one() for t in TABLES}
        non_empty = {t: n for t, n in counts.items() if n}
        if non_empty:
            raise RuntimeError(f"Refusing to downgrade: MOSDAC tables hold rows {non_empty}.")
    for table in reversed(TABLES):
        op.drop_table(table, schema=SCHEMA)
