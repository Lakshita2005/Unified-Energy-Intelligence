"""Grid-India: columns for the NLDC Daily PSP workbook, entity representation and availability timestamps.

Reviewed under decision G1 (docs/grid_india_source_assessment.md). Additive:
nullable columns, CHECK constraints, foreign keys to existing tables and
lookup indexes are added to the nine existing, empty ``grid_*`` tables. The
only change to an existing column is dropping NOT NULL from
``grid_state_power_supply.state_code`` (non-state entities such as DVC). No
table, column, constraint or row is dropped or rewritten.

Representation:
* All-India: ``region_code IS NULL`` in ``grid_daily_power_supply`` and
  ``grid_15min_scada`` (as in ``npp_demand_generation``). Additive totals
  (fuel mix, outage, regional exchange) are not stored; they are validated.
* States and other drawal entities (DVC, J&K + Ladakh, bulk consumers,
  Section J entities): ``area_code`` / ``area_name`` / ``area_type`` in
  ``grid_state_power_supply``; ``state_code`` is set only for real states.

Availability (decision G3), on every table:
* ``source_published_at``: the actual upload time (Grid-India ``CreatedOn``),
  only where it is reliable (report dates from 2025-05-12); else NULL.
* ``source_available_at``: when the row may be used without look-ahead.
* ``availability_basis``: ``PUBLISHED`` (= source_published_at) or
  ``ESTIMATED_D_PLUS_2_0900`` (report date + 2 days, 09:00 IST).
* A CHECK constraint enforces the combinations: an estimated row has no
  actual publication time and its availability is exactly D+2 09:00 IST.

The upgrade refuses to run if any ``grid_*`` table already has rows, so it
can never act on existing data. The downgrade refuses to run if dropping the
new columns would lose data (any row in a grid table).

Revision ID: 0002_grid_india
Revises: 0001_cea
Create Date: 2026-11-13
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import context, op

from config.settings import get_settings

revision = "0002_grid_india"
down_revision = "0001_cea"
branch_labels = None
depends_on = None

SCHEMA = get_settings().db_schema

GRID_TABLES = (
    "grid_daily_power_supply", "grid_state_power_supply", "grid_sourcewise_generation", "grid_generation_outage",
    "grid_regional_exchange", "grid_interregional_line_exchange", "grid_international_line_exchange",
    "grid_crossborder_market_schedule", "grid_15min_scada",
)
MW = sa.Numeric(16, 3)
MU = sa.Numeric(18, 4)

# table -> [(column, type, comment)]  (all nullable)
NEW_COLUMNS: dict[str, list[tuple[str, sa.types.TypeEngine, str]]] = {
    "grid_daily_power_supply": [
        ("evening_peak_demand_met_mw", MW, "Demand met during evening peak (20:00), MW"),
        ("evening_peak_shortage_mw", MW, "Shortage during evening peak, MW"),
        ("energy_shortage_mu", MU, "Energy shortage, MU"),
        ("max_demand_met_mw", MW, "Maximum demand met during the day, MW"),
        ("max_demand_met_time", sa.Time, "Time of maximum demand met (IST, on the report day)"),
        ("hydro_gen_mu", MU, "Hydro generation, MU"),
        ("wind_gen_mu", MU, "Wind generation, MU"),
        ("solar_gen_mu", MU, "Solar generation, MU"),
    ],
    "grid_state_power_supply": [
        ("area_code", sa.String(40), "STATE: ISO 3166-2:IN code; SYSTEM: DVC, JK_LA; BULK_CONSUMER: BULK_<region>; "
                                     "ENTITY: normalised published name"),
        ("area_name", sa.Text, "Name as published"),
        ("area_type", sa.String(16), "STATE, SYSTEM, BULK_CONSUMER or ENTITY"),
        ("region_code", sa.String(10), "Region the report lists the area under"),
        ("shortage_at_max_demand_mw", MW, "Shortage during maximum demand, MW"),
        ("drawal_schedule_mu", MU, "Drawal schedule, MU"),
        ("od_ud_mu", MU, "Over-drawal (+) / under-drawal (-), MU"),
        ("max_od_mw", MW, "Maximum over-drawal, MW (first value of 'max OD/UD')"),
        ("max_ud_mw", MW, "Maximum under-drawal, MW, negative (second value of 'max OD/UD')"),
        ("energy_shortage_mu", MU, "Energy shortage, MU"),
    ],
    "grid_sourcewise_generation": [
        ("fuel_label", sa.Text, "Fuel row label as published, e.g. 'Gas, Naptha & Diesel'"),
    ],
    "grid_generation_outage": [
        ("sector", sa.String(10), "CENTRAL or STATE (Section F rows)"),
    ],
    "grid_regional_exchange": [],
    "grid_interregional_line_exchange": [
        ("voltage_level", sa.String(20), "As published, e.g. '765 kV', 'HVDC'"),
        ("circuits", sa.String(10), "No. of circuits as published ('-' kept as NULL)"),
        ("max_import_mw", MW, "Maximum import, MW"),
        ("max_export_mw", MW, "Maximum export, MW"),
        ("import_mu", MU, "Import, MU"),
        ("export_mu", MU, "Export, MU"),
    ],
    "grid_international_line_exchange": [
        ("region_code", sa.String(10), "Indian region the line connects to"),
        ("max_mw", MW, "Maximum flow, MW (import +ve)"),
        ("min_mw", MW, "Minimum flow, MW (import +ve)"),
        ("avg_mw", MW, "Average flow, MW (import +ve)"),
    ],
    "grid_crossborder_market_schedule": [],
    "grid_15min_scada": [
        ("storage_demand_mw", MW, "Storage demand (pumped storage and BESS charging), MW"),
        ("nuclear_generation_mw", MW, "Nuclear, MW"),
        ("hydro_generation_mw", MW, "Hydro, MW"),
        ("gas_generation_mw", MW, "Gas, MW"),
        ("thermal_generation_mw", MW, "Thermal, MW"),
        ("storage_generation_mw", MW, "Storage generation (pumped storage and BESS), MW"),
        ("other_generation_mw", MW, "Others, MW"),
        ("net_demand_met_mw", MW, "Net demand met = demand met - wind - solar, MW"),
        ("total_generation_mw", MW, "Total generation, MW"),
        ("net_transnational_exchange_mw", MW, "Net transnational exchange, MW (import +ve)"),
    ],
}

# Lineage and availability columns added to every grid table.
LINEAGE_COLUMNS: list[tuple[str, sa.types.TypeEngine, str]] = [
    ("source_published_at", sa.DateTime(timezone=True),
     "Actual upload time from Grid-India (CreatedOn) where reliable (report dates from 2025-05-12); else NULL"),
    ("source_available_at", sa.DateTime(timezone=True),
     "When this row may be used without look-ahead: source_published_at, or the estimate in availability_basis"),
    ("availability_basis", sa.String(30), "PUBLISHED or ESTIMATED_D_PLUS_2_0900"),
]

# table -> natural-key lookup index columns (non-unique; uniqueness is enforced by the loader)
LOOKUP_INDEXES: dict[str, tuple[str, ...]] = {
    "grid_daily_power_supply": ("region_code", "report_date"),
    "grid_state_power_supply": ("area_code", "report_date"),
    "grid_sourcewise_generation": ("region_code", "fuel_type", "report_date"),
    "grid_generation_outage": ("region_code", "sector", "report_date"),
    "grid_regional_exchange": ("region_code", "report_date"),
    "grid_interregional_line_exchange": ("from_region_code", "to_region_code", "line_name", "report_date"),
    "grid_international_line_exchange": ("country_code", "line_name", "report_date"),
    "grid_crossborder_market_schedule": ("country_code", "contract_type", "power_exchange", "direction",
                                         "report_date"),
    # grid_15min_scada already has ix_scada_region_time (region_code, interval_start)
}

# grid_state_power_supply: every row has an area; state_code is set exactly for real states.
STATE_CHECKS: tuple[tuple[str, str], ...] = (
    ("ck_grid_state_power_supply_area_code", "area_code IS NOT NULL"),
    ("ck_grid_state_power_supply_area_type", "area_type IN ('STATE', 'SYSTEM', 'BULK_CONSUMER', 'ENTITY')"),
    ("ck_grid_state_power_supply_state_code", "(area_type = 'STATE') = (state_code IS NOT NULL)"),
)


# The operating day of a row, for the D+2 09:00 IST estimate.
REPORT_DAY = {"grid_15min_scada": "(interval_start AT TIME ZONE 'Asia/Kolkata')::date"}


def _timing_check(table: str) -> str:
    """Actual and estimated availability can never be confused (decision G3).

    * PUBLISHED: an actual upload time exists and the row is available from it.
    * ESTIMATED_D_PLUS_2_0900: no actual time is recorded, and availability is
      exactly the report day + 2 days at 09:00 IST.
    * NULL basis: neither timestamp is set.
    """
    day = REPORT_DAY.get(table, "report_date")
    estimate = f"((({day}) + 2)::timestamp + time '09:00') AT TIME ZONE 'Asia/Kolkata'"
    return ("(availability_basis = 'PUBLISHED' AND source_published_at IS NOT NULL "
            "AND source_available_at = source_published_at) OR "
            "(availability_basis = 'ESTIMATED_D_PLUS_2_0900' AND source_published_at IS NULL "
            f"AND source_available_at = {estimate}) OR "
            "(availability_basis IS NULL AND source_published_at IS NULL AND source_available_at IS NULL)")


def _index_name(table: str) -> str:
    return f"ix_{table}_key"


def _row_counts(bind: sa.engine.Connection) -> dict[str, int]:
    if context.is_offline_mode():  # `alembic upgrade --sql` renders SQL only; the check runs on real upgrades
        return {}
    return {t: bind.execute(sa.text(f"SELECT count(*) FROM {SCHEMA}.{t}")).scalar_one() for t in GRID_TABLES}


def upgrade() -> None:
    bind = op.get_bind()
    non_empty = {t: n for t, n in _row_counts(bind).items() if n}
    if non_empty:
        raise RuntimeError(f"0002_grid_india expects empty grid tables; found rows in {non_empty}. Nothing changed.")

    for table in GRID_TABLES:
        for name, type_, comment in [*NEW_COLUMNS[table], *LINEAGE_COLUMNS]:
            op.add_column(table, sa.Column(name, type_, nullable=True, comment=comment), schema=SCHEMA)
        op.add_column(table, sa.Column("document_id", sa.BigInteger, nullable=True,
                                       comment="Source workbook (pi.source_document)"), schema=SCHEMA)
        op.create_foreign_key(f"fk_{table}_document_id", table, "source_document", ["document_id"], ["document_id"],
                              source_schema=SCHEMA, referent_schema=SCHEMA)
        op.create_check_constraint(
            op.f(f"ck_{table}_availability_basis"), table,
            "availability_basis IS NULL OR availability_basis IN ('PUBLISHED', 'ESTIMATED_D_PLUS_2_0900')",
            schema=SCHEMA)
        op.create_check_constraint(op.f(f"ck_{table}_published_basis"), table, _timing_check(table), schema=SCHEMA)
        if table in LOOKUP_INDEXES:
            op.create_index(_index_name(table), table, list(LOOKUP_INDEXES[table]), schema=SCHEMA)

    # States and other drawal entities.
    op.alter_column("grid_state_power_supply", "state_code", existing_type=sa.String(10), nullable=True,
                    schema=SCHEMA)
    op.create_foreign_key("fk_grid_state_power_supply_region_code", "grid_state_power_supply", "region",
                          ["region_code"], ["region_code"], source_schema=SCHEMA, referent_schema=SCHEMA)
    for name, condition in STATE_CHECKS:
        op.create_check_constraint(op.f(name), "grid_state_power_supply", condition, schema=SCHEMA)

    op.create_check_constraint(op.f("ck_grid_generation_outage_sector"), "grid_generation_outage",
                               "sector IS NULL OR sector IN ('CENTRAL', 'STATE')", schema=SCHEMA)
    op.create_foreign_key("fk_grid_international_line_exchange_region_code", "grid_international_line_exchange",
                          "region", ["region_code"], ["region_code"], source_schema=SCHEMA, referent_schema=SCHEMA)
    op.create_check_constraint(op.f("ck_grid_crossborder_market_schedule_direction"),
                               "grid_crossborder_market_schedule",
                               "direction IS NULL OR direction IN ('EXPORT', 'IMPORT')", schema=SCHEMA)


def downgrade() -> None:
    bind = op.get_bind()
    non_empty = {t: n for t, n in _row_counts(bind).items() if n}
    if non_empty:
        raise RuntimeError(
            f"Refusing to downgrade 0002_grid_india: grid tables contain rows {non_empty}. Dropping the added "
            "columns would discard loaded data. Export the data first, then remove the rows deliberately.")

    op.drop_constraint(op.f("ck_grid_crossborder_market_schedule_direction"), "grid_crossborder_market_schedule",
                       type_="check", schema=SCHEMA)
    op.drop_constraint("fk_grid_international_line_exchange_region_code", "grid_international_line_exchange",
                       type_="foreignkey", schema=SCHEMA)
    op.drop_constraint(op.f("ck_grid_generation_outage_sector"), "grid_generation_outage", type_="check",
                       schema=SCHEMA)
    for name, _ in reversed(STATE_CHECKS):
        op.drop_constraint(op.f(name), "grid_state_power_supply", type_="check", schema=SCHEMA)
    op.drop_constraint("fk_grid_state_power_supply_region_code", "grid_state_power_supply", type_="foreignkey",
                       schema=SCHEMA)
    op.alter_column("grid_state_power_supply", "state_code", existing_type=sa.String(10), nullable=False,
                    schema=SCHEMA)

    for table in reversed(GRID_TABLES):
        if table in LOOKUP_INDEXES:
            op.drop_index(_index_name(table), table_name=table, schema=SCHEMA)
        op.drop_constraint(op.f(f"ck_{table}_published_basis"), table, type_="check", schema=SCHEMA)
        op.drop_constraint(op.f(f"ck_{table}_availability_basis"), table, type_="check", schema=SCHEMA)
        op.drop_constraint(f"fk_{table}_document_id", table, type_="foreignkey", schema=SCHEMA)
        op.drop_column(table, "document_id", schema=SCHEMA)
        for name, _, _ in reversed([*NEW_COLUMNS[table], *LINEAGE_COLUMNS]):
            op.drop_column(table, name, schema=SCHEMA)
