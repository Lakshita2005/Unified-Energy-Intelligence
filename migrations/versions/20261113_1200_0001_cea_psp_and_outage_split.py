"""CEA: monthly power supply position table and outage category columns.

Approved in docs/cea_source_assessment.md (decision D2, 2026-11-13). This is
the first Alembic migration for the ``pi`` schema, which was created outside
Alembic. It is purely additive: it creates one new table and adds nullable
columns and an index. No existing column, constraint or row is changed.

* ``cea_power_supply_position``: CEA monthly "Power Supply Position" reports
  (energy requirement/supplied, peak demand/met), one row per area x month x
  period (month or April-to-month) x measure.
* ``cea_dgr_unit_outage``: planned / forced (major) / forced (minor) / other
  MW as published in DGR sub-report 10, the published unit label and state.

Revision ID: 0001_cea
Revises:
Create Date: 2026-11-13
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from config.settings import get_settings

revision = "0001_cea"
down_revision = None
branch_labels = None
depends_on = None

SCHEMA = get_settings().db_schema


def upgrade() -> None:
    op.create_table(
        "cea_power_supply_position",
        sa.Column("id", sa.BigInteger, sa.Identity(always=True), primary_key=True),
        sa.Column("report_month", sa.Date, nullable=False, comment="First day of the reported month"),
        sa.Column("period_scope", sa.String(10), nullable=False, comment="MONTH or FYTD (April to report month)"),
        sa.Column("measure", sa.String(10), nullable=False, comment="ENERGY (MU) or PEAK (MW)"),
        sa.Column("area_code", sa.String(40), nullable=False),
        sa.Column("area_name", sa.Text, nullable=False, comment="As published"),
        sa.Column("area_type", sa.String(12), nullable=False),
        sa.Column("state_code", sa.String(10), sa.ForeignKey(f"{SCHEMA}.state.state_code"), nullable=True),
        sa.Column("region_code", sa.String(10), sa.ForeignKey(f"{SCHEMA}.region.region_code"), nullable=True),
        sa.Column("requirement", sa.Numeric(18, 3), comment="Energy requirement (MU) or peak demand (MW)"),
        sa.Column("supplied", sa.Numeric(18, 3), comment="Energy supplied (MU) or peak met (MW)"),
        sa.Column("not_supplied", sa.Numeric(18, 3), comment="Energy not supplied (MU) or demand not met (MW)"),
        sa.Column("not_supplied_pct", sa.Numeric(8, 2)),
        sa.Column("unit", sa.String(4), nullable=False),
        sa.Column("report_status", sa.Text, comment="e.g. Revised, Corrigendum (from the report title)"),
        sa.Column("footnote_flags", sa.Text, comment="Footnote markers printed on the row, e.g. (#) (**)"),
        sa.Column("ingestion_run_id", sa.BigInteger, sa.ForeignKey(f"{SCHEMA}.ingestion_run.ingestion_run_id"),
                  nullable=False),
        sa.Column("document_id", sa.BigInteger, sa.ForeignKey(f"{SCHEMA}.source_document.document_id"),
                  nullable=True),
        sa.CheckConstraint("period_scope IN ('MONTH', 'FYTD')", name="period_scope"),
        sa.CheckConstraint("measure IN ('ENERGY', 'PEAK')", name="measure"),
        sa.CheckConstraint("area_type IN ('STATE', 'SYSTEM', 'REGION', 'COUNTRY', 'OTHER')",
                           name="area_type"),
        sa.CheckConstraint("(measure = 'ENERGY' AND unit = 'MU') OR (measure = 'PEAK' AND unit = 'MW')",
                           name="unit"),
        sa.CheckConstraint("report_month = date_trunc('month', report_month)::date", name="month_start"),
        sa.UniqueConstraint("area_code", "report_month", "period_scope", "measure", "ingestion_run_id",
                            name="uq_cea_psp_key_run"),
        schema=SCHEMA,
    )
    op.create_index("ix_cea_psp_key", "cea_power_supply_position",
                    ["area_code", "report_month", "period_scope", "measure"], schema=SCHEMA)

    for name, type_ in (("planned_mw", sa.Numeric(14, 3)), ("forced_major_mw", sa.Numeric(14, 3)),
                        ("forced_minor_mw", sa.Numeric(14, 3)), ("other_mw", sa.Numeric(14, 3)),
                        ("unit_label", sa.String(20)), ("state_name", sa.Text)):
        op.add_column("cea_dgr_unit_outage", sa.Column(name, type_, nullable=True), schema=SCHEMA)
    op.create_index("ix_cea_outage_plant_unit_date", "cea_dgr_unit_outage",
                    ["plant_id", "unit_label", "report_date"], schema=SCHEMA)


def downgrade() -> None:
    op.drop_index("ix_cea_outage_plant_unit_date", table_name="cea_dgr_unit_outage", schema=SCHEMA)
    for name in ("state_name", "unit_label", "other_mw", "forced_minor_mw", "forced_major_mw", "planned_mw"):
        op.drop_column("cea_dgr_unit_outage", name, schema=SCHEMA)
    op.drop_index("ix_cea_psp_key", table_name="cea_power_supply_position", schema=SCHEMA)
    op.drop_table("cea_power_supply_position", schema=SCHEMA)
