"""Grid-India CrossBorder: allow direction 'NET' on grid_crossborder_market_schedule.

Approved 2026-11-14 (CrossBorder audit, decision C1). The Daily PSP
'CrossBorder' sheet publishes three blocks per country and contract type:
'Export From India', 'Import by India' and 'Net from India' (signed,
"-ve : Export / +ve : Import"). The published Net values are stored as rows
with direction 'NET' (signed, ``sign_convention`` holds the published
convention) instead of being derived, like the published O/D/U/D in Section E.

The only change is the CHECK constraint on ``direction``: it is recreated
with 'NET' added. No column is added, dropped or rewritten.

Revision ID: 0005_grid_crossborder
Revises: 0004_grid_intl_region_label
Create Date: 2026-11-14
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import context, op

from config.settings import get_settings

revision = "0005_grid_crossborder"
down_revision = "0004_grid_intl_region_label"
branch_labels = None
depends_on = None

SCHEMA = get_settings().db_schema
TABLE = "grid_crossborder_market_schedule"
CONSTRAINT = "ck_grid_crossborder_market_schedule_direction"


def upgrade() -> None:
    op.drop_constraint(op.f(CONSTRAINT), TABLE, type_="check", schema=SCHEMA)
    op.create_check_constraint(op.f(CONSTRAINT), TABLE, "direction IS NULL OR direction IN ('EXPORT', 'IMPORT', 'NET')",
                               schema=SCHEMA)


def downgrade() -> None:
    if not context.is_offline_mode():
        net_rows = op.get_bind().execute(sa.text(
            f"SELECT count(*) FROM {SCHEMA}.{TABLE} WHERE direction = 'NET'")).scalar_one()
        if net_rows:
            raise RuntimeError(f"Refusing to downgrade: {net_rows} rows hold published Net (direction 'NET') values.")
    op.drop_constraint(op.f(CONSTRAINT), TABLE, type_="check", schema=SCHEMA)
    op.create_check_constraint(op.f(CONSTRAINT), TABLE, "direction IS NULL OR direction IN ('EXPORT', 'IMPORT')",
                               schema=SCHEMA)
