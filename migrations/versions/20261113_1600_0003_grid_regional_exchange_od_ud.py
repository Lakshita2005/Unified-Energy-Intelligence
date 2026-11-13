"""Grid-India Section E: published over/under-drawal on grid_regional_exchange.

Approved 2026-11-13. The Daily PSP Section E row 'O/D/U/D(MU)' is published
separately from Schedule and Actual and is not always equal to
Actual - Schedule (80 of 324 values differ by up to 0.013 MU in the 2023-2026
workbooks), so it cannot be derived without changing published values.
Additive: one nullable column; nothing is dropped or rewritten.

Revision ID: 0003_grid_regional_exchange_od_ud
Revises: 0002_grid_india
Create Date: 2026-11-13
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import context, op

from config.settings import get_settings

revision = "0003_grid_regional_exchange_od_ud"
down_revision = "0002_grid_india"
branch_labels = None
depends_on = None

SCHEMA = get_settings().db_schema


def upgrade() -> None:
    op.add_column("grid_regional_exchange", sa.Column(
        "od_ud_mu", sa.Numeric(18, 4), nullable=True,
        comment="Published over-drawal (+) / under-drawal (-), MU (Section E 'O/D/U/D'); not always Actual - Schedule"),
        schema=SCHEMA)


def downgrade() -> None:
    if not context.is_offline_mode():
        populated = op.get_bind().execute(sa.text(
            f"SELECT count(*) FROM {SCHEMA}.grid_regional_exchange WHERE od_ud_mu IS NOT NULL")).scalar_one()
        if populated:
            raise RuntimeError(f"Refusing to downgrade: {populated} rows hold published od_ud_mu values.")
    op.drop_column("grid_regional_exchange", "od_ud_mu", schema=SCHEMA)
