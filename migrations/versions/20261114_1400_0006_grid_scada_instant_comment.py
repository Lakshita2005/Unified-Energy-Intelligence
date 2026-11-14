"""Grid-India TimeSeries: document that grid_15min_scada.interval_start is an instant.

Approved 2026-11-14 (TimeSeries audit, decision T2). The Daily PSP
'TimeSeries' sheet publishes '15 Min (INSTANTANEOUS)' all-India SCADA values
at each 15-minute mark; they are not interval averages. The values are stored
with the mark in ``interval_start``; this migration only sets the column
comment so the meaning is visible in the schema. No column, constraint, index
or row is changed.

Revision ID: 0006_grid_scada_instant_comment
Revises: 0005_grid_crossborder
Create Date: 2026-11-14
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from config.settings import get_settings

revision = "0006_grid_scada_instant_comment"
down_revision = "0005_grid_crossborder"
branch_labels = None
depends_on = None

SCHEMA = get_settings().db_schema
COMMENT = ("15-minute mark (IST, timezone-aware). Grid-India TimeSeries values are instantaneous SCADA readings at this "
           "mark, not averages over the following 15 minutes")


def upgrade() -> None:
    op.alter_column("grid_15min_scada", "interval_start", existing_type=sa.DateTime(timezone=True),
                    existing_nullable=False, comment=COMMENT, schema=SCHEMA)


def downgrade() -> None:
    op.alter_column("grid_15min_scada", "interval_start", existing_type=sa.DateTime(timezone=True),
                    existing_nullable=False, comment=None, existing_comment=COMMENT, schema=SCHEMA)
