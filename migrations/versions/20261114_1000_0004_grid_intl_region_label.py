"""Grid-India IR-Line: published Region cell on grid_international_line_exchange.

Approved 2026-11-14 (IR-Line audit). The international part of the Daily PSP
'IR-Line' sheet publishes a Region cell such as 'ER', 'NER', 'NR' or
'ER (Isolated from Indian Grid)' (Godda-Rahanpur, a radial line reported
separately in Section D). ``region_code`` alone loses that qualifier, so the
cell is kept verbatim in ``region_label``. Additive: one nullable column;
nothing is dropped or rewritten.

Revision ID: 0004_grid_intl_region_label
Revises: 0003_grid_regional_exch_od_ud
Create Date: 2026-11-14
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import context, op

from config.settings import get_settings

revision = "0004_grid_intl_region_label"
down_revision = "0003_grid_regional_exch_od_ud"
branch_labels = None
depends_on = None

SCHEMA = get_settings().db_schema


def upgrade() -> None:
    op.add_column("grid_international_line_exchange", sa.Column(
        "region_label", sa.Text, nullable=True,
        comment="Published Region cell verbatim (IR-Line international section), e.g. 'ER (Isolated from Indian Grid)'"),
        schema=SCHEMA)


def downgrade() -> None:
    if not context.is_offline_mode():
        populated = op.get_bind().execute(sa.text(
            f"SELECT count(*) FROM {SCHEMA}.grid_international_line_exchange WHERE region_label IS NOT NULL")).scalar_one()
        if populated:
            raise RuntimeError(f"Refusing to downgrade: {populated} rows hold published region_label values.")
    op.drop_column("grid_international_line_exchange", "region_label", schema=SCHEMA)
