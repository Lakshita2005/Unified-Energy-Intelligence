"""Grid-India sections C and J: Area Control Error (ACE) maximum on grid_state_power_supply.

Prepared 2026-11-25 (option A); NOT applied. From report date 2026-11-18 the Daily PSP sections C (sheet
MOP_E) and J (sheet MOP_E1) publish "ACE²(Max) +ve/ -ve (MW)" (footnote "²Area Control Error") in place of
"Max OD(+)/ UD(-) (MW)". ACE is a different measure from maximum over/under-drawal, so it must not be stored
in max_od_mw / max_ud_mw. Until this migration is applied, the parser keeps those NULL for ACE-layout
reports and keeps the ACE pair in the record's raw data and the raw workbook (ACE_NOT_STORED warning).

Additive: two nullable columns; nothing is dropped or rewritten. After applying it, the parser must also
emit the two columns (values from the record's raw ace_max_positive_mw / ace_max_negative_mw), and report
dates from 2026-11-18 can be re-parsed with ``--reprocess`` to fill them as new row versions.

Privileges: grid_state_power_supply is owned by ``postgres``; the application account (``pi_app``) is not a
member of the owner role and cannot ALTER it. Apply with the schema owner, after a rolled-back dry run.

Revision ID: 0010_grid_state_ace
Revises: 0009_mosdac
Create Date: 2026-11-25
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import context, op

from config.settings import get_settings

revision = "0010_grid_state_ace"
down_revision = "0009_mosdac"
branch_labels = None
depends_on = None

SCHEMA = get_settings().db_schema
TABLE = "grid_state_power_supply"
MW = sa.Numeric(16, 3)   # same type as max_od_mw / max_ud_mw (0002_grid_india)
COLUMNS: tuple[tuple[str, str], ...] = (
    ("max_ace_pos_mw",
     "Maximum positive Area Control Error, MW (first value of 'ACE (Max) +ve/-ve (MW)', sections C/J, "
     "published from report date 2026-11-18); NULL for earlier reports, which publish max OD/UD instead"),
    ("max_ace_neg_mw",
     "Maximum negative Area Control Error, MW, as published (second value of 'ACE (Max) +ve/-ve (MW)', "
     "sections C/J, from report date 2026-11-18); NULL for earlier reports"),
)


def upgrade() -> None:
    for name, comment in COLUMNS:
        op.add_column(TABLE, sa.Column(name, MW, nullable=True, comment=comment), schema=SCHEMA)


def downgrade() -> None:
    if not context.is_offline_mode():
        populated = op.get_bind().execute(sa.text(
            f"SELECT count(*) FROM {SCHEMA}.{TABLE} "
            "WHERE max_ace_pos_mw IS NOT NULL OR max_ace_neg_mw IS NOT NULL")).scalar_one()
        if populated:
            raise RuntimeError(f"Refusing to downgrade: {populated} rows hold published ACE values "
                               "(max_ace_pos_mw / max_ace_neg_mw).")
    for name, _ in reversed(COLUMNS):
        op.drop_column(TABLE, name, schema=SCHEMA)
