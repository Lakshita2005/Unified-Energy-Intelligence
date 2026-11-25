"""IEX market snapshot volumes and congestion: pi.iex_market_snapshot.

Prepared 2026-11-25; NOT applied. Additive: one new table and its lookup indexes; nothing existing is changed.

The IEX ``market-snapshot`` response (DAM, GDAM, RTM; one record per delivery date x 15-minute block) carries
purchase bid, sell bid, MCV, final scheduled volume, the congestion flag and, for GDAM, a volume breakdown by
source. ``pi.iex_market_price`` stores only its price; until now the rest stayed in the raw files. This table
holds it, loaded from the same raw document as the price (datasets IEX_DAM_SNAPSHOT, IEX_GDAM_SNAPSHOT,
IEX_RTM_SNAPSHOT), and history is backfilled from stored raw files (``python -m scripts.ingest_iex
backfill-snapshot``).

* Volumes are MW (15-minute interval, per the IEX site's column definitions), stored exactly as published
  (unconstrained ``numeric``).
* GDAM totals share the DAM/RTM columns: ``total_sell_bid`` -> ``sell_bid_mw``, ``total_unconstrained_m_c_v`` ->
  ``mcv_mw``, ``total_cleared_volume`` -> ``final_scheduled_volume_mw``.
* GDAM per-source columns: solar / non_solar / hydro up to delivery date 2026-07-07, hydro / wind / other_re /
  dre from 2026-07-08; sources not published for a date are NULL, and all are NULL for DAM and RTM.
* Loader natural key ``(market_code, delivery_date, time_block)`` with the VERSION policy: a revised re-fetch
  adds a new row and earlier rows are kept, so there is no unique constraint.

The upgrade refuses if the table already exists; the downgrade refuses if it holds rows.

Privileges: the ``pi`` schema is owned by ``postgres``. Apply with the schema owner after a rolled-back dry
run. The application role (``PI_APP_ROLE``, default ``pi_app``) only reads the table and appends rows (the
loader's VERSION policy never updates or deletes), so it ends with exactly SELECT and INSERT: the schema's
default privileges also grant UPDATE and DELETE on every new ``pi`` table, and this migration revokes those
two for this table only (the schema's default privileges are not changed). The identity column's sequence
keeps the schema's default sequence privileges.

Chain (approved 2026-11-25): 0009_mosdac -> 0010_grid_state_ace -> 0012_area_state_map ->
0013_iex_market_snapshot -> 0011_pi_jobs.

Revision ID: 0013_iex_market_snapshot
Revises: 0012_area_state_map
Create Date: 2026-11-25
"""

from __future__ import annotations

import os
import re

import sqlalchemy as sa
from alembic import context, op

from config.settings import get_settings

revision = "0013_iex_market_snapshot"
down_revision = "0012_area_state_map"
branch_labels = None
depends_on = None

SCHEMA = get_settings().db_schema
TABLE = "iex_market_snapshot"
APP_ROLE = os.environ.get("PI_APP_ROLE", "pi_app")
if not re.fullmatch(r"[a-z_][a-z0-9_]*", APP_ROLE):
    raise RuntimeError(f"PI_APP_ROLE must be a plain lower-case role name, got {APP_ROLE!r}")


def app_role_statements(schema: str = SCHEMA, role: str = APP_ROLE) -> list[str]:
    """Exactly SELECT and INSERT for the application role (a function so tests can check it offline)."""
    return [f"GRANT SELECT, INSERT ON {schema}.{TABLE} TO {role}",
            f"REVOKE UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON {schema}.{TABLE} FROM {role}"]


def _restrict_app_role() -> None:
    if not context.is_offline_mode() and op.get_bind().execute(
            sa.text("SELECT 1 FROM pg_roles WHERE rolname = :r"), {"r": APP_ROLE}).first() is None:
        return   # no such role in this database: nothing was granted to it
    for statement in app_role_statements():
        op.execute(statement)
GDAM_SOURCES = ("solar", "non_solar", "hydro", "wind", "other_re", "dre")
BREAKDOWN = (("sell_bid_mw", "sell_bid"), ("mcv_mw", "unconstrained_m_c_v"), ("cleared_volume_mw", "cleared_volume"))
BREAKDOWN_COLUMNS = tuple(f"{source}_{column}" for source in GDAM_SOURCES for column, _ in BREAKDOWN)
VOLUME_COLUMNS = ("purchase_bid_mw", "sell_bid_mw", "mcv_mw", "final_scheduled_volume_mw", *BREAKDOWN_COLUMNS)


def upgrade() -> None:
    if not context.is_offline_mode() and sa.inspect(op.get_bind()).has_table(TABLE, schema=SCHEMA):
        raise RuntimeError(f"Refusing to upgrade: {SCHEMA}.{TABLE} already exists")
    op.create_table(
        TABLE,
        sa.Column("id", sa.BigInteger, sa.Identity(always=True), primary_key=True),
        sa.Column("market_code", sa.String(20), sa.ForeignKey(f"{SCHEMA}.market_master.market_code"),
                  nullable=False, comment="IEX_DAM, IEX_GDAM or IEX_RTM"),
        sa.Column("delivery_date", sa.Date, nullable=False, comment="IST delivery day"),
        sa.Column("time_block", sa.SmallInteger, nullable=False,
                  comment="15-minute block 1-96 (1 = 00:00-00:15 IST)"),
        sa.Column("purchase_bid_mw", sa.Numeric, nullable=False, comment="Published purchase_bid, MW"),
        sa.Column("sell_bid_mw", sa.Numeric, nullable=False,
                  comment="Published sell_bid (DAM/RTM) or total_sell_bid (GDAM), MW"),
        sa.Column("mcv_mw", sa.Numeric, nullable=False,
                  comment="Market clearing volume: mcv (DAM/RTM) or total_unconstrained_m_c_v (GDAM), MW; "
                          "unconstrained, before congestion"),
        sa.Column("final_scheduled_volume_mw", sa.Numeric, nullable=False,
                  comment="final_scheduled_volume (DAM/RTM) or total_cleared_volume (GDAM), MW; after congestion"),
        sa.Column("congestion", sa.Boolean, nullable=False, comment="Published congestion flag (YES/NO)"),
        *(sa.Column(f"{source}_{column}", sa.Numeric, nullable=True,
                    comment=f"GDAM {source}_{field}, MW; NULL for DAM/RTM and when not published for the date")
          for source in GDAM_SOURCES for column, field in BREAKDOWN),
        sa.Column("retrieved_at", sa.DateTime(timezone=True), nullable=False,
                  comment="When this system received the snapshot response"),
        sa.Column("ingestion_run_id", sa.BigInteger, sa.ForeignKey(f"{SCHEMA}.ingestion_run.ingestion_run_id"),
                  nullable=False),
        sa.Column("document_id", sa.BigInteger, sa.ForeignKey(f"{SCHEMA}.source_document.document_id"),
                  nullable=True),
        sa.CheckConstraint("time_block BETWEEN 1 AND 96", name="time_block"),
        *(sa.CheckConstraint(f"{column} IS NULL OR {column} >= 0", name=f"{column}_nonneg")
          for column in VOLUME_COLUMNS),
        sa.CheckConstraint(f"market_code = 'IEX_GDAM' OR num_nonnulls({', '.join(BREAKDOWN_COLUMNS)}) = 0",
                           name="breakdown_gdam_only"),
        schema=SCHEMA,
        comment="IEX market snapshot volumes and congestion per market x delivery date x 15-minute block; "
                "every revision kept",
    )
    op.create_index(f"ix_{TABLE}_key", TABLE, ["market_code", "delivery_date", "time_block"], schema=SCHEMA)
    op.create_index(f"ix_{TABLE}_document", TABLE, ["document_id"], schema=SCHEMA)
    _restrict_app_role()


def downgrade() -> None:
    if not context.is_offline_mode():
        rows = op.get_bind().execute(sa.text(f"SELECT count(*) FROM {SCHEMA}.{TABLE}")).scalar_one()
        if rows:
            raise RuntimeError(f"Refusing to downgrade: {SCHEMA}.{TABLE} holds {rows} rows.")
    op.drop_table(TABLE, schema=SCHEMA)
