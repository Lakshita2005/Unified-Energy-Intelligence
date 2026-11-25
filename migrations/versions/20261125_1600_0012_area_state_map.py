"""IEX bid area -> state mapping with source and validity period: pi.area_state_map.

Prepared 2026-11-25; NOT applied. Additive: one new reference table; nothing existing is changed.

IEX bid areas (pi.market_area A1 ... W3) are exchange clearing zones defined for market splitting, not
states. This table records which states (or parts of a state, e.g. North Goa) a bid area covered over which
delivery dates, and the source each row rests on. Rows are seeded by
``python -m scripts.ingest_iex area-states`` (src/pipelines/iex/area_states.py), never by a price run.

* Validity is ``[effective_from, effective_to)`` in IST delivery dates; ``effective_to`` NULL = still valid.
  ``effective_from`` is the earliest date the cited source shows the mapping, unless ``verification_status``
  is OFFICIAL (then it is the officially notified start). A state can have gaps where no source is known.
* ``state_part`` NULL = the whole state; otherwise the published part (Goa and Puducherry are split
  between areas).
* At most one row per (state, part, effective_from) and at most one open-ended row per (state, part).
  Non-overlap of closed periods is checked by the seeding code (no btree_gist exclusion constraint).

Privileges: the ``pi`` schema is owned by ``postgres``. Apply with the schema owner after a rolled-back dry
run. The application role (``PI_APP_ROLE``, default ``pi_app``) only reads the table and appends seed rows,
so it ends with exactly SELECT and INSERT: the schema's default privileges also grant UPDATE and DELETE on
every new ``pi`` table, and this migration revokes those two for this table only (the schema's default
privileges are not changed).

Chain (approved 2026-11-25): 0009_mosdac -> 0010_grid_state_ace -> 0012_area_state_map ->
0013_iex_market_snapshot -> 0011_pi_jobs. This migration uses nothing from 0010.

Revision ID: 0012_area_state_map
Revises: 0010_grid_state_ace
Create Date: 2026-11-25
"""

from __future__ import annotations

import os
import re

import sqlalchemy as sa
from alembic import context, op

from config.settings import get_settings

revision = "0012_area_state_map"
down_revision = "0010_grid_state_ace"
branch_labels = None
depends_on = None

SCHEMA = get_settings().db_schema
TABLE = "area_state_map"
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


def upgrade() -> None:
    if not context.is_offline_mode() and sa.inspect(op.get_bind()).has_table(TABLE, schema=SCHEMA):
        raise RuntimeError(f"Refusing to upgrade: {SCHEMA}.{TABLE} already exists")
    op.create_table(
        TABLE,
        sa.Column("id", sa.BigInteger, sa.Identity(always=True), primary_key=True),
        sa.Column("area_code", sa.String(40), sa.ForeignKey(f"{SCHEMA}.market_area.area_code"), nullable=False,
                  comment="IEX bid area (A1 ... W3); MKT is not a geographic area and is not mapped"),
        sa.Column("state_code", sa.String(10), sa.ForeignKey(f"{SCHEMA}.state.state_code"), nullable=False),
        sa.Column("state_part", sa.Text, nullable=True,
                  comment="Published part of the state in this area (e.g. 'North Goa'); NULL = whole state"),
        sa.Column("effective_from", sa.Date, nullable=False,
                  comment="First IST delivery date covered (inclusive); see verification_status for its basis"),
        sa.Column("effective_to", sa.Date, nullable=True,
                  comment="End of validity (exclusive); NULL = still valid"),
        sa.Column("verification_status", sa.String(12), nullable=False,
                  comment="OFFICIAL: IEX/CERC/Grid-India publication; SECONDARY: third party citing the "
                          "exchange; INFERRED: derived from other sources (see notes)"),
        sa.Column("source_name", sa.Text, nullable=False, comment="Document the row rests on"),
        sa.Column("source_url", sa.Text, nullable=True),
        sa.Column("source_published_on", sa.Date, nullable=True, comment="Publication date of the source"),
        sa.Column("notes", sa.Text, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("effective_to IS NULL OR effective_to > effective_from", name="valid_period"),
        sa.CheckConstraint("verification_status IN ('OFFICIAL', 'SECONDARY', 'INFERRED')",
                           name="verification_status"),
        sa.CheckConstraint("area_code <> 'MKT'", name="not_mkt"),
        sa.CheckConstraint("state_part IS NULL OR btrim(state_part) <> ''", name="state_part_not_blank"),
        schema=SCHEMA,
        comment="Which states (or parts) each IEX bid area covers, per validity period and source",
    )
    op.create_index("uq_area_state_map_start", TABLE,
                    ["state_code", sa.text("coalesce(state_part, '')"), "effective_from"],
                    unique=True, schema=SCHEMA)
    op.create_index("uq_area_state_map_open", TABLE, ["state_code", sa.text("coalesce(state_part, '')")],
                    unique=True, schema=SCHEMA, postgresql_where=sa.text("effective_to IS NULL"))
    op.create_index("ix_area_state_map_area", TABLE, ["area_code", "effective_from"], schema=SCHEMA)
    _restrict_app_role()


def downgrade() -> None:
    if not context.is_offline_mode():
        rows = op.get_bind().execute(sa.text(f"SELECT count(*) FROM {SCHEMA}.{TABLE}")).scalar_one()
        if rows:
            raise RuntimeError(f"Refusing to downgrade: {SCHEMA}.{TABLE} holds {rows} rows.")
    op.drop_table(TABLE, schema=SCHEMA)
