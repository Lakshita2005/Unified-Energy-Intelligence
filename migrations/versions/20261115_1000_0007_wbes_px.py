"""WBES PX Dashboard: region, link and entity schedule tables.

Approved 2026-11-15 (WBES milestone 1, PX Dashboard only; decisions W1-W10).
One WBES ``GetPowerExchangeByDetails`` response (delivery date x power
exchange x market, every revision returned) is stored once in raw storage and
parsed into three tables, one dataset each:

* ``wbes_px_region_schedule``  (``WBES_PX_REGION``): injection / drawal per WBES region and block;
* ``wbes_px_link_schedule``    (``WBES_PX_LINK``):   net amount per link and block;
* ``wbes_px_entity_schedule``  (``WBES_PX_ENTITY``): injection / drawal per entity and block.

Design constraints from the approval:

* Units are not verified (W6): value columns are neutral (``*_value``), stored
  as unconstrained ``numeric`` so the published digits are kept exactly.
* WBES region ids are preserved (``wbes_region_id``); ``region_code`` is set
  only for verified mappings and is NULL for NLDC.
* Revision number, power-exchange id and CreatedOn are lineage (W4): they are
  stored on every row but do not make a row "changed".
* No unique constraint: uniqueness of the PX entity key is unverified (W7);
  the loader enforces natural keys, and the lookup indexes are non-unique.

Additive: three new tables and their indexes; no existing object is changed.
The upgrade refuses if a table already exists; the downgrade refuses if any of
the three tables holds rows.

Revision ID: 0007_wbes_px
Revises: 0006_grid_scada_instant_comment
Create Date: 2026-11-15
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import context, op

from config.settings import get_settings

revision = "0007_wbes_px"
down_revision = "0006_grid_scada_instant_comment"
branch_labels = None
depends_on = None

SCHEMA = get_settings().db_schema
TABLES = ("wbes_px_region_schedule", "wbes_px_link_schedule", "wbes_px_entity_schedule")
UNIT_NOTE = "unit not yet verified (W6); value as published"


def _common_columns() -> list[sa.Column]:
    return [
        sa.Column("id", sa.BigInteger, sa.Identity(always=True), primary_key=True),
        sa.Column("delivery_date", sa.Date, nullable=False, comment="WBES EffectiveForDate (IST delivery day)"),
        sa.Column("power_exchange", sa.String(20), nullable=False, comment="Power exchange name as published (e.g. IEX, PXI, HPX)"),
        sa.Column("power_exchange_type_id", sa.Integer, comment="WBES PowerExchangeTypeId (lineage)"),
        sa.Column("market", sa.String(30), nullable=False, comment="Market (transaction type) name as published (e.g. DAM, RTM)"),
        sa.Column("market_type_id", sa.Integer, comment="WBES PowerExTransactionTypeId (lineage)"),
        sa.Column("time_block", sa.SmallInteger, nullable=False, comment="15-minute block 1-96 (position in the published array)"),
    ]


def _lineage_columns() -> list[sa.Column]:
    return [
        sa.Column("revision_no", sa.Integer, nullable=False, comment="WBES RevisionNo of the record this value came from (lineage)"),
        sa.Column("power_ex_id", sa.String(64), comment="WBES PowerExId of that record (lineage)"),
        sa.Column("retrieved_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source_published_at", sa.DateTime(timezone=True),
                  comment="WBES CreatedOn of the revision, parsed (original kept in raw storage); NULL if not parseable"),
        sa.Column("source_available_at", sa.DateTime(timezone=True), nullable=False,
                  comment="When the value may be used without look-ahead"),
        sa.Column("availability_basis", sa.String(30), nullable=False,
                  comment="SOURCE_CREATED_ON (= source_published_at) or RETRIEVED_AT (= retrieved_at)"),
        sa.Column("ingestion_run_id", sa.BigInteger, sa.ForeignKey(f"{SCHEMA}.ingestion_run.ingestion_run_id"),
                  nullable=False),
        sa.Column("document_id", sa.BigInteger, sa.ForeignKey(f"{SCHEMA}.source_document.document_id")),
        sa.CheckConstraint("time_block BETWEEN 1 AND 96", name="time_block"),
        sa.CheckConstraint("availability_basis IN ('SOURCE_CREATED_ON', 'RETRIEVED_AT')", name="availability_basis"),
        sa.CheckConstraint(
            "(availability_basis = 'SOURCE_CREATED_ON' AND source_published_at IS NOT NULL "
            "AND source_available_at = source_published_at) OR "
            "(availability_basis = 'RETRIEVED_AT' AND source_available_at = retrieved_at)",
            name="availability_consistent"),
    ]


def _region_columns(*, required: bool) -> list[sa.Column]:
    return [
        sa.Column("wbes_region_id", sa.SmallInteger, nullable=not required, comment="WBES region id as published"),
        sa.Column("region_code", sa.String(10), sa.ForeignKey(f"{SCHEMA}.region.region_code"),
                  comment="PI region, only where the WBES id mapping is verified; NULL for NLDC"),
    ]


def upgrade() -> None:
    if not context.is_offline_mode():
        existing = [t for t in TABLES if sa.inspect(op.get_bind()).has_table(t, schema=SCHEMA)]
        if existing:
            raise RuntimeError(f"Refusing to upgrade: tables already exist: {existing}")

    op.create_table(
        "wbes_px_region_schedule",
        *_common_columns(),
        *_region_columns(required=True),
        sa.Column("injection_value", sa.Numeric, comment=f"Region injection; {UNIT_NOTE}"),
        sa.Column("drawal_value", sa.Numeric, comment=f"Region drawal; {UNIT_NOTE}"),
        *_lineage_columns(),
        schema=SCHEMA,
        comment="WBES PX Dashboard: region-wise injection/drawal per exchange, market and block (dataset WBES_PX_REGION)",
    )
    op.create_table(
        "wbes_px_link_schedule",
        *_common_columns(),
        sa.Column("link_number", sa.Integer, comment="WBES LinkNumber as published"),
        sa.Column("link_name", sa.Text, nullable=False, comment="WBES LinkName as published"),
        sa.Column("net_amount_value", sa.Numeric, comment=f"Link net amount; sign convention and {UNIT_NOTE}"),
        *_lineage_columns(),
        schema=SCHEMA,
        comment="WBES PX Dashboard: link-wise net amount per exchange, market and block (dataset WBES_PX_LINK)",
    )
    op.create_table(
        "wbes_px_entity_schedule",
        *_common_columns(),
        sa.Column("entity_acronym", sa.Text, nullable=False, comment="WBES EntityAcronym as published"),
        sa.Column("entity_unit_acronym", sa.Text, comment="WBES EntityUnitAcronym as published"),
        sa.Column("entity_px_code", sa.Text, comment="WBES EntityPxCode as published"),
        *_region_columns(required=False),
        sa.Column("injection_value", sa.Numeric, comment=f"Entity injection; {UNIT_NOTE}"),
        sa.Column("drawal_value", sa.Numeric, comment=f"Entity drawal; {UNIT_NOTE}"),
        *_lineage_columns(),
        schema=SCHEMA,
        comment="WBES PX Dashboard: entity-wise injection/drawal per exchange, market and block (dataset WBES_PX_ENTITY)",
    )
    # Non-unique lookup indexes on the natural keys (uniqueness is enforced by the loader; W7).
    op.create_index("ix_wbes_px_region_key", "wbes_px_region_schedule",
                    ["delivery_date", "power_exchange", "market", "wbes_region_id", "time_block"], schema=SCHEMA)
    op.create_index("ix_wbes_px_link_key", "wbes_px_link_schedule",
                    ["delivery_date", "power_exchange", "market", "link_name", "time_block"], schema=SCHEMA)
    op.create_index("ix_wbes_px_entity_key", "wbes_px_entity_schedule",
                    ["delivery_date", "power_exchange", "market", "entity_acronym", "entity_unit_acronym",
                     "entity_px_code", "time_block"], schema=SCHEMA)
    for table in TABLES:
        op.create_index(f"ix_{table}_document", table, ["document_id"], schema=SCHEMA)


def downgrade() -> None:
    if not context.is_offline_mode():
        bind = op.get_bind()
        counts = {t: bind.execute(sa.text(f"SELECT count(*) FROM {SCHEMA}.{t}")).scalar_one() for t in TABLES}
        non_empty = {t: n for t, n in counts.items() if n}
        if non_empty:
            raise RuntimeError(f"Refusing to downgrade: WBES PX tables hold rows {non_empty}.")
    for table in reversed(TABLES):
        op.drop_table(table, schema=SCHEMA)
