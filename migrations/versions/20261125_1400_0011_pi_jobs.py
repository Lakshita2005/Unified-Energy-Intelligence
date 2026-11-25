"""Scheduler / worker job queue: schema pi_jobs.

Prepared 2026-11-25; NOT applied. Creates a separate schema for the availability-based scheduler
(src/scheduler, docs/scheduler.md). No existing table is changed: ingestion runs, documents and data
quality issues stay in ``pi``; jobs refer to their ``pi.ingestion_run`` ids.

Tables:
* ``schedule``            - configuration copied from config/scheduler.toml (``sync-config``)
* ``availability_check``  - every availability check: outcome, detected items, duration, error
* ``job``                 - the queue: status, attempts, backoff, lease, results, de-duplication key
* ``job_attempt``         - every attempt of every job (retries included)
* ``source_watermark``    - last known version and state per (schedule, item)
* ``process_heartbeat``   - liveness of pi-scheduler / pi-worker processes (health checks)

Key invariants:
* at most one ACTIVE job (PENDING / RUNNING / RETRY_WAIT) per ``dedupe_key`` (partial unique index);
* RUNNING jobs always carry a lease owner and expiry (CHECK), so a crashed worker's job is recoverable.

Privileges (least privilege; the ``pi`` schema and the database are owned by ``postgres``, the application
account cannot create schemas). Apply this migration as the schema owner (after a rolled-back dry run; see
docs/scheduler.md "Applying migration 0011"). The application role (``PI_JOBS_APP_ROLE``, default
``pi_app``) gets USAGE on the schema, use of its identity sequences, and per table:

===================  ==============================================================================
schedule             SELECT; INSERT/UPDATE on every column EXCEPT ``enabled`` (sync-config cannot
                     switch a schedule on; enabling is admin-only)
availability_check   SELECT, INSERT, UPDATE
job                  SELECT, INSERT, UPDATE
job_attempt          SELECT, INSERT (append-only history)
source_watermark     SELECT, INSERT, UPDATE
process_heartbeat    SELECT, INSERT, UPDATE
===================  ==============================================================================

No DELETE and no TRUNCATE anywhere: the application never deletes. Retention clean-up is an owner/admin
task (docs/scheduler.md "Retention"). The optional ``PI_JOBS_ADMIN_ROLE`` (a separate, existing role; unset
= only the owner administers) gets USAGE, SELECT on all tables and UPDATE (``enabled``) on ``schedule``.

Foreign keys used by retention: ``job.check_id`` and ``source_watermark.last_job_id`` are ``ON DELETE SET
NULL`` (deleting old checks / jobs keeps newer jobs and watermarks), ``job_attempt.job_id`` is ``ON DELETE
CASCADE`` (attempts are part of their job).

Chain (approved 2026-11-25): 0009_mosdac -> 0010_grid_state_ace -> 0012_area_state_map ->
0013_iex_market_snapshot -> 0011_pi_jobs. This migration only creates objects in ``pi_jobs`` and references
no table or column of ``pi`` (including those added by 0010, 0012 and 0013); its place at the end of the
chain is ordering only.

Revision ID: 0011_pi_jobs
Revises: 0013_iex_market_snapshot
Create Date: 2026-11-25
"""

from __future__ import annotations

import os
import re

import sqlalchemy as sa
from alembic import context, op
from sqlalchemy.dialects.postgresql import JSONB

revision = "0011_pi_jobs"
down_revision = "0013_iex_market_snapshot"
branch_labels = None
depends_on = None

SCHEMA = "pi_jobs"
APP_ROLE = os.environ.get("PI_JOBS_APP_ROLE", "pi_app")
ADMIN_ROLE = os.environ.get("PI_JOBS_ADMIN_ROLE", "").strip()
for _name, _role in (("PI_JOBS_APP_ROLE", APP_ROLE), ("PI_JOBS_ADMIN_ROLE", ADMIN_ROLE)):
    if _role and not re.fullmatch(r"[a-z_][a-z0-9_]*", _role):
        raise RuntimeError(f"{_name} must be a plain lower-case role name, got {_role!r}")
if ADMIN_ROLE and ADMIN_ROLE == APP_ROLE:
    raise RuntimeError("PI_JOBS_ADMIN_ROLE must differ from PI_JOBS_APP_ROLE (enabling schedules is admin-only)")

JOB_STATUSES = ("PENDING", "RUNNING", "RETRY_WAIT", "SUCCEEDED", "PARTIAL", "NOT_PUBLISHED", "FAILED", "CANCELLED")
CHECK_OUTCOMES = ("NEW_DATA", "NO_NEW_DATA", "NOT_YET_PUBLISHED", "LATE", "SOURCE_ERROR")
ERROR_CLASSES = ("TRANSIENT", "NOT_PUBLISHED", "PERMANENT")
WATERMARK_STATUSES = ("QUEUED", "INGESTED", "INGESTED_WITH_REJECTS", "NOT_PUBLISHED", "FAILED", "QUARANTINED")
TABLES = ("schedule", "availability_check", "job", "job_attempt", "source_watermark", "process_heartbeat")
# Columns of pi_jobs.schedule the application may write (sync-config); 'enabled' is deliberately absent.
SCHEDULE_APP_COLUMNS = ("schedule_key", "checker", "source_code", "dataset_codes", "lock_group", "config",
                        "synced_at")
APP_TABLE_PRIVILEGES = {
    "availability_check": "SELECT, INSERT, UPDATE",
    "job": "SELECT, INSERT, UPDATE",
    "job_attempt": "SELECT, INSERT",
    "source_watermark": "SELECT, INSERT, UPDATE",
    "process_heartbeat": "SELECT, INSERT, UPDATE",
}


def _in(column: str, values: tuple[str, ...], nullable: bool = False) -> str:
    listed = ", ".join(f"'{v}'" for v in values)
    return f"{column} IS NULL OR {column} IN ({listed})" if nullable else f"{column} IN ({listed})"


def upgrade() -> None:
    op.execute(f"CREATE SCHEMA {SCHEMA}")
    op.execute(f"COMMENT ON SCHEMA {SCHEMA} IS 'Availability-based scheduler and job queue (src/scheduler)'")
    ts = sa.TIMESTAMP(timezone=True)

    op.create_table(
        "schedule",
        sa.Column("schedule_key", sa.Text, primary_key=True),
        sa.Column("checker", sa.Text, nullable=False),
        sa.Column("enabled", sa.Boolean, nullable=False, server_default=sa.false(),
                  comment="Admin-only switch (pi_jobs enable/disable); the application role cannot update it"),
        sa.Column("source_code", sa.Text, nullable=False),
        sa.Column("dataset_codes", sa.ARRAY(sa.Text), nullable=False),
        sa.Column("lock_group", sa.Text, nullable=False),
        sa.Column("config", JSONB, nullable=False,
                  comment="Full schedule configuration as loaded from config/scheduler.toml"),
        sa.Column("synced_at", ts, nullable=False),
        sa.Column("created_at", ts, nullable=False, server_default=sa.func.now()),
        schema=SCHEMA, comment="Schedules copied from config/scheduler.toml by 'pi_jobs sync-config'")

    op.create_table(
        "availability_check",
        sa.Column("check_id", sa.BigInteger, sa.Identity(), primary_key=True),
        sa.Column("schedule_key", sa.Text, sa.ForeignKey(f"{SCHEMA}.schedule.schedule_key"), nullable=False),
        sa.Column("checked_at", ts, nullable=False),
        sa.Column("outcome", sa.Text, nullable=False),
        sa.Column("duration_ms", sa.Integer, nullable=False),
        sa.Column("detections", sa.Integer, nullable=False, server_default="0"),
        sa.Column("jobs_created", sa.Integer, nullable=False, server_default="0"),
        sa.Column("detail", JSONB, nullable=False, server_default="{}"),
        sa.Column("error", sa.Text),
        sa.Column("error_class", sa.Text),
        sa.CheckConstraint(_in("outcome", CHECK_OUTCOMES), name="ck_availability_check_outcome"),
        sa.CheckConstraint(_in("error_class", ERROR_CLASSES, nullable=True), name="ck_availability_check_error_class"),
        schema=SCHEMA, comment="One row per availability check")
    op.create_index("ix_availability_check_schedule_time", "availability_check", ["schedule_key", "checked_at"],
                    schema=SCHEMA)

    op.create_table(
        "job",
        sa.Column("job_id", sa.BigInteger, sa.Identity(), primary_key=True),
        sa.Column("schedule_key", sa.Text, sa.ForeignKey(f"{SCHEMA}.schedule.schedule_key"), nullable=False),
        sa.Column("dedupe_key", sa.Text, nullable=False, comment="schedule|item|version"),
        sa.Column("item_key", sa.Text, nullable=False),
        sa.Column("version_token", sa.Text, nullable=False),
        sa.Column("target", JSONB, nullable=False, comment="Runner and its arguments"),
        sa.Column("lock_group", sa.Text, nullable=False),
        sa.Column("priority", sa.Integer, nullable=False, server_default="100"),
        sa.Column("status", sa.Text, nullable=False),
        sa.Column("attempts", sa.Integer, nullable=False, server_default="0"),
        sa.Column("max_attempts", sa.Integer, nullable=False),
        sa.Column("backoff_base_seconds", sa.Float, nullable=False),
        sa.Column("backoff_max_seconds", sa.Float, nullable=False),
        sa.Column("next_attempt_at", ts, nullable=False),
        sa.Column("created_at", ts, nullable=False),
        sa.Column("updated_at", ts, nullable=False),
        sa.Column("started_at", ts),
        sa.Column("finished_at", ts),
        sa.Column("lease_owner", sa.Text),
        sa.Column("lease_expires_at", ts),
        sa.Column("check_id", sa.BigInteger,
                  sa.ForeignKey(f"{SCHEMA}.availability_check.check_id", ondelete="SET NULL")),
        sa.Column("ingestion_run_ids", sa.ARRAY(sa.BigInteger), nullable=False, server_default="{}",
                  comment="pi.ingestion_run ids of all attempts"),
        sa.Column("records_read", sa.BigInteger, nullable=False, server_default="0"),
        sa.Column("records_inserted", sa.BigInteger, nullable=False, server_default="0"),
        sa.Column("records_updated", sa.BigInteger, nullable=False, server_default="0"),
        sa.Column("records_unchanged", sa.BigInteger, nullable=False, server_default="0"),
        sa.Column("records_rejected", sa.BigInteger, nullable=False, server_default="0"),
        sa.Column("data_quality_issues", sa.BigInteger, nullable=False, server_default="0"),
        sa.Column("last_error", sa.Text),
        sa.Column("error_class", sa.Text),
        sa.Column("detail", JSONB, nullable=False, server_default="{}"),
        sa.Column("result", JSONB, nullable=False, server_default="{}"),
        sa.CheckConstraint(_in("status", JOB_STATUSES), name="ck_job_status"),
        sa.CheckConstraint(_in("error_class", ERROR_CLASSES, nullable=True), name="ck_job_error_class"),
        sa.CheckConstraint("attempts >= 0 AND max_attempts >= 1", name="ck_job_attempts"),
        sa.CheckConstraint("status <> 'RUNNING' OR (lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)",
                           name="ck_job_running_has_lease"),
        schema=SCHEMA, comment="Ingestion job queue")
    op.create_index("ux_job_active_dedupe", "job", ["dedupe_key"], unique=True, schema=SCHEMA,
                    postgresql_where=sa.text("status IN ('PENDING', 'RUNNING', 'RETRY_WAIT')"))
    op.create_index("ix_job_due", "job", ["status", "next_attempt_at", "priority"], schema=SCHEMA,
                    postgresql_where=sa.text("status IN ('PENDING', 'RETRY_WAIT')"))
    op.create_index("ix_job_running_group", "job", ["lock_group"], schema=SCHEMA,
                    postgresql_where=sa.text("status = 'RUNNING'"))
    op.create_index("ix_job_schedule_created", "job", ["schedule_key", "created_at"], schema=SCHEMA)

    op.create_table(
        "job_attempt",
        sa.Column("attempt_id", sa.BigInteger, sa.Identity(), primary_key=True),
        sa.Column("job_id", sa.BigInteger, sa.ForeignKey(f"{SCHEMA}.job.job_id", ondelete="CASCADE"), nullable=False),
        sa.Column("attempt_no", sa.Integer, nullable=False),
        sa.Column("worker_id", sa.Text, nullable=False),
        sa.Column("started_at", ts),
        sa.Column("finished_at", ts, nullable=False),
        sa.Column("status", sa.Text, nullable=False),
        sa.Column("error_class", sa.Text),
        sa.Column("error", sa.Text),
        sa.Column("ingestion_run_ids", sa.ARRAY(sa.BigInteger), nullable=False, server_default="{}"),
        sa.Column("records_read", sa.BigInteger, nullable=False, server_default="0"),
        sa.Column("records_inserted", sa.BigInteger, nullable=False, server_default="0"),
        sa.Column("records_updated", sa.BigInteger, nullable=False, server_default="0"),
        sa.Column("records_unchanged", sa.BigInteger, nullable=False, server_default="0"),
        sa.Column("records_rejected", sa.BigInteger, nullable=False, server_default="0"),
        sa.Column("data_quality_issues", sa.BigInteger, nullable=False, server_default="0"),
        sa.Column("detail", JSONB, nullable=False, server_default="{}"),
        sa.CheckConstraint(_in("error_class", ERROR_CLASSES, nullable=True), name="ck_job_attempt_error_class"),
        schema=SCHEMA, comment="Every attempt of every job")
    op.create_index("ix_job_attempt_job", "job_attempt", ["job_id", "attempt_no"], schema=SCHEMA)

    op.create_table(
        "source_watermark",
        sa.Column("schedule_key", sa.Text, sa.ForeignKey(f"{SCHEMA}.schedule.schedule_key"), primary_key=True),
        sa.Column("item_key", sa.Text, primary_key=True),
        sa.Column("version_token", sa.Text, nullable=False),
        sa.Column("status", sa.Text, nullable=False),
        sa.Column("last_job_id", sa.BigInteger, sa.ForeignKey(f"{SCHEMA}.job.job_id", ondelete="SET NULL")),
        sa.Column("first_seen_at", ts, nullable=False),
        sa.Column("updated_at", ts, nullable=False),
        sa.CheckConstraint(_in("status", WATERMARK_STATUSES), name="ck_source_watermark_status"),
        schema=SCHEMA, comment="Last known version and state of each detected item")

    op.create_table(
        "process_heartbeat",
        sa.Column("process_id", sa.Text, primary_key=True),
        sa.Column("process_kind", sa.Text, nullable=False),
        sa.Column("host", sa.Text, nullable=False),
        sa.Column("pid", sa.Integer, nullable=False),
        sa.Column("started_at", ts, nullable=False),
        sa.Column("last_seen_at", ts, nullable=False),
        sa.Column("detail", JSONB, nullable=False, server_default="{}"),
        sa.CheckConstraint("process_kind IN ('scheduler', 'worker')", name="ck_process_heartbeat_kind"),
        schema=SCHEMA, comment="Liveness of pi-scheduler and pi-worker processes")

    for statement in grant_statements(APP_ROLE, ADMIN_ROLE or None):
        op.execute(statement)


def grant_statements(app_role: str, admin_role: str | None) -> list[str]:
    """The GRANTs of this migration (a function so tests can check them without a database)."""
    columns = ", ".join(SCHEDULE_APP_COLUMNS)
    statements = [
        f"GRANT USAGE ON SCHEMA {SCHEMA} TO {app_role}",
        f"GRANT SELECT ON {SCHEMA}.schedule TO {app_role}",
        f"GRANT INSERT ({columns}), UPDATE ({columns}) ON {SCHEMA}.schedule TO {app_role}",
        *(f"GRANT {privileges} ON {SCHEMA}.{table} TO {app_role}"
          for table, privileges in APP_TABLE_PRIVILEGES.items()),
        f"GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA {SCHEMA} TO {app_role}",
    ]
    if admin_role:
        statements += [
            f"GRANT USAGE ON SCHEMA {SCHEMA} TO {admin_role}",
            f"GRANT SELECT ON ALL TABLES IN SCHEMA {SCHEMA} TO {admin_role}",
            f"GRANT UPDATE (enabled) ON {SCHEMA}.schedule TO {admin_role}",
        ]
    return statements


def downgrade() -> None:
    if not context.is_offline_mode():
        jobs = op.get_bind().execute(sa.text(f"SELECT count(*) FROM {SCHEMA}.job")).scalar_one()
        if jobs and os.environ.get("PI_JOBS_ALLOW_DROP") != "yes":
            raise RuntimeError(f"Refusing to downgrade: {SCHEMA}.job holds {jobs} job(s) of history. "
                               "Set PI_JOBS_ALLOW_DROP=yes to drop it deliberately.")
    for table in reversed(TABLES):
        op.drop_table(table, schema=SCHEMA)
    op.execute(f"DROP SCHEMA {SCHEMA}")
