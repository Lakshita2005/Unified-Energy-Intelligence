"""Migration 0011_pi_jobs (static, offline SQL only: no database connection) and the offline CLI."""

from __future__ import annotations

import io

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory

from config.settings import PROJECT_ROOT


def _offline_sql(revision_range: str, *, downgrade: bool = False) -> str:
    cfg = Config(str(PROJECT_ROOT / "alembic.ini"))
    buffer = io.StringIO()
    cfg.output_buffer = buffer
    (command.downgrade if downgrade else command.upgrade)(cfg, revision_range, sql=True)
    return buffer.getvalue()


APPROVED_CHAIN = ["0009_mosdac", "0010_grid_state_ace", "0012_area_state_map", "0013_iex_market_snapshot",
                  "0011_pi_jobs"]


def test_chain_is_linear_in_the_approved_order() -> None:
    scripts = ScriptDirectory(str(PROJECT_ROOT / "migrations"))
    heads = scripts.get_heads()
    assert len(heads) == 1                                   # exactly one head, no branch
    chain = [r.revision for r in scripts.iterate_revisions(heads[0], "base")][::-1]   # base -> head
    position = chain.index("0009_mosdac")
    assert chain[position:position + len(APPROVED_CHAIN)] == APPROVED_CHAIN
    assert all(len(scripts.get_revision(r).nextrev) <= 1 for r in chain)             # no branch point
    assert all(len(r) <= 32 for r in chain)                  # alembic_version.version_num is varchar(32)


def test_0011_uses_nothing_from_the_pi_schema() -> None:
    source = (PROJECT_ROOT / "migrations" / "versions" / "20261125_1400_0011_pi_jobs.py").read_text(encoding="utf-8")
    code = source.split('"""', 2)[2]                          # without the module docstring
    assert "get_settings" not in code and "db_schema" not in code
    foreign_keys = [line for line in code.splitlines() if "ForeignKey(" in line]
    assert foreign_keys and all('f"{SCHEMA}.' in line for line in foreign_keys)       # SCHEMA = "pi_jobs"


def test_whole_chain_renders_offline_both_ways() -> None:
    try:
        up = _offline_sql("0009_mosdac:head")
        down = _offline_sql("head:0009_mosdac", downgrade=True)
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"offline rendering not possible here: {exc}")
    order = [line.split("version_num='")[1].split("'")[0] for line in up.splitlines()
             if line.startswith("UPDATE alembic_version SET version_num=")]
    assert order == APPROVED_CHAIN[1:]
    assert "DROP TABLE pi.iex_market_snapshot" in down and "DROP TABLE pi.area_state_map" in down


def test_new_iex_tables_end_with_select_and_insert_only() -> None:
    try:
        sql = _offline_sql("0010_grid_state_ace:0013_iex_market_snapshot")
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"offline rendering not possible here: {exc}")
    for table in ("area_state_map", "iex_market_snapshot"):
        assert f"GRANT SELECT, INSERT ON pi.{table} TO pi_app" in sql
        assert f"REVOKE UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON pi.{table} FROM pi_app" in sql
    assert "DEFAULT PRIVILEGES" not in sql                    # the schema's default privileges are untouched


def test_0011_offline_sql_creates_the_jobs_schema_only() -> None:
    try:
        sql = _offline_sql("0013_iex_market_snapshot:0011_pi_jobs")
    except Exception as exc:  # pragma: no cover - env.py needs DB_* settings to render the URL
        pytest.skip(f"offline rendering not possible here: {exc}")
    assert "CREATE SCHEMA pi_jobs" in sql
    for table in ("schedule", "availability_check", "job", "job_attempt", "source_watermark", "process_heartbeat"):
        assert f"CREATE TABLE pi_jobs.{table}" in sql
    assert "CREATE UNIQUE INDEX ux_job_active_dedupe ON pi_jobs.job (dedupe_key) WHERE status IN " in sql
    assert "GRANT USAGE ON SCHEMA pi_jobs TO pi_app" in sql
    assert "GRANT SELECT ON pi_jobs.schedule TO pi_app" in sql
    assert "GRANT SELECT, INSERT ON pi_jobs.job_attempt TO pi_app" in sql
    # least privilege: no DELETE / TRUNCATE for the application, no table-wide grant
    assert "DELETE" not in sql.split("CREATE SCHEMA pi_jobs", 1)[1].replace("ON DELETE", "")
    assert "TRUNCATE" not in sql and "ON ALL TABLES" not in sql
    # retention-friendly foreign keys
    assert "REFERENCES pi_jobs.availability_check (check_id) ON DELETE SET NULL" in sql
    assert "REFERENCES pi_jobs.job (job_id) ON DELETE SET NULL" in sql
    assert "REFERENCES pi_jobs.job (job_id) ON DELETE CASCADE" in sql
    # nothing outside pi_jobs is altered
    statements = [s.strip() for s in sql.split(";") if s.strip() and not s.strip().startswith("--")]
    touching = [s for s in statements if s.upper().startswith(("ALTER", "DROP", "UPDATE", "DELETE", "INSERT"))
                and "alembic_version" not in s]
    assert all("pi_jobs." in s for s in touching), touching


def test_0011_offline_downgrade_drops_only_pi_jobs() -> None:
    try:
        sql = _offline_sql("0011_pi_jobs:0013_iex_market_snapshot", downgrade=True)
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"offline rendering not possible here: {exc}")
    drops = [line for line in sql.splitlines() if line.startswith("DROP")]
    assert drops and all("pi_jobs" in line for line in drops)


def _migration_module():
    import importlib.util

    path = PROJECT_ROOT / "migrations" / "versions" / "20261125_1400_0011_pi_jobs.py"
    spec = importlib.util.spec_from_file_location("migration_0011", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_0011_app_role_cannot_enable_schedules_or_delete() -> None:
    m = _migration_module()
    statements = m.grant_statements("pi_app", None)
    joined = "\n".join(statements)
    assert "enabled" not in m.SCHEDULE_APP_COLUMNS
    schedule_writes = [s for s in statements if "pi_jobs.schedule" in s and ("INSERT" in s or "UPDATE" in s)]
    assert schedule_writes == [
        "GRANT INSERT (schedule_key, checker, source_code, dataset_codes, lock_group, config, synced_at), "
        "UPDATE (schedule_key, checker, source_code, dataset_codes, lock_group, config, synced_at) "
        "ON pi_jobs.schedule TO pi_app"]
    assert "DELETE" not in joined and "TRUNCATE" not in joined and "ALL TABLES" not in joined
    assert "GRANT SELECT, INSERT ON pi_jobs.job_attempt TO pi_app" in statements   # append-only history


def test_0011_optional_admin_role_gets_only_the_enable_switch() -> None:
    m = _migration_module()
    admin = [s for s in m.grant_statements("pi_app", "pi_admin") if "pi_admin" in s]
    assert admin == ["GRANT USAGE ON SCHEMA pi_jobs TO pi_admin",
                     "GRANT SELECT ON ALL TABLES IN SCHEMA pi_jobs TO pi_admin",
                     "GRANT UPDATE (enabled) ON pi_jobs.schedule TO pi_admin"]


def test_validate_config_command_runs_offline(capsys) -> None:
    from scripts.pi_jobs import main
    from tests.support.scheduler import settings

    assert main(["validate-config"], settings=settings(scheduler_global_enable=False)) == 0
    out = capsys.readouterr().out
    assert "13 schedule(s), 7 enabled" in out and "SCHEDULER_GLOBAL_ENABLE=False" in out   # 7 approved 2026-11-26
