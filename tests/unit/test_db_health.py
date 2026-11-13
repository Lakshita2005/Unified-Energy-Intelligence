"""Unit tests for src.common.db_health and scripts.db_health_check (no live database)."""

from __future__ import annotations

import json
import re

import pytest

from config.expected_schema import EXPECTED_TABLES
from scripts.db_health_check import EXIT_CONFIG_ERROR, EXIT_UNHEALTHY, format_report, main
from src.common.database import DatabaseManager
from src.common.db_health import (
    ConnectionInfo,
    HealthReport,
    TableVerification,
    run_health_check,
    verify_tables,
)
from tests.conftest import SettingsFactory

# Nothing listens on port 1, so connections are refused immediately.
UNREACHABLE_URL = "postgresql+psycopg://nobody:not-a-real-secret@127.0.0.1:1/nowhere"


def _healthy_report() -> HealthReport:
    return HealthReport(
        schema="pi",
        connected=True,
        connection=ConnectionInfo("price_intelligence_dev", "postgres", "18.4", "127.0.0.1/32", 5432),
        schema_exists=True,
        schema_usable=True,
        tables={"plant": 10, "state": 3},
        table_verification=verify_tables(["plant", "state"], ["plant", "state"]),
    )


def test_verify_tables_reports_missing_and_unexpected() -> None:
    result = verify_tables(actual=["a", "b", "extra"], expected=["a", "b", "c"])

    assert result == TableVerification(present=("a", "b"), missing=("c",), unexpected=("extra",))
    assert result.ok is False


def test_verify_tables_allows_extra_tables() -> None:
    assert verify_tables(actual=["a", "b", "extra"], expected=["a", "b"]).ok is True


def test_report_ok_requires_every_check() -> None:
    report = _healthy_report()
    assert report.ok is True

    report.errors.append("Connected as user 'x', expected 'y'")
    assert report.ok is False

    missing = _healthy_report()
    missing.table_verification = verify_tables(["plant"], ["plant", "state"])
    assert missing.ok is False

    assert HealthReport(schema="pi").ok is False


def test_report_to_dict_is_json_serialisable() -> None:
    data = json.loads(json.dumps(_healthy_report().to_dict()))

    assert data["ok"] is True
    assert data["table_verification"]["ok"] is True
    assert data["connection"]["database"] == "price_intelligence_dev"


def test_format_report_lists_missing_tables() -> None:
    report = _healthy_report()
    report.table_verification = verify_tables(["plant"], ["plant", "state"])

    text = format_report(report)

    assert "MISSING: pi.state" in text
    assert "Overall: UNHEALTHY" in text


def test_health_check_reports_connection_failure_without_raising() -> None:
    manager = DatabaseManager(UNREACHABLE_URL, connect_args={"connect_timeout": 2})
    try:
        report = run_health_check(manager, schema="pi", expected_tables=["plant"])
    finally:
        manager.dispose()

    assert report.connected is False
    assert report.ok is False
    assert report.errors and "OperationalError" in report.errors[0]
    assert "not-a-real-secret" not in json.dumps(report.to_dict())


def test_script_returns_config_error_when_credentials_missing(make_settings: SettingsFactory) -> None:
    settings = make_settings(db_host=None, db_name=None, db_user=None)

    assert main([], settings=settings) == EXIT_CONFIG_ERROR


def test_script_unhealthy_output_never_contains_password(
    make_settings: SettingsFactory, capsys: pytest.CaptureFixture[str]
) -> None:
    settings = make_settings(
        db_host="127.0.0.1", db_port=1, db_name="nowhere", db_user="nobody",
        db_password="Sup3r$ecret!", db_connect_timeout_seconds=2,
    )

    exit_code = main(["--json"], settings=settings)

    captured = capsys.readouterr()
    leaked = "Sup3r$ecret!" in captured.out + captured.err
    assert exit_code == EXIT_UNHEALTHY
    assert not leaked, "password appeared in health check output"
    assert json.loads(captured.out)["ok"] is False


def test_expected_tables_are_valid_identifiers() -> None:
    assert EXPECTED_TABLES
    assert all(re.fullmatch(r"[a-z_][a-z0-9_]*", name) for name in EXPECTED_TABLES)
