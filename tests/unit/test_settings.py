"""Tests for config.settings."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from config.settings import PROJECT_ROOT, Settings


def test_environment_variables_override_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DB_HOST", "db.internal")
    monkeypatch.setenv("HTTP_MAX_RETRIES", "7")

    settings = Settings(_env_file=None)

    assert settings.db_host == "db.internal"
    assert settings.http_max_retries == 7


def test_relative_paths_resolve_from_project_root() -> None:
    settings = Settings(_env_file=None, raw_data_dir="custom_raw", log_dir="custom_logs")

    assert settings.raw_data_dir == (PROJECT_ROOT / "custom_raw").resolve()
    assert settings.log_dir == (PROJECT_ROOT / "custom_logs").resolve()


def test_blank_credentials_are_treated_as_unset() -> None:
    settings = Settings(_env_file=None, db_host="", db_user="  ", db_password="")

    assert settings.db_host is None
    assert settings.db_user is None
    assert settings.db_password is None


def test_whitespace_is_stripped_except_from_password() -> None:
    settings = Settings(_env_file=None, db_name="price_intelligence_dev ", db_password=" pw ")

    assert settings.db_name == "price_intelligence_dev"
    assert settings.db_password is not None and settings.db_password.get_secret_value() == " pw "


def test_db_schema_defaults_to_pi_and_is_validated() -> None:
    assert Settings(_env_file=None).db_schema == "pi"
    assert Settings(_env_file=None, db_schema="staging").db_schema == "staging"
    with pytest.raises(ValidationError):
        Settings(_env_file=None, db_schema="pi; DROP SCHEMA pi")


def test_log_level_is_normalised_and_validated() -> None:
    assert Settings(_env_file=None, log_level="debug").log_level == "DEBUG"
    with pytest.raises(ValidationError):
        Settings(_env_file=None, log_level="verbose")


def test_password_is_not_exposed_in_repr() -> None:
    settings = Settings(_env_file=None, db_password="s3cret")

    assert "s3cret" not in repr(settings)
