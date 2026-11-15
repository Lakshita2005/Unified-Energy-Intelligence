"""WBES CLI commands that need neither the network nor the database."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path

import pytest

from config.settings import Settings
from scripts.ingest_wbes import main
from src.pipelines.wbes.session import SessionTokens, TokenStore
from tests.support.wbes import UTILITY, make_jwt


def _settings(tmp_path: Path) -> Settings:
    return Settings(raw_data_dir=tmp_path / "raw", log_dir=tmp_path / "logs", log_to_file=False,
                    wbes_session_file=tmp_path / "private" / "session.json")


def test_session_status_never_prints_tokens(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    settings = _settings(tmp_path)
    assert main(["session"], settings=settings) == 3
    assert "No WBES session" in capsys.readouterr().out
    access = make_jwt(time.time() + 600)
    TokenStore(settings.wbes_session_file).save(
        SessionTokens(access, "very-secret-refresh", UTILITY, datetime(2026, 11, 14, tzinfo=timezone.utc)))
    assert main(["session"], settings=settings) == 0
    out = capsys.readouterr().out
    assert UTILITY in out and "valid" in out and access not in out and "very-secret-refresh" not in out
    assert main(["session", "--delete"], settings=settings) == 0
    assert not settings.wbes_session_file.exists()


def test_session_file_in_forbidden_location_is_a_configuration_error(tmp_path: Path) -> None:
    settings = Settings(raw_data_dir=tmp_path / "raw", log_to_file=False,
                        wbes_session_file=tmp_path / "raw" / "session.json")
    assert main(["session"], settings=settings) == 2


def test_px_argument_validation(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    with pytest.raises(SystemExit):
        main(["px", "--date", "2026-11-10", "--end-date", "2026-11-11"], settings=settings)
    with pytest.raises(SystemExit):
        main(["px", "--start-date", "2026-11-10"], settings=settings)
    assert main(["px", "--date", "2026-11-10", "--parts", "region,prices"], settings=settings) == 2
