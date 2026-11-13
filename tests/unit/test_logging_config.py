"""Tests for src.common.logging_config."""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

from src.common.logging_config import JsonFormatter, get_logger, reset_logging, setup_logging
from tests.conftest import SettingsFactory


def _managed_handlers() -> list[logging.Handler]:
    return [h for h in logging.getLogger().handlers if getattr(h, "_price_intelligence_managed", False)]


def test_setup_is_idempotent(make_settings: SettingsFactory) -> None:
    settings = make_settings()

    setup_logging(settings)
    setup_logging(settings)

    assert len(_managed_handlers()) == 1


def test_force_replaces_handlers(make_settings: SettingsFactory) -> None:
    setup_logging(make_settings())
    setup_logging(make_settings(log_to_file=True), force=True)

    assert len(_managed_handlers()) == 2


def test_writes_to_log_file(make_settings: SettingsFactory, tmp_path: Path) -> None:
    settings = make_settings(log_to_file=True, log_dir=tmp_path / "logs", log_file_name="app.log")
    setup_logging(settings)

    get_logger("tests.file").info("hello file")
    reset_logging()  # flush and release the file (required on Windows)

    assert "hello file" in (tmp_path / "logs" / "app.log").read_text(encoding="utf-8")


def test_httpx_logger_is_quieted(make_settings: SettingsFactory) -> None:
    setup_logging(make_settings(log_level="DEBUG"))

    assert logging.getLogger("httpx").level == logging.WARNING


def test_json_formatter_includes_extra_and_exception() -> None:
    logger = logging.getLogger("tests.json")
    try:
        raise ValueError("bad")
    except ValueError:
        record = logger.makeRecord(
            "tests.json", logging.ERROR, __file__, 1, "failed %s", ("job",),
            exc_info=sys.exc_info(), extra={"source": "example"},
        )

    payload = json.loads(JsonFormatter().format(record))

    assert payload["message"] == "failed job"
    assert payload["level"] == "ERROR"
    assert payload["extra"] == {"source": "example"}
    assert "ValueError: bad" in payload["exception"]
