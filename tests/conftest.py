"""Shared pytest fixtures."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from config.settings import Settings
from src.common.logging_config import reset_logging

SettingsFactory = Callable[..., Settings]


@pytest.fixture
def make_settings(tmp_path: Path) -> SettingsFactory:
    """Build isolated Settings that ignore the developer's .env file.

    Paths default to ``tmp_path`` so tests never touch the real logs/ or raw_data/.
    """

    def _factory(**overrides: Any) -> Settings:
        values: dict[str, Any] = {
            "log_dir": tmp_path / "logs",
            "raw_data_dir": tmp_path / "raw_data",
            "log_to_file": False,
        }
        values.update(overrides)
        return Settings(_env_file=None, **values)

    return _factory


@pytest.fixture(autouse=True)
def _reset_logging() -> Iterator[None]:
    """Ensure each test starts and ends without our logging handlers installed."""
    reset_logging()
    yield
    reset_logging()
