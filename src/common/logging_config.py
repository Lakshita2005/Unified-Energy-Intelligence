"""Centralised logging configuration.

Call :func:`setup_logging` once at process start-up (entry points, scripts,
Alembic). Everywhere else, obtain a module logger with :func:`get_logger`::

    from src.common.logging_config import get_logger

    logger = get_logger(__name__)

Output goes to stdout and, optionally, to a size-rotated file under
``LOG_DIR``. ``LOG_FORMAT=json`` emits one JSON object per line, suitable for
log aggregators.

Note: ``RotatingFileHandler`` is not safe for several processes writing the
same file. If you run multiple workers, give each its own ``LOG_FILE_NAME``.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from typing import TextIO

from config.settings import Settings, get_settings

TEXT_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S%z"

# Third-party loggers that are noisy at INFO. httpx in particular logs full
# request URLs at INFO, which may include API keys in query strings.
_QUIETED_LOGGERS: tuple[str, ...] = ("httpx", "httpcore", "urllib3")

# Attribute set on handlers we install so they can be identified and removed.
_MANAGED_HANDLER_ATTR = "_price_intelligence_managed"

# Attributes present on every LogRecord; anything else came from ``extra=``.
_STANDARD_RECORD_ATTRS: frozenset[str] = frozenset(
    vars(logging.makeLogRecord({})).keys() | {"message", "asctime", "taskName"}
)

_configured = False


class JsonFormatter(logging.Formatter):
    """Format log records as single-line JSON objects.

    Fields passed through ``logger.info(..., extra={...})`` are included under
    the ``extra`` key.
    """

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "line": record.lineno,
        }
        extras = {
            key: value
            for key, value in vars(record).items()
            if key not in _STANDARD_RECORD_ATTRS and not key.startswith("_")
        }
        if extras:
            payload["extra"] = extras
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, ensure_ascii=False)


def _build_formatter(settings: Settings) -> logging.Formatter:
    if settings.log_format == "json":
        return JsonFormatter()
    return logging.Formatter(TEXT_FORMAT, DATE_FORMAT)


def _remove_managed_handlers(logger: logging.Logger) -> None:
    for handler in list(logger.handlers):
        if getattr(handler, _MANAGED_HANDLER_ATTR, False):
            logger.removeHandler(handler)
            handler.close()


def setup_logging(
    settings: Settings | None = None,
    *,
    level: str | None = None,
    stream: TextIO | None = None,
    force: bool = False,
) -> None:
    """Configure the root logger for the application.

    Safe to call multiple times: subsequent calls are no-ops unless ``force``
    is true, in which case previously installed handlers are replaced.

    Args:
        settings: Settings to use; defaults to :func:`config.settings.get_settings`.
        level: Override for ``LOG_LEVEL`` (e.g. ``"DEBUG"``).
        stream: Console stream (default stdout). Pass ``sys.stderr`` when stdout
            carries program output such as JSON.
        force: Reconfigure even if logging was already set up.
    """
    global _configured
    if _configured and not force:
        return

    settings = settings or get_settings()
    resolved_level = (level or settings.log_level).upper()
    formatter = _build_formatter(settings)

    root = logging.getLogger()
    _remove_managed_handlers(root)
    root.setLevel(resolved_level)

    console_handler = logging.StreamHandler(stream or sys.stdout)
    console_handler.setFormatter(formatter)
    setattr(console_handler, _MANAGED_HANDLER_ATTR, True)
    root.addHandler(console_handler)

    if settings.log_to_file:
        settings.log_dir.mkdir(parents=True, exist_ok=True)
        file_handler = RotatingFileHandler(
            settings.log_dir / settings.log_file_name,
            maxBytes=settings.log_max_bytes,
            backupCount=settings.log_backup_count,
            encoding="utf-8",
            delay=True,
        )
        file_handler.setFormatter(formatter)
        setattr(file_handler, _MANAGED_HANDLER_ATTR, True)
        root.addHandler(file_handler)

    quiet_level = max(logging.WARNING, logging.getLevelName(resolved_level))
    for name in _QUIETED_LOGGERS:
        logging.getLogger(name).setLevel(quiet_level)

    logging.captureWarnings(True)
    _configured = True


def reset_logging() -> None:
    """Remove and close handlers installed by :func:`setup_logging`.

    Mainly useful in tests, and before process exit on Windows so log files
    are released.
    """
    global _configured
    _remove_managed_handlers(logging.getLogger())
    _configured = False


def get_logger(name: str) -> logging.Logger:
    """Return a named logger. Pass ``__name__`` from the calling module."""
    return logging.getLogger(name)
