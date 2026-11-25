"""Process plumbing shared by pi-scheduler and pi-worker: graceful stop and logging.

Stop requests come from Ctrl+C / Ctrl+Break (what Windows service wrappers such as NSSM and WinSW send on
"stop"), SIGTERM, or an optional stop file (for hosts that cannot send a console signal, e.g. Task
Scheduler). The processes finish their current unit of work and exit; anything interrupted harder is
recovered through job leases.
"""

from __future__ import annotations

import signal
import threading
import time
from pathlib import Path

from config.settings import Settings
from src.common.logging_config import get_logger, setup_logging

logger = get_logger(__name__)

WAIT_SLICE_SECONDS = 1.0

# Process exit codes (service wrappers restart on 1, not on 2; see docs/scheduler.md).
EXIT_OK = 0                 # normal stop
EXIT_RUNTIME_FAILURE = 1    # e.g. scheduler lock lost / already held: a restart is appropriate
EXIT_CONFIGURATION = 2      # configuration error (missing migration, invalid config): do NOT restart


class StopSignal:
    """A stop flag that can also be raised by a file appearing."""

    def __init__(self, stop_file: Path | None = None) -> None:
        self._event = threading.Event()
        self.stop_file = stop_file
        self.reason: str | None = None

    def request(self, reason: str) -> None:
        if not self._event.is_set():
            self.reason = reason
            logger.info("Stop requested (%s): finishing current work", reason)
        self._event.set()

    def is_set(self) -> bool:
        if not self._event.is_set() and self.stop_file is not None and self.stop_file.exists():
            self.request(f"stop file {self.stop_file}")
        return self._event.is_set()

    def wait(self, seconds: float) -> bool:
        """Sleep up to ``seconds``; True if a stop was requested meanwhile.

        Always waits in slices of at most :data:`WAIT_SLICE_SECONDS`. On Windows a console Ctrl+C / Ctrl+Break
        is delivered to Python only when the main thread returns from a blocking wait, so a single long
        wait would delay a service stop by up to the whole interval (measured: 28-30 s for a 30 s wait).
        """
        deadline = time.monotonic() + max(0.0, seconds)
        while True:
            if self.is_set():
                return True
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return self.is_set()
            if self._event.wait(min(WAIT_SLICE_SECONDS, remaining)):
                return True


def install_signal_handlers(stop: StopSignal) -> None:
    """Map SIGINT, SIGTERM and (Windows) SIGBREAK to a graceful stop."""
    def handler(signum: int, _frame: object) -> None:
        stop.request(signal.Signals(signum).name)

    for name in ("SIGINT", "SIGTERM", "SIGBREAK"):
        sig = getattr(signal, name, None)
        if sig is not None:
            try:
                signal.signal(sig, handler)
            except (ValueError, OSError):   # not in the main thread, or not supported here
                pass


def configure_process_logging(settings: Settings, log_file_name: str) -> Settings:
    """Settings for this process with its own rotating log file (LOG_DIR/<log_file_name>)."""
    process_settings = settings.model_copy(update={"log_file_name": log_file_name})
    setup_logging(process_settings)
    return process_settings
