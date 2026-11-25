"""Wiring: build the scheduler / worker / repository from settings (used by the scripts)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from config.settings import Settings
from src.common.database import DatabaseManager, SavepointDatabase
from src.common.exceptions import ConfigurationError
from src.common.http_client import HttpClient
from src.scheduler.checkers import build_checkers
from src.scheduler.checkers.state import PostgresSourceState
from src.scheduler.clock import SystemClock
from src.scheduler.config import ScheduleConfig, SchedulerConfig, load_scheduler_config
from src.scheduler.executors import Executor
from src.scheduler.locks import AdvisoryLockFactory
from src.scheduler.repository import PostgresJobRepository, process_identity
from src.scheduler.scheduler import Scheduler
from src.scheduler.worker import Worker


def load_config(settings: Settings) -> tuple[SchedulerConfig, dict]:
    """Load and fully validate the configuration (including every checker's parameters)."""
    config = load_scheduler_config(settings.scheduler_config_file, settings.scheduler_timezone)
    return config, build_checkers(config)


@dataclass
class Runtime:
    settings: Settings
    database: DatabaseManager
    repository: PostgresJobRepository

    def require_schema(self) -> None:
        if not self.repository.schema_exists():
            raise ConfigurationError(f"schema {self.settings.scheduler_jobs_schema!r} not found: migration "
                                     "0011_pi_jobs has not been applied (it must be applied by the schema owner)")

    def preview_sync(self, schedules: list[ScheduleConfig], now: datetime) -> dict[str, str]:
        """``sync_schedules`` inside one outer transaction that is ALWAYS rolled back (sync-config --dry-run)."""
        with self.database.engine.connect() as connection:
            outer = connection.begin()
            try:
                return PostgresJobRepository(SavepointDatabase(connection),
                                             self.settings.scheduler_jobs_schema).sync_schedules(schedules, now)
            finally:
                outer.rollback()

    def close(self) -> None:
        self.database.dispose()


def open_runtime(settings: Settings) -> Runtime:
    database = DatabaseManager.from_settings(settings)
    return Runtime(settings, database, PostgresJobRepository(database, settings.scheduler_jobs_schema))


def build_scheduler(runtime: Runtime) -> Scheduler:
    config, checkers = load_config(runtime.settings)
    return Scheduler(config=config, checkers=checkers, repository=runtime.repository, clock=SystemClock(),
                     settings=runtime.settings, http_factory=lambda: HttpClient(settings=runtime.settings),
                     state=PostgresSourceState(runtime.database, runtime.settings.db_schema),
                     locks=AdvisoryLockFactory(runtime.database.engine), process_id=process_identity("pi-scheduler"))


def build_worker(runtime: Runtime) -> Worker:
    return Worker(repository=runtime.repository, executor=Executor(runtime.settings),
                  locks=AdvisoryLockFactory(runtime.database.engine), clock=SystemClock(), settings=runtime.settings,
                  worker_id=process_identity("pi-worker"))
