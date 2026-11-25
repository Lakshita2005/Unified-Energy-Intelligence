"""Source-specific availability checkers and their registry.

To add a dataset: implement an :class:`~src.scheduler.checkers.base.AvailabilityChecker`, register it in
:data:`CHECKERS`, add an executor for its job target (:mod:`src.scheduler.executors`) and a
``[[schedule]]`` entry in ``config/scheduler.toml``. The scheduler itself does not change.
"""

from __future__ import annotations

from src.scheduler.checkers.base import AvailabilityChecker, CheckContext
from src.scheduler.checkers.cea import CeaDailyChecker, CeaPspChecker
from src.scheduler.checkers.grid_india import GridIndiaPspChecker
from src.scheduler.checkers.iex import IexChecker
from src.scheduler.checkers.mosdac import MosdacChecker
from src.scheduler.checkers.npp import NppChecker
from src.scheduler.checkers.open_meteo import OpenMeteoForecastChecker, OpenMeteoHistoricalChecker
from src.scheduler.config import SchedulerConfig, SchedulerConfigError

CHECKERS: dict[str, type[AvailabilityChecker]] = {
    cls.name: cls for cls in (CeaDailyChecker, CeaPspChecker, IexChecker, GridIndiaPspChecker, NppChecker,
                              MosdacChecker, OpenMeteoForecastChecker, OpenMeteoHistoricalChecker)
}


def build_checkers(config: SchedulerConfig) -> dict[str, AvailabilityChecker]:
    """One checker per schedule (all of them, enabled or not, so the whole file is validated).

    Raises:
        SchedulerConfigError: Unknown checker or invalid parameters.
    """
    checkers: dict[str, AvailabilityChecker] = {}
    for key, schedule in config.schedules.items():
        cls = CHECKERS.get(schedule.checker)
        if cls is None:
            raise SchedulerConfigError(f"{key}: unknown checker {schedule.checker!r}; available: {sorted(CHECKERS)}")
        try:
            checkers[key] = cls(schedule)
        except ValueError as exc:
            raise SchedulerConfigError(str(exc)) from exc
    return checkers


__all__ = ["CHECKERS", "AvailabilityChecker", "CheckContext", "build_checkers"]
