"""Grid-India Daily PSP availability checker (all nine sections, one shared workbook).

The file listing (POST, the same call the pipeline makes; one request per neighbouring month) is read for
the report dates in the catch-up window ending yesterday, and every workbook version listed is compared
with ``source_document`` of each section dataset (document name = the file name):

* a workbook not registered in every requested section -> *new* (job for that report date, only for the
  sections that miss it);
* a second file for a date that is already loaded (e.g. ``..._269_611.xls``) -> a different file name, so it
  is detected the same way and loaded as a *revision* (the pipeline keeps the earlier version);
* a report date with no workbook listed -> *not published yet* (``LATE`` after ``late_after_hours``).

A workbook already registered in all sections is never processed again; re-processing stays an explicit
``--reprocess`` command.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
from typing import Any, ClassVar

from src.pipelines.grid_india import api as gi_api
from src.pipelines.grid_india.pipeline import SECTION_PIPELINES
from src.scheduler.checkers.base import AvailabilityChecker, CheckContext, date_span
from src.scheduler.models import CheckResult, Detection, version_token


class GridIndiaPspChecker(AvailabilityChecker):
    name: ClassVar[str] = "grid_india_psp"
    defaults: ClassVar[dict[str, Any]] = {
        "sections": list(SECTION_PIPELINES),   # supply, states, fuel, outage, exchange, ir_lines, ...
        "late_after_hours": 16,                # LATE when D is not listed by D+1 16:00 local
    }

    def validate(self) -> None:
        unknown = [s for s in self.params["sections"] if s not in SECTION_PIPELINES]
        if unknown or not self.params["sections"]:
            raise ValueError(f"{self.schedule.key}: sections must be among {sorted(SECTION_PIPELINES)}, got {unknown}")

    def check(self, ctx: CheckContext) -> CheckResult:
        yesterday = ctx.today - timedelta(days=1)
        first = max(yesterday - timedelta(days=ctx.catchup_days() - 1), gi_api.XLS_HISTORY_START)
        entries: list[gi_api.ListingEntry] = []
        months = gi_api.listing_months(first, yesterday)
        for month in months:
            entries += gi_api.list_files(ctx.http, ctx.settings.grid_india_api_base_url, month)[0]
        workbooks = gi_api.select_workbooks(entries, first, yesterday)
        sections = list(self.params["sections"])
        datasets = {s: SECTION_PIPELINES[s].dataset.code for s in sections}
        registered = ctx.state.document_names(sorted(set(datasets.values())), first)
        by_day: dict[date, list[gi_api.ListingEntry]] = defaultdict(list)
        for entry in workbooks:
            by_day[entry.report_date].append(entry)
        detections: list[Detection] = []
        not_published: list[str] = []
        late: list[str] = []
        for day in date_span(first, yesterday):
            if not by_day.get(day):
                already_loaded = any(d == day for names in registered.values() for d, _name in names)
                if not already_loaded:   # e.g. a misfiled listing entry for a day that is already stored
                    hours = self.params["late_after_hours"]
                    (late if ctx.is_late(day + timedelta(days=1), hours) else not_published).append(day.isoformat())
                continue
            for entry in by_day[day]:
                missing = [s for s in sections if (day, entry.file_name) not in registered.get(datasets[s], set())]
                if not missing:
                    continue
                loaded_versions = {n for code in set(datasets.values()) for d, n in registered.get(code, set())
                                   if d == day}
                detections.append(Detection(
                    f"{day}|{entry.file_name}",
                    version_token(entry.file_path, entry.created_on),
                    {"runner": "grid_india_psp", "start_date": day.isoformat(), "end_date": day.isoformat(),
                     "sections": missing},
                    {"report_date": day.isoformat(), "file": entry.file_name, "created_on": entry.created_on,
                     "missing_sections": missing, "revision": bool(loaded_versions - {entry.file_name})}))
        return CheckResult.from_detections(detections, not_published=not_published, late=late,
                                           requests=len(months), window=f"{first}..{yesterday}",
                                           workbooks_listed=len(workbooks))
