"""CEA availability checkers.

**Daily reports** (``cea_daily``: coal, dgr10 outages, dgr2 plant-daily, on the NPP mirror). Report D is
published on D+1, at no fixed time (coal about 15:30, DGR about 17:00 IST, sometimes days late). For each
report date in the catch-up window (``catchup_max_days``, ending yesterday):

* not stored yet -> GET the file (the same request the pipeline makes): HTTP 404 means *not published
  yet* (reported, never an error); a workbook means *new*; an HTML page instead of a workbook is treated
  as not published (the pipeline would reject it);
* already stored -> on the first check of a day, the last ``revision_days`` dates are fetched again and
  compared by SHA-256 with the stored documents; a different file is a *revision*.

One detection per (report date, report): ``item_key = "YYYY-MM-DD|coal"``, version = file SHA-256.

**Monthly Power Supply Position** (``cea_psp``: energy and peak PDFs listed on cea.nic.in). The listing
page is fetched (one request per measure); a month listed but not stored is *new*. A stored month whose
rows are not all ``Revised`` (e.g. a provisional issue) is re-checked once a day: the PDF is downloaded
and a different SHA-256 than the stored document is a *revision*. A month listed twice is ambiguous: no
job, the check is recorded as a source error (the pipeline would refuse it as well).
"""

from __future__ import annotations

import hashlib
from datetime import date, timedelta
from typing import Any, ClassVar

from src.pipelines.cea import api as cea_api
from src.pipelines.cea.parse_psp import parse_listing
from src.scheduler.checkers.base import AvailabilityChecker, CheckContext, date_span
from src.scheduler.models import CheckOutcome, CheckResult, Detection, ErrorClass

# file kind -> (report, ingest_cea command, dataset code, URL marker identifying the file in source_document)
DAILY_REPORTS: dict[str, tuple[cea_api.DailyReport, str, str, str]] = {
    "coal": (cea_api.COAL, "coal", "CEA_COAL_STOCK_DAILY", "dailyCoal1-"),
    "dgr10": (cea_api.DGR10, "outages", "CEA_DGR_DAILY", "/dgr10-"),
    "dgr2": (cea_api.DGR2, "plant-daily", "CEA_DGR_DAILY", "/dgr2-"),
}
PSP_REPORTS = {"ENERGY": (cea_api.PSP_ENERGY, "psp-energy", "CEA_PSP_ENERGY_MONTHLY"),
               "PEAK": (cea_api.PSP_PEAK, "psp-peak", "CEA_PSP_PEAK_MONTHLY")}


class CeaDailyChecker(AvailabilityChecker):
    name: ClassVar[str] = "cea_daily"
    defaults: ClassVar[dict[str, Any]] = {
        "reports": ["coal"],       # coal, dgr10, dgr2
        "revision_days": 2,        # stored dates re-checked for a revised file once a day
        "late_after_hours": 48,    # LATE when D is still missing this long after D+1 00:00 local
        "not_before": None,        # optional 'YYYY-MM-DD': never look earlier (no backfill)
    }

    def validate(self) -> None:
        unknown = [r for r in self.params["reports"] if r not in DAILY_REPORTS]
        if unknown or not self.params["reports"]:
            raise ValueError(f"{self.schedule.key}: reports must be among {sorted(DAILY_REPORTS)}, got {unknown}")

    def check(self, ctx: CheckContext) -> CheckResult:
        base = ctx.settings.cea_npp_reports_base_url
        yesterday = ctx.today - timedelta(days=1)
        first = yesterday - timedelta(days=ctx.catchup_days() - 1)
        if self.params["not_before"]:
            first = max(first, date.fromisoformat(self.params["not_before"]))
        detections: list[Detection] = []
        not_published: list[str] = []
        late: list[str] = []
        requests = 0
        for kind in self.params["reports"]:
            report, command, dataset, marker = DAILY_REPORTS[kind]
            days = [d for d in date_span(max(first, report.history_start), yesterday)]
            if not days:
                continue
            stored = ctx.state.document_checksums(dataset, days[0], marker)
            for day in days:
                known = stored.get(day, set())
                revision_check = bool(known) and ctx.first_check_of_day and day >= yesterday - timedelta(
                    days=int(self.params["revision_days"]) - 1)
                if known and not revision_check:
                    continue
                fetched = cea_api.fetch_file(ctx.http, report.url(base, day))
                requests += 1
                label = f"{day}|{kind}"
                if fetched is None or cea_api.looks_like_html(fetched.content):
                    if not known:
                        (late if ctx.is_late(day + timedelta(days=1), self.params["late_after_hours"])
                         else not_published).append(label)
                    continue
                checksum = hashlib.sha256(fetched.content).hexdigest()
                if checksum in known:
                    continue
                detections.append(Detection(
                    label, checksum[:16],
                    {"runner": "cea_daily", "commands": [command], "start_date": day.isoformat(),
                     "end_date": day.isoformat()},
                    {"report": kind, "report_date": day.isoformat(), "bytes": len(fetched.content),
                     "last_modified": fetched.last_modified, "revision": bool(known)}))
        return CheckResult.from_detections(detections, not_published=not_published, late=late, requests=requests,
                                           window=f"{first}..{yesterday}")


class CeaPspChecker(AvailabilityChecker):
    name: ClassVar[str] = "cea_psp"
    defaults: ClassVar[dict[str, Any]] = {
        "measures": ["ENERGY", "PEAK"],
        "months_back": 3,           # months before the current one that are considered
        "late_after_days": None,    # optional: LATE when month M is not listed this many days after M ends
    }

    def validate(self) -> None:
        unknown = [m for m in self.params["measures"] if m not in PSP_REPORTS]
        if unknown or not self.params["measures"]:
            raise ValueError(f"{self.schedule.key}: measures must be among {sorted(PSP_REPORTS)}, got {unknown}")

    def check(self, ctx: CheckContext) -> CheckResult:
        site = ctx.settings.cea_site_base_url
        current = cea_api.month_start(ctx.today)
        months = cea_api.months(self._months_ago(current, int(self.params["months_back"])),
                                current - timedelta(days=1))
        months = [m for m in months if m >= cea_api.PSP_HISTORY_START]
        detections: list[Detection] = []
        not_published: list[str] = []
        late: list[str] = []
        ambiguous: list[str] = []
        requests = 0
        for measure in self.params["measures"]:
            report, command, _dataset = PSP_REPORTS[measure]
            listing = cea_api.fetch_file(ctx.http, report.listing_url(site), not_found_ok=False)
            requests += 1
            entries = parse_listing(listing.content, listing_title=report.listing_title)
            stored = ctx.state.cea_psp_months(measure, months[0]) if months else {}
            stored_docs = ctx.state.document_checksums(PSP_REPORTS[measure][2], months[0]) if months else {}
            for month in months:
                label = f"{measure}|{month:%Y-%m}"
                listed = [e for e in entries if e.month == month]
                if len(listed) > 1:
                    ambiguous.append(label)
                    continue
                if not listed:
                    if month not in stored:
                        days = self.params["late_after_days"]
                        month_end = (month + timedelta(days=32)).replace(day=1)
                        (late if days is not None and ctx.is_late(month_end, float(days) * 24)
                         else not_published).append(label)
                    continue
                target = {"runner": "cea_psp", "kind": command, "month": f"{month:%Y-%m}"}
                if month not in stored:
                    detections.append(Detection(label, "listed", target,
                                                {"measure": measure, "month": f"{month:%Y-%m}", "revision": False}))
                elif stored[month] != {"Revised"} and ctx.first_check_of_day:
                    pdf = cea_api.fetch_file(ctx.http, cea_api.pdf_url(site, listed[0].uuid), not_found_ok=False)
                    requests += 1
                    checksum = hashlib.sha256(pdf.content).hexdigest()
                    if checksum not in stored_docs.get(month, set()):
                        detections.append(Detection(label, checksum[:16], target,
                                                    {"measure": measure, "month": f"{month:%Y-%m}", "revision": True,
                                                     "stored_status": sorted(stored[month])}))
        result = CheckResult.from_detections(detections, not_published=not_published, late=late, requests=requests,
                                             months=[f"{m:%Y-%m}" for m in months])
        if ambiguous and not detections:
            result.outcome = CheckOutcome.SOURCE_ERROR
            result.error = f"listed more than once (needs a person to resolve): {ambiguous}"
            result.error_class = ErrorClass.PERMANENT
        if ambiguous:
            result.detail["ambiguous"] = ambiguous
        return result

    @staticmethod
    def _months_ago(month: date, count: int) -> date:
        for _ in range(count):
            month = (month - timedelta(days=1)).replace(day=1)
        return month
