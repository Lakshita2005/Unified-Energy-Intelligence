"""Grid-India file listing API, workbook selection and availability timestamps.

Verified live on 2026-11-13 (docs/grid_india_source_assessment.md)::

    POST {api}/file  {"_source": "GRDW", "_type": "DAILY_PSP_REPORT", "_fileDate": "2026-27", "_month": "11"}
      -> retData: [{Id, Title_ "12.11.26_NLDC_PSP", FilePath, MimeType, CreatedOn "13-11-2026 10:40", ...}]
    GET  {cdn}/{FilePath}

* ``Title_`` carries the operating day D; the file is uploaded on D+1.
* A revision is an additional file for the same title (e.g. ``..._269_611.xls``).
* The listing month is not strictly the report month (the August listing also
  holds the 31 July report), so neighbouring months are queried and files are
  selected by the date in ``Title_``.
* ``MimeType`` is inconsistent; files are selected by extension.

Availability (decision G3): ``CreatedOn`` is the actual upload time and is
reliable for report dates from 2025-05-12. Earlier ``CreatedOn`` values are
bulk re-upload dates, so those rows get the estimate D+2 09:00 IST and no
actual publication time.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from src.common.exceptions import HttpStatusError
from src.common.http_client import HttpClient
from src.pipelines.grid_india.errors import GridIndiaApiError

IST = ZoneInfo("Asia/Kolkata")
SOURCE = "GRDW"
PSP_FILE_TYPE = "DAILY_PSP_REPORT"
XLS_HISTORY_START = date(2023, 1, 1)     # Daily PSP workbooks (.xls); PDF only before
RELIABLE_PUBLICATION_FROM = date(2025, 5, 12)
MAX_DAYS_PER_COMMAND = 31
REQUEST_PAUSE_SECONDS = 0.5
PUBLISHED = "PUBLISHED"
ESTIMATED = "ESTIMATED_D_PLUS_2_0900"
_TITLE = re.compile(r"^(\d{2})\.(\d{2})\.(\d{2})_NLDC_PSP", re.IGNORECASE)


def today_ist() -> date:
    return datetime.now(IST).date()


def fy_label(day: date) -> str:
    """Indian financial year label used by the API: April 2026 - March 2027 -> '2026-27'."""
    start = day.year if day.month >= 4 else day.year - 1
    return f"{start}-{(start + 1) % 100:02d}"


def listing_months(start: date, end: date) -> list[date]:
    """First-of-month dates to query: the month before ``start`` through the month after ``end``."""
    first = (start.replace(day=1) - timedelta(days=1)).replace(day=1)
    last = (end.replace(day=1) + timedelta(days=32)).replace(day=1)
    months, current = [], first
    while current <= last:
        months.append(current)
        current = (current + timedelta(days=32)).replace(day=1)
    return months


def validate_date_range(start: date, end: date, *, today: date) -> None:
    if end < start:
        raise ValueError(f"end date {end} is before start date {start}")
    if (end - start).days + 1 > MAX_DAYS_PER_COMMAND:
        raise ValueError(f"at most {MAX_DAYS_PER_COMMAND} days per command ({start}..{end} requested)")
    if start < XLS_HISTORY_START:
        raise ValueError(f"Daily PSP workbooks (.xls) are available from {XLS_HISTORY_START}; earlier reports are "
                         f"PDF only and out of scope ({start} requested)")
    if end >= today:
        raise ValueError(f"the report for {end} cannot be published yet (latest: {today - timedelta(days=1)})")


@dataclass(frozen=True)
class ListingEntry:
    """One file in the listing."""

    file_id: int | None
    title: str
    report_date: date
    file_path: str
    created_on: datetime | None
    modified_on: datetime | None
    mime_type: str | None
    raw: dict[str, Any]

    @property
    def file_name(self) -> str:
        return self.file_path.rsplit("/", 1)[-1]

    @property
    def is_workbook(self) -> bool:
        return self.file_path.lower().endswith((".xls", ".xlsx"))


def _listing_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(str(value).strip(), "%d-%m-%Y %H:%M").replace(tzinfo=IST)
    except ValueError:
        return None


def report_date_of(title: str) -> date | None:
    match = _TITLE.match(title.strip())
    if not match:
        return None
    try:
        return date(2000 + int(match[3]), int(match[2]), int(match[1]))
    except ValueError:
        return None


def parse_listing(payload: Any) -> list[ListingEntry]:
    """Entries of a listing response.

    Raises:
        GridIndiaApiError: The response is not the expected envelope.
    """
    if not isinstance(payload, dict) or "retData" not in payload:
        raise GridIndiaApiError(f"unexpected listing response: {str(payload)[:200]}")
    if payload.get("flagType") not in (0, None):
        raise GridIndiaApiError(f"listing API reported an error: {payload.get('retMessage')}")
    entries = []
    for item in payload.get("retData") or []:
        title, path = str(item.get("Title_") or ""), str(item.get("FilePath") or "")
        day = report_date_of(title)
        if day is None or not path:
            continue
        entries.append(ListingEntry(item.get("Id"), title, day, path, _listing_time(item.get("CreatedOn")),
                                    _listing_time(item.get("ModifiedOn")), item.get("MimeType"), item))
    return entries


def list_files(client: HttpClient, base_url: str, month: date) -> tuple[list[ListingEntry], bytes]:
    """POST the listing for one month. Returns the entries and the raw response body."""
    body = {"_source": SOURCE, "_type": PSP_FILE_TYPE, "_fileDate": fy_label(month), "_month": f"{month.month:02d}"}
    try:
        response = client.post(f"{base_url.rstrip('/')}/file", json=body, retry=True)
    except HttpStatusError as exc:
        raise GridIndiaApiError(f"listing API returned HTTP {exc.status_code} for {body}") from exc
    try:
        payload = response.json()
    except ValueError as exc:
        raise GridIndiaApiError(f"listing API returned a non-JSON response for {body}") from exc
    return parse_listing(payload), response.content


def select_workbooks(entries: list[ListingEntry], start: date, end: date) -> list[ListingEntry]:
    """Every workbook version for report dates in range, oldest upload first (revisions after originals)."""
    unique = {(e.file_path, e.file_id): e for e in entries if e.is_workbook and start <= e.report_date <= end}
    far_future = datetime.max.replace(tzinfo=timezone.utc)
    return sorted(unique.values(), key=lambda e: (e.report_date, e.created_on or far_future, e.file_path))


def download(client: HttpClient, cdn_base: str, entry: ListingEntry) -> tuple[bytes, str, str | None, datetime]:
    """GET a workbook. Returns (content, url, content type, retrieval time). Retried by ``client``."""
    url = f"{cdn_base.rstrip('/')}/{entry.file_path.lstrip('/')}"
    try:
        response = client.get(url)
    except HttpStatusError as exc:
        raise GridIndiaApiError(f"file CDN returned HTTP {exc.status_code} for {url}") from exc
    content = response.content
    if content[:512].lstrip().lower().startswith((b"<!doctype html", b"<html")):
        raise GridIndiaApiError(f"an HTML page was returned instead of the workbook ({url})")
    return content, url, response.headers.get("content-type"), datetime.now(timezone.utc)


@dataclass(frozen=True)
class Availability:
    """When a report's rows may be used (decision G3)."""

    published_at: datetime | None   # actual upload time, only where reliable
    available_at: datetime
    basis: str
    note: str | None = None         # set when an actual time was present but not used


def estimated_availability(report_date: date) -> datetime:
    return datetime.combine(report_date + timedelta(days=2), time(9, 0), IST)


def availability(report_date: date, created_on: datetime | None) -> Availability:
    """Actual upload time from 2025-05-12 (when plausible); otherwise D+2 09:00 IST, flagged as an estimate."""
    estimate = Availability(None, estimated_availability(report_date), ESTIMATED)
    if report_date < RELIABLE_PUBLICATION_FROM:
        return estimate
    if created_on is None:
        return Availability(None, estimate.available_at, ESTIMATED, "no CreatedOn in the listing")
    if created_on < datetime.combine(report_date + timedelta(days=1), time(0, 0), IST):
        return Availability(None, estimate.available_at, ESTIMATED,
                            f"CreatedOn {created_on.isoformat()} is before the operating day ended")
    return Availability(created_on, created_on, PUBLISHED)
