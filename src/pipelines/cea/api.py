"""CEA report locations, publication rules and HTTP access.

Verified live on 2026-11-13 (docs/cea_source_assessment.md):

* Daily reports on the NPP mirror (``.xls``; ``.pdf`` copies also exist)::

    {npp}/daily/fuel/{DD-MM-YYYY}/dailyCoal1-{YYYY-MM-DD}.xls   coal stock "AS ON" the date
    {npp}/daily/dgr/{DD-MM-YYYY}/dgr{N}-{YYYY-MM-DD}.xls        generation report sub-report N

  A report that is not (yet) published returns HTTP 404. Report D appears on
  D+1 afternoon (coal ~15:30 IST, DGR ~17:00 IST, from ``Last-Modified``).
* Monthly Power Supply Position (energy / peak) PDFs, listed on cea.nic.in
  report pages; each file is ``{site}/api/dashboardReports/download-pdf/en/{uuid}``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from zoneinfo import ZoneInfo

from src.common.exceptions import HttpStatusError
from src.common.http_client import HttpClient
from src.pipelines.cea.errors import CeaHttpError, InvalidResponseError

IST = ZoneInfo("Asia/Kolkata")
MAX_DAYS_PER_COMMAND = 31
MAX_MONTHS_PER_COMMAND = 12
REQUEST_PAUSE_SECONDS = 0.5  # between consecutive requests of one run

# First report dates verified on the NPP mirror (earlier dates return 404).
COAL_HISTORY_START = date(2024, 6, 15)
DGR_HISTORY_START = date(2018, 4, 1)
# The PSP listings start in 2013, but entries before mid-2020 are either a
# different (older) layout or mislabelled generation reports; the parser
# rejects anything that is not the current layout.
PSP_HISTORY_START = date(2020, 4, 1)


@dataclass(frozen=True)
class DailyReport:
    """One daily .xls report on the NPP mirror."""

    kind: str          # "coal", "dgr10", "dgr2"
    folder: str        # "fuel" or "dgr"
    file_stem: str     # "dailyCoal1", "dgr10", "dgr2"
    history_start: date

    def url(self, base: str, day: date) -> str:
        return f"{base.rstrip('/')}/daily/{self.folder}/{day:%d-%m-%Y}/{self.file_stem}-{day:%Y-%m-%d}.xls"


COAL = DailyReport("coal", "fuel", "dailyCoal1", COAL_HISTORY_START)
DGR10 = DailyReport("dgr10", "dgr", "dgr10", DGR_HISTORY_START)
DGR2 = DailyReport("dgr2", "dgr", "dgr2", DGR_HISTORY_START)


@dataclass(frozen=True)
class PspReport:
    """One monthly Power Supply Position report series on cea.nic.in."""

    measure: str               # "ENERGY" or "PEAK"
    listing_title: str         # "Power Supply Position - Energy"
    sub_sub_heading_id: int

    def listing_url(self, site: str) -> str:
        return (f"{site.rstrip('/')}/reports/dashboardView?headingId=107&subHeadingId=1003"
                f"&subSubHeadingId={self.sub_sub_heading_id}&lang=en")


PSP_ENERGY = PspReport("ENERGY", "Power Supply Position - Energy", 10010)
PSP_PEAK = PspReport("PEAK", "Power Supply Position - Peak", 10011)


def pdf_url(site: str, uuid: str) -> str:
    return f"{site.rstrip('/')}/api/dashboardReports/download-pdf/en/{uuid}"


def today_ist() -> date:
    return datetime.now(IST).date()


def validate_date_range(report: DailyReport, start: date, end: date, *, today: date) -> None:
    """Reject ranges that are reversed, too long, before the history start or not yet publishable.

    Report D is published on D+1, so the latest requestable date is yesterday (IST).
    """
    if end < start:
        raise ValueError(f"end date {end} is before start date {start}")
    if (end - start).days + 1 > MAX_DAYS_PER_COMMAND:
        raise ValueError(f"at most {MAX_DAYS_PER_COMMAND} days per command ({start}..{end} requested)")
    if start < report.history_start:
        raise ValueError(f"{report.kind} reports are available from {report.history_start} ({start} requested)")
    if end >= today:
        raise ValueError(f"{report.kind} report for {end} cannot be published yet (latest: {today - timedelta(days=1)})")


def month_start(value: date) -> date:
    return value.replace(day=1)


def months(start: date, end: date) -> list[date]:
    result, current = [], month_start(start)
    while current <= month_start(end):
        result.append(current)
        current = (current + timedelta(days=32)).replace(day=1)
    return result


def validate_month_range(start: date, end: date, *, today: date) -> None:
    """Months are first-of-month dates. The latest requestable month is the previous one."""
    start, end = month_start(start), month_start(end)
    if end < start:
        raise ValueError(f"end month {end:%Y-%m} is before start month {start:%Y-%m}")
    if len(months(start, end)) > MAX_MONTHS_PER_COMMAND:
        raise ValueError(f"at most {MAX_MONTHS_PER_COMMAND} months per command")
    if start < PSP_HISTORY_START:
        raise ValueError(f"monthly power supply position is supported from {PSP_HISTORY_START:%Y-%m} "
                         f"({start:%Y-%m} requested)")
    if end >= month_start(today):
        raise ValueError(f"the report for {end:%Y-%m} cannot be published yet")


def parse_month(value: str) -> date:
    match = re.fullmatch(r"(\d{4})-(\d{2})", value.strip())
    if not match:
        raise ValueError(f"invalid month {value!r}, expected YYYY-MM")
    return date(int(match[1]), int(match[2]), 1)


@dataclass(frozen=True)
class FetchedFile:
    content: bytes
    content_type: str | None
    retrieved_at: datetime
    url: str
    last_modified: datetime | None


def _last_modified(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def looks_like_html(content: bytes) -> bool:
    head = content[:512].lstrip().lower()
    return head.startswith(b"<!doctype html") or head.startswith(b"<html")


def fetch_file(client: HttpClient, url: str, *, not_found_ok: bool = True) -> FetchedFile | None:
    """GET a report file. Returns None on HTTP 404 (not published) when ``not_found_ok``.

    Timeouts and transient statuses are retried by ``client``.

    Raises:
        CeaHttpError: Any other error status.
        HttpRequestError: The request could not be completed after retries.
    """
    try:
        response = client.get(url)
    except HttpStatusError as exc:
        if exc.status_code == 404 and not_found_ok:
            return None
        raise CeaHttpError(f"CEA/NPP returned HTTP {exc.status_code} for {url}") from exc
    return FetchedFile(
        content=response.content,
        content_type=response.headers.get("content-type"),
        retrieved_at=datetime.now(timezone.utc),
        url=url,
        last_modified=_last_modified(response.headers.get("last-modified")),
    )


def require_binary(fetched: FetchedFile, what: str) -> None:
    """An HTML page served with status 200 instead of the report means 'not available'; fail loudly."""
    if looks_like_html(fetched.content):
        raise InvalidResponseError(f"{what}: an HTML page was returned instead of the report ({fetched.url})")
