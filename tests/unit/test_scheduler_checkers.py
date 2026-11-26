"""Source-specific availability checkers with mocked HTTP and a fake stored state (no network, no database)."""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

import httpx
import pytest

from src.scheduler.checkers.cea import CeaDailyChecker, CeaPspChecker
from src.scheduler.checkers.base import CheckContext
from src.scheduler.checkers.grid_india import GridIndiaPspChecker
from src.scheduler.checkers.iex import IexChecker
from src.scheduler.checkers.mosdac import MosdacChecker
from src.scheduler.checkers.npp import NppChecker
from src.scheduler.checkers.open_meteo import OpenMeteoForecastChecker, OpenMeteoHistoricalChecker
from src.scheduler.models import CheckOutcome, Watermark, WatermarkStatus
from tests.support import cea as cea_support
from tests.support import iex as iex_support
from tests.support import mosdac as mosdac_support
from tests.support.grid_india import FileSpec
from tests.support.scheduler import FakeState, http, schedule, settings

IST = ZoneInfo("Asia/Kolkata")
XLS = b"\xd0\xcf\x11\xe0" + b"workbook"


def ctx(sched, *, now: datetime, state: FakeState, handler, previous: datetime | None = None, cfg=None,
        watermarks: dict | None = None) -> CheckContext:
    marks = watermarks or {}
    return CheckContext(sched, now, previous, http(handler, cfg), cfg or settings(), state, lambda k: marks.get(k))


class Recorder:
    """httpx handler routing by a function, recording every request."""

    def __init__(self, route) -> None:
        self.route = route
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return self.route(request)


def ist(day: int, hh: int, mm: int = 0, month: int = 11) -> datetime:
    return datetime(2026, month, day, hh, mm, tzinfo=IST)


# =========================================================================== CEA daily
def cea_schedule(**params):
    return schedule("CEA_COAL_DAILY", checker="cea_daily", catchup_max_days=3,
                    params={"reports": ["coal"], "revision_days": 1, "late_after_hours": 48, **params})


def coal_route(files: dict[date, bytes]):
    def route(request: httpx.Request) -> httpx.Response:
        for day, body in files.items():
            if request.url.path.endswith(f"dailyCoal1-{day:%Y-%m-%d}.xls"):
                return httpx.Response(200, content=body)
        return httpx.Response(404, text="not found")
    return route


def test_cea_new_report_detected_and_unpublished_day_reported() -> None:
    state = FakeState(checksums={("CEA_COAL_STOCK_DAILY", "dailyCoal1-"): {date(2026, 11, 22): {"x"}}})
    route = Recorder(coal_route({date(2026, 11, 23): XLS}))
    checker = CeaDailyChecker(cea_schedule())
    result = checker.check(ctx(checker.schedule, now=ist(25, 12, 30), previous=ist(25, 12, 0), state=state,
                               handler=route))
    assert result.outcome is CheckOutcome.NEW_DATA
    [d] = result.detections
    assert d.item_key == "2026-11-23|coal" and d.version_token == hashlib.sha256(XLS).hexdigest()[:16]
    assert d.target == {"runner": "cea_daily", "commands": ["coal"], "start_date": "2026-11-23",
                        "end_date": "2026-11-23"}
    assert result.detail["not_published"] == ["2026-11-24|coal"]       # 404: not an error
    assert len(route.requests) == 2                                      # the stored 22-Nov is not fetched


def test_cea_nothing_new_and_no_requests_for_stored_days() -> None:
    days = {date(2026, 11, d): {hashlib.sha256(XLS).hexdigest()} for d in (22, 23, 24)}
    route = Recorder(coal_route({}))
    checker = CeaDailyChecker(cea_schedule())
    result = checker.check(ctx(checker.schedule, now=ist(25, 18), previous=ist(25, 17, 30),
                               state=FakeState(checksums={("CEA_COAL_STOCK_DAILY", "dailyCoal1-"): days}),
                               handler=route))
    assert result.outcome is CheckOutcome.NO_NEW_DATA and route.requests == []


def test_cea_revised_file_detected_on_first_check_of_the_day_only() -> None:
    days = {date(2026, 11, d): {hashlib.sha256(XLS).hexdigest()} for d in (22, 23, 24)}
    state = FakeState(checksums={("CEA_COAL_STOCK_DAILY", "dailyCoal1-"): days})
    revised = XLS + b"-rev"
    checker = CeaDailyChecker(cea_schedule())
    first = Recorder(coal_route({date(2026, 11, 24): revised}))
    result = checker.check(ctx(checker.schedule, now=ist(25, 12), previous=ist(24, 23, 30), state=state,
                               handler=first))
    [d] = result.detections
    assert d.detail["revision"] and d.item_key == "2026-11-24|coal" and len(first.requests) == 1
    later = Recorder(coal_route({date(2026, 11, 24): revised}))
    assert checker.check(ctx(checker.schedule, now=ist(25, 12, 30), previous=ist(25, 12), state=state,
                             handler=later)).detections == [] and later.requests == []


def test_cea_late_html_and_catch_up_window() -> None:
    route = Recorder(lambda r: httpx.Response(200, content=b"<!DOCTYPE html><html>maintenance</html>")
                     if "11-2026/dailyCoal1-2026-11-24" in str(r.url) else httpx.Response(404))
    checker = CeaDailyChecker(cea_schedule())
    result = checker.check(ctx(checker.schedule, now=ist(25, 13), state=FakeState(), handler=route))
    # catch-up 3 days: 22, 23, 24 Nov; 22 Nov is past D+1 00:00 + 48 h -> LATE; HTML is "not published"
    assert result.outcome is CheckOutcome.LATE
    assert result.detail["late"] == ["2026-11-22|coal"]
    assert result.detail["not_published"] == ["2026-11-23|coal", "2026-11-24|coal"]
    assert len(route.requests) == 3


def test_cea_dgr_checks_both_files_separately() -> None:
    sched = schedule("CEA_DGR_DAILY", checker="cea_daily", catchup_max_days=1, params={"reports": ["dgr10", "dgr2"]})
    route = Recorder(lambda r: httpx.Response(200, content=XLS) if "/dgr10-" in str(r.url) else httpx.Response(404))
    result = CeaDailyChecker(sched).check(ctx(sched, now=ist(25, 18), state=FakeState(), handler=route))
    assert [d.item_key for d in result.detections] == ["2026-11-24|dgr10"]
    assert result.detections[0].target["commands"] == ["outages"]
    assert result.detail["not_published"] == ["2026-11-24|dgr2"]


# =========================================================================== CEA monthly PSP
def test_cea_psp_new_month_listed_and_provisional_revision() -> None:
    sched = schedule("CEA_PSP_MONTHLY", checker="cea_psp", params={"measures": ["ENERGY"], "months_back": 2})
    listing = cea_support.listing_html("Power Supply Position - Energy",
                                       [("2026-09", "11111111-1111-4111-8111-111111111111"),
                                        ("2026-10", "22222222-2222-4222-8222-222222222222")])
    pdf = b"%PDF-1.7 revised october"

    def route(request: httpx.Request) -> httpx.Response:
        if "download-pdf" in str(request.url):
            return httpx.Response(200, content=pdf)
        return httpx.Response(200, content=listing)

    # September stored and Revised; October not stored -> new
    state = FakeState(psp={"ENERGY": {date(2026, 9, 1): {"Revised"}}})
    result = CeaPspChecker(sched).check(ctx(sched, now=ist(25, 10), state=state, handler=Recorder(route)))
    assert [(d.item_key, d.version_token, d.target["kind"]) for d in result.detections] == [
        ("ENERGY|2026-10", "listed", "psp-energy")]
    # October stored as provisional (status unknown): the PDF is compared by SHA-256 once a day
    state.psp["ENERGY"][date(2026, 10, 1)] = {"UNKNOWN"}
    state.checksums[("CEA_PSP_ENERGY_MONTHLY", None)] = {date(2026, 10, 1): {"old"}}
    recorder = Recorder(route)
    revised = CeaPspChecker(sched).check(ctx(sched, now=ist(26, 10), previous=ist(25, 18), state=state,
                                             handler=recorder))
    [d] = revised.detections
    assert d.detail["revision"] and d.version_token == hashlib.sha256(pdf).hexdigest()[:16]
    assert len(recorder.requests) == 2   # listing + PDF


def test_cea_psp_month_listed_twice_is_a_source_error_without_jobs() -> None:
    sched = schedule("CEA_PSP_MONTHLY", checker="cea_psp", params={"measures": ["PEAK"], "months_back": 1})
    listing = cea_support.listing_html("Power Supply Position - Peak",
                                       [("2026-10", "33333333-3333-4333-8333-333333333333"),
                                        ("2026-10", "44444444-4444-4444-8444-444444444444")])
    result = CeaPspChecker(sched).check(ctx(sched, now=ist(25, 10), state=FakeState(),
                                            handler=lambda r: httpx.Response(200, content=listing)))
    assert result.outcome is CheckOutcome.SOURCE_ERROR and result.detections == []
    assert result.detail["ambiguous"] == ["PEAK|2026-10"]


# =========================================================================== IEX
def iex_schedule(market: str, **params):
    return schedule(f"IEX_{market.upper()}", checker="iex", catchup_max_days=2, params={"market": market, **params})


def stored_mkt(day: date, blocks: range, market: str = "rtm") -> dict[int, Decimal]:
    rec = iex_support.dam_record
    return {b: Decimal(str(rec(day, b, spaced=market != "rtm")["mcp"])) for b in blocks}


def test_iex_rtm_partial_day_then_more_blocks_then_complete() -> None:
    sched = iex_schedule("rtm")
    today, yesterday = date(2026, 11, 25), date(2026, 11, 24)
    state = FakeState(iex={"IEX_RTM": {yesterday: stored_mkt(yesterday, range(1, 97))}})
    checker = IexChecker(sched)

    def run(blocks_today: range, now: datetime):
        body = iex_support.envelope([iex_support.dam_record(d, b, spaced=False) for d, bl in
                                     ((yesterday, range(1, 97)), (today, blocks_today)) for b in bl])
        return checker.check(ctx(sched, now=now, state=state, handler=lambda r: httpx.Response(200, content=body)))

    first = run(range(1, 41), ist(25, 10))
    [d] = first.detections
    assert d.item_key == "2026-11-25" and d.detail["partial"] and d.detail["new_blocks"] == 40
    assert d.target == {"runner": "iex", "kind": "rtm", "start_date": "2026-11-25", "end_date": "2026-11-25"}
    state.iex["IEX_RTM"][today] = stored_mkt(today, range(1, 41))            # the job loaded blocks 1-40
    assert run(range(1, 41), ist(25, 10, 30)).outcome is CheckOutcome.NO_NEW_DATA
    more = run(range(1, 53), ist(25, 13))
    [d2] = more.detections
    assert d2.detail["new_blocks"] == 12 and d2.version_token != d.version_token   # a new version of the day
    state.iex["IEX_RTM"][today] = stored_mkt(today, range(1, 97))
    assert run(range(1, 97), ist(25, 23, 59)).outcome is CheckOutcome.NO_NEW_DATA


def test_iex_revised_price_and_unpublished_tomorrow() -> None:
    sched = iex_schedule("dam", late_after_hours=16.5)
    today, tomorrow = date(2026, 11, 25), date(2026, 11, 26)
    state = FakeState(iex={"IEX_DAM": {d: stored_mkt(d, range(1, 97), "dam") for d in (date(2026, 11, 24), today)}})
    records = [iex_support.dam_record(d, b) for d in (date(2026, 11, 24), today) for b in range(1, 97)]
    records[100] = iex_support.dam_record(today, 5, price="9999.99")          # one block revised
    body = iex_support.envelope(records)
    result = IexChecker(sched).check(ctx(sched, now=ist(25, 13), state=state,
                                         handler=lambda r: httpx.Response(200, content=body)))
    [d] = result.detections
    assert d.item_key == "2026-11-25" and d.detail["changed_blocks"] == 1 and d.detail["revision"]
    assert result.detail["not_published"] == [tomorrow.isoformat()]
    late = IexChecker(sched).check(ctx(sched, now=ist(25, 16, 45), state=state,
                                       handler=lambda r: httpx.Response(200, content=body)))
    assert late.detail["late"] == [tomorrow.isoformat()]


def test_iex_empty_response_means_not_published() -> None:
    sched = iex_schedule("gdam")
    body = iex_support.envelope([])
    result = IexChecker(sched).check(ctx(sched, now=ist(25, 12, 45), state=FakeState(),
                                         handler=lambda r: httpx.Response(200, content=body)))
    assert result.outcome is CheckOutcome.NOT_YET_PUBLISHED and len(result.detail["not_published"]) == 3


# =========================================================================== Grid-India
def grid_route(specs: list[FileSpec]):
    def route(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content or b"{}")
        month = int(body.get("_month", 0))
        entries = [s.entry(i) for i, s in enumerate(specs, start=1) if s.report_date.month == month]
        return httpx.Response(200, json={"flagType": 0, "retData": entries})
    return route


def test_grid_india_new_workbook_revision_and_partially_loaded_sections() -> None:
    sched = schedule("GRID_INDIA_PSP", checker="grid_india_psp", catchup_max_days=3,
                     params={"sections": ["supply", "states"], "late_after_hours": 16})
    d22, d23, d24 = date(2026, 11, 22), date(2026, 11, 23), date(2026, 11, 24)
    specs = [FileSpec(d22, XLS, suffix="240"), FileSpec(d23, XLS, suffix="723"),
             FileSpec(d23, XLS, datetime(2026, 11, 24, 16, 38), suffix="723_611"), FileSpec(d24, XLS, suffix="597")]
    name = lambda d, s: f"{d:%d.%m.%y}_NLDC_PSP_{s}.xls"   # noqa: E731
    state = FakeState(names={
        "GRID_INDIA_PSP_DAILY": {(d22, name(d22, "240")), (d23, name(d23, "723"))},
        "GRID_INDIA_PSP_STATES": {(d22, name(d22, "240")), (d23, name(d23, "723")), (d24, name(d24, "597"))},
    })
    route = Recorder(grid_route(specs))
    result = GridIndiaPspChecker(sched).check(ctx(sched, now=ist(25, 11), state=state, handler=route))
    found = {d.item_key: d for d in result.detections}
    assert set(found) == {"2026-11-23|23.11.26_NLDC_PSP_723_611.xls", "2026-11-24|24.11.26_NLDC_PSP_597.xls"}
    revision = found["2026-11-23|23.11.26_NLDC_PSP_723_611.xls"]
    assert revision.detail["revision"] and revision.target["sections"] == ["supply", "states"]
    assert found["2026-11-24|24.11.26_NLDC_PSP_597.xls"].target["sections"] == ["supply"]   # states already loaded
    assert len(route.requests) == 3                        # listings for Oct, Nov, Dec (neighbouring months)


def test_grid_india_not_listed_yet_then_late() -> None:
    sched = schedule("GRID_INDIA_PSP", checker="grid_india_psp", catchup_max_days=1)
    early = GridIndiaPspChecker(sched).check(ctx(sched, now=ist(25, 9, 15), state=FakeState(),
                                                 handler=grid_route([])))
    assert early.outcome is CheckOutcome.NOT_YET_PUBLISHED and early.detail["not_published"] == ["2026-11-24"]
    late = GridIndiaPspChecker(sched).check(ctx(sched, now=ist(25, 16, 30), state=FakeState(),
                                                handler=grid_route([])))
    assert late.outcome is CheckOutcome.LATE


# =========================================================================== NPP
def npp_body(*updated: int) -> bytes:
    return json.dumps([{"updated_on": u, "name_of_data": "DEMAND MET", "value_of_data": 150000} for u in updated]).encode()


def test_npp_newer_snapshots_today_and_yesterday_completed_once() -> None:
    sched = schedule("NPP_DEMAND", checker="npp", catchup_max_days=2, params={"kind": "demand"})
    today, yesterday = date(2026, 11, 25), date(2026, 11, 24)
    state = FakeState(npp={yesterday: 1000, today: 2000})

    def route(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=npp_body(1500 if request.url.params["date"] == "2026-11-24" else 2500))

    first = Recorder(route)
    result = NppChecker(sched).check(ctx(sched, now=ist(25, 0, 30), previous=ist(24, 23, 30), state=state,
                                         handler=first))
    assert sorted(d.item_key for d in result.detections) == ["2026-11-24", "2026-11-25"]
    later = Recorder(route)
    state.npp = {yesterday: 1500, today: 2500}
    again = NppChecker(sched).check(ctx(sched, now=ist(25, 1), previous=ist(25, 0, 30), state=state, handler=later))
    assert again.outcome is CheckOutcome.NO_NEW_DATA
    assert [r.url.params["date"] for r in later.requests] == ["2026-11-25"]    # yesterday only on the first check


def test_npp_empty_day_is_not_published() -> None:
    sched = schedule("NPP_GENERATION", checker="npp", catchup_max_days=1, params={"kind": "generation"})
    result = NppChecker(sched).check(ctx(sched, now=ist(25, 0, 5), state=FakeState(),
                                         handler=lambda r: httpx.Response(200, content=b"[]")))
    assert result.outcome is CheckOutcome.NOT_YET_PUBLISHED


# =========================================================================== MOSDAC
def test_mosdac_new_series_detected_once(tmp_path) -> None:
    locations = tmp_path / "mosdac.toml"
    locations.write_text('[[location]]\ncode = "ESEPL"\nname = "ESEPL"\nlatitude = 17.4042\nlongitude = 77.3753\n'
                         'coordinate_source = "test"\n', encoding="utf-8")
    cfg = settings(mosdac_locations_file=locations)
    sched = schedule("MOSDAC_SOLAR", checker="mosdac", params={"kind": "solar"})
    body = json.dumps(mosdac_support.solar_records(289)).encode()
    route = Recorder(lambda r: httpx.Response(200, content=body, headers={"content-type": "application/json"}))
    series = datetime(2030, 1, 10, 5, 30, tzinfo=IST)
    new = MosdacChecker(sched).check(ctx(sched, now=ist(25, 9), state=FakeState(), handler=route, cfg=cfg))
    [d] = new.detections
    assert d.item_key == f"series|{series.isoformat()}"
    assert d.target == {"runner": "mosdac", "command": "solar", "series_start": series.isoformat()}
    assert len(route.requests) == 1                                     # one probe, not 16 requests
    same = MosdacChecker(sched).check(ctx(sched, now=ist(25, 12), state=FakeState(mosdac={"ESEPL": series}),
                                          handler=route, cfg=cfg))
    assert same.outcome is CheckOutcome.NO_NEW_DATA


def test_mosdac_server_error_propagates_for_classification(tmp_path) -> None:
    locations = tmp_path / "mosdac.toml"
    locations.write_text('[[location]]\ncode = "ESEPL"\nname = "E"\nlatitude = 17.4\nlongitude = 77.3\n'
                         'coordinate_source = "t"\n', encoding="utf-8")
    sched = schedule("MOSDAC_WIND", checker="mosdac", params={"kind": "wind"})
    from src.common.exceptions import HttpStatusError
    from src.scheduler.retry import classify_exception
    from src.scheduler.models import ErrorClass

    with pytest.raises(HttpStatusError) as caught:
        MosdacChecker(sched).check(ctx(sched, now=ist(25, 9), state=FakeState(),
                                       handler=lambda r: httpx.Response(500, json={"status": "Internal Server Error"}),
                                       cfg=settings(mosdac_locations_file=locations)))
    assert classify_exception(caught.value) is ErrorClass.TRANSIENT


# =========================================================================== Open-Meteo
def test_open_meteo_refresh_slots_and_downtime_collapse() -> None:
    sched = schedule("OPEN_METEO_FORECAST", checker="open_meteo_forecast", params={"slots": ["01:30", "07:30",
                                                                                              "13:30", "19:30"]})
    checker = OpenMeteoForecastChecker(sched)
    result = checker.check(ctx(sched, now=ist(25, 14), state=FakeState(), handler=lambda r: httpx.Response(500)))
    [d] = result.detections
    assert d.item_key == f"slot|{ist(25, 13, 30).isoformat()}" and d.target["kind"] == "forecast"
    done = {d.item_key: Watermark(d.item_key, "refresh", WatermarkStatus.INGESTED, ist(25, 13, 40))}
    assert checker.check(ctx(sched, now=ist(25, 19), state=FakeState(), handler=lambda r: httpx.Response(500),
                             watermarks=done)).detections == []
    # down from 25-Nov 14:00 until 26-Nov 09:00: only the latest slot (07:30) is fetched, not every missed one
    [late] = checker.check(ctx(sched, now=ist(26, 9), state=FakeState(), handler=lambda r: httpx.Response(500),
                               watermarks=done)).detections
    assert late.item_key == f"slot|{ist(26, 7, 30).isoformat()}"
    # just after midnight the latest slot is yesterday's 19:30
    [night] = checker.check(ctx(sched, now=ist(26, 0, 10), state=FakeState(),
                                handler=lambda r: httpx.Response(500))).detections
    assert night.item_key == f"slot|{ist(25, 19, 30).isoformat()}"


def test_open_meteo_historical_targets_lagged_days_without_watermark() -> None:
    sched = schedule("OPEN_METEO_HISTORICAL", checker="open_meteo_historical", catchup_max_days=2,
                     params={"lag_days": 6})
    marks = {"2026-11-18": Watermark("2026-11-18", "archive", WatermarkStatus.INGESTED, ist(24, 3))}
    result = OpenMeteoHistoricalChecker(sched).check(ctx(sched, now=ist(25, 3, 30), state=FakeState(),
                                                         handler=lambda r: httpx.Response(500), watermarks=marks))
    assert [d.item_key for d in result.detections] == ["2026-11-19"]


def test_utc_instant_maps_to_the_local_day() -> None:
    sched = cea_schedule()
    late_utc = datetime(2026, 11, 24, 19, 0, tzinfo=timezone.utc)        # = 25-Nov 00:30 IST
    c = ctx(sched, now=late_utc, state=FakeState(), handler=lambda r: httpx.Response(404))
    assert c.today == date(2026, 11, 25) and c.first_check_of_day
    # deadlines count from local (IST) midnight of the reference day: 25-Nov 00:00 IST + 0.5 h = now
    assert c.is_late(date(2026, 11, 25), 0.5) and not c.is_late(date(2026, 11, 25), 0.6)
    assert c.is_late(date(2026, 11, 24), 24.5) and not c.is_late(date(2026, 11, 24), 24.6)
    assert c.local_moment(date(2026, 11, 25)) - late_utc == timedelta(minutes=-30)
