"""WBES PX Dashboard endpoints and response envelopes (assessment, section 3).

* ``GET  /powerexchange/api/PowerExchange/GetPxTypeList/date/{dd-mm-yyyy}``      -> exchanges
  (``PowerExchangeTypeName`` / ``PowerExchangeTypeId``)
* ``GET  /powerexchange/api/PowerExchange/GetPxTransTypeList/date/{dd-mm-yyyy}`` -> markets
  (``PowerExTransactionTypeName`` / ``PowerExTransactionTypeId``)
* ``POST /powerexchange/api/PowerExchange/GetPowerExchangeByDetails`` with
  ``{InputDate, PowerExchangeTypeId, PowerExTransactionTypeId, RevisionNo: -1}`` -> records
  (``ResponseBody``: one record per revision, as far as known; ``ResponseStatus.Code``
  ``WBES-200`` means success)

The list endpoints are documented to return a bare JSON list; an envelope with
``ResponseBody`` is accepted too. Anything else is a :class:`WbesResponseError`
(never guessed). An unsuccessful ``ResponseStatus`` is an error; an empty
``ResponseBody`` means "not published yet" and is reported, not treated as data.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from src.pipelines.wbes.errors import WbesResponseError

IST = ZoneInfo("Asia/Kolkata")
SUCCESS_CODE = "WBES-200"
EXCHANGES_PATH = "/powerexchange/api/PowerExchange/GetPxTypeList/date/{day}"
MARKETS_PATH = "/powerexchange/api/PowerExchange/GetPxTransTypeList/date/{day}"
DETAILS_PATH = "/powerexchange/api/PowerExchange/GetPowerExchangeByDetails"
ALL_REVISIONS = -1
MAX_DAYS_PER_COMMAND = 31


def wbes_date(day: date) -> str:
    return day.strftime("%d-%m-%Y")


def today_ist() -> date:
    return datetime.now(IST).date()


def validate_date_range(start: date, end: date, *, today: date) -> None:
    if end < start:
        raise ValueError(f"end date {end} is before start date {start}")
    if (end - start).days + 1 > MAX_DAYS_PER_COMMAND:
        raise ValueError(f"at most {MAX_DAYS_PER_COMMAND} delivery days per command")
    if end > today + timedelta(days=1):
        raise ValueError(f"{end} is beyond tomorrow ({today + timedelta(days=1)}); day-ahead is the latest published")


@dataclass(frozen=True)
class ListedItem:
    item_id: Any
    name: str


def _body(payload: Any, what: str) -> Any:
    """``ResponseBody`` of an envelope, after checking ``ResponseStatus``; a bare list is returned as-is."""
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        raise WbesResponseError(f"{what}: expected a JSON object or list, got {type(payload).__name__}")
    status = payload.get("ResponseStatus")
    if isinstance(status, dict) and status.get("Code") not in (None, SUCCESS_CODE):
        raise WbesResponseError(f"{what}: WBES returned {status.get('Code')}: {status.get('Message')}")
    if "ResponseBody" not in payload:
        raise WbesResponseError(f"{what}: response has no ResponseBody")
    return payload["ResponseBody"]


def parse_listing(payload: Any, *, name_field: str, id_field: str, what: str) -> list[ListedItem]:
    body = _body(payload, what)
    if body is None:
        return []
    if not isinstance(body, list):
        raise WbesResponseError(f"{what}: expected a list")
    items = []
    for entry in body:
        if not isinstance(entry, dict) or not str(entry.get(name_field) or "").strip() or entry.get(id_field) is None:
            raise WbesResponseError(f"{what}: entry without {name_field}/{id_field}: {str(entry)[:120]}")
        items.append(ListedItem(entry[id_field], str(entry[name_field]).strip()))
    names = [i.name for i in items]
    if len(names) != len(set(names)):
        raise WbesResponseError(f"{what}: duplicate names {names}")
    return items


def parse_exchanges(payload: Any) -> list[ListedItem]:
    return parse_listing(payload, name_field="PowerExchangeTypeName", id_field="PowerExchangeTypeId",
                         what="exchange list")


def parse_markets(payload: Any) -> list[ListedItem]:
    return parse_listing(payload, name_field="PowerExTransactionTypeName", id_field="PowerExTransactionTypeId",
                         what="market list")


def details_request(day: date, exchange_id: Any, market_id: Any) -> dict[str, Any]:
    return {"InputDate": wbes_date(day), "PowerExchangeTypeId": exchange_id, "PowerExTransactionTypeId": market_id,
            "RevisionNo": ALL_REVISIONS}


@dataclass(frozen=True)
class DetailsEnvelope:
    records: list[dict[str, Any]]
    status_code: str | None          # None: the response had no ResponseStatus


def parse_details(payload: Any) -> DetailsEnvelope:
    """Records of a ``GetPowerExchangeByDetails`` response.

    Raises:
        WbesResponseError: Unsuccessful ResponseStatus, or not an object with a list ResponseBody.
    """
    if not isinstance(payload, dict):
        raise WbesResponseError(f"PX details: expected a JSON object, got {type(payload).__name__}")
    status = payload.get("ResponseStatus")
    code = status.get("Code") if isinstance(status, dict) else None
    body = _body(payload, "PX details")
    if body is None:
        body = []
    if not isinstance(body, list) or not all(isinstance(r, dict) for r in body):
        raise WbesResponseError("PX details: ResponseBody is not a list of records")
    return DetailsEnvelope(body, code)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)
