"""Parse a WBES ``GetPowerExchangeByDetails`` response into region, link and entity records.

One response covers one delivery date x power exchange x market and may hold
several revision records. Each record has (assessment, section 4):

* header: ``EffectiveForDate`` (dd-mm-yyyy), ``RevisionNo``, ``PowerExId``, ``CreatedOn``;
* ``AllRegionPowerExJsonData.RegionWiseJsonData[]``: ``Region``, ``Injection[]``, ``Drawal[]``;
* ``AllLinkPowerExJsonData.LinkWiseJsonData[]``: ``LinkNumber``, ``LinkName``, ``NetAmount[]``;
* ``AllEntityPowerExJsonData.EntityWiseJsonData[]``: ``EntityAcronym``, ``EntityUnitAcronym``,
  ``EntityPxCode``, ``EntityRegionId``, ``Injection[]``, ``Drawal[]``.

Rules:

* **Series length.** A value series must be a list of 96 entries. For a delivery day that
  had already ended when the response was retrieved (a *complete day*), any other length
  rejects the item (never padded). For a day not yet complete at retrieval (today or a
  future day), a *shorter* list is treated as "not yet completely published": the item is
  skipped with an ``INCOMPLETE_SERIES`` INFO note (no values stored, no position assumed)
  and loads in full on a later run. A longer list is always rejected.
* Entries must be numbers (exact ``Decimal``) or null (stored NULL; reported as
  ``MISSING_VALUE``, or ``BLOCKS_NOT_YET_PUBLISHED`` INFO for a day not yet complete).
  Zero is a value and is kept.
* Units and the ``NetAmount`` sign convention are unverified: values are stored as published.
* WBES region ids are kept; an unknown id is reported; ``region_code`` only when verified.
* Within one revision, a natural key that occurs twice rejects both items (W7).
* **CreatedOn.** Values with an explicit offset (or ``/Date(ms)/``) are exact. For values
  without an offset the time zone is configurable (``WBES_CREATED_ON_NAIVE_TIMEZONE``);
  while it is ``unverified`` (the default) the *later* plausible reading is used, so the
  availability time is never earlier than the truth (no look-ahead): the UTC reading,
  unless that would be after the retrieval time, then the IST reading. This is reported
  (``CREATED_ON_TIMEZONE_UNVERIFIED`` INFO). The original text stays in the raw files.
* For every revision and part, the keys of all items **published** in it are recorded
  (including items rejected or incomplete), so the pipelines can detect stored items that
  a newer revision no longer publishes.
"""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from src.ingestion.models import ParseFailure, ParsedRecord
from src.pipelines.wbes.api import IST
from src.pipelines.wbes.reference import EXPECTED_BLOCKS, is_known_region, region_code_for

PARTS = ("region", "link", "entity")
CREATED_ON_FUTURE_TOLERANCE = timedelta(minutes=5)
NAIVE_TIMEZONES = ("unverified", "Asia/Kolkata", "UTC")

_DOTNET_DATE = re.compile(r"^/Date\((-?\d+)([+-]\d{4})?\)/$")
_LOCAL_FORMATS = ("%d-%m-%Y %H:%M:%S", "%d-%m-%Y %H:%M", "%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M")


def read_created_on(value: Any) -> datetime | None:
    """WBES ``CreatedOn`` as published: aware if it carries an offset (or is ``/Date(ms)/``), else naive.

    Recognised: ISO 8601, ``dd-mm-yyyy HH:MM[:SS]``, ``dd/mm/yyyy HH:MM[:SS]`` and
    ``/Date(ms[+hhmm])/`` (epoch milliseconds, an absolute instant). None if not recognised.
    """
    if value is None or isinstance(value, bool):
        return None
    text = str(value).strip()
    if not text:
        return None
    match = _DOTNET_DATE.match(text)
    if match:
        return datetime.fromtimestamp(int(match[1]) / 1000, tz=timezone.utc)
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        pass
    for fmt in _LOCAL_FORMATS:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def resolve_created_on(value: Any, retrieved_at: datetime, naive_timezone: str = "unverified",
                       ) -> tuple[datetime | None, str | None]:
    """Aware ``CreatedOn`` and an optional note explaining how a value without offset was read."""
    parsed = read_created_on(value)
    if parsed is None or parsed.tzinfo is not None:
        return parsed, None
    if naive_timezone == "Asia/Kolkata":
        return parsed.replace(tzinfo=IST), None
    if naive_timezone == "UTC":
        return parsed.replace(tzinfo=timezone.utc), None
    as_utc, as_ist = parsed.replace(tzinfo=timezone.utc), parsed.replace(tzinfo=IST)   # as_ist is 5:30 earlier
    if as_utc <= retrieved_at + CREATED_ON_FUTURE_TOLERANCE:
        return as_utc, ("CreatedOn has no offset and its time zone is unverified; the later reading (UTC) is used "
                        "so availability is never earlier than the truth")
    return as_ist, ("CreatedOn has no offset and its time zone is unverified; the UTC reading would be after "
                    "retrieval, so the IST reading is used")


def parse_created_on(value: Any, retrieved_at: datetime | None = None, naive_timezone: str = "unverified",
                     ) -> datetime | None:
    """Convenience wrapper of :func:`resolve_created_on` (retrieval time defaults to now)."""
    return resolve_created_on(value, retrieved_at or datetime.now(timezone.utc), naive_timezone)[0]


class SeriesError(ValueError):
    pass


class IncompleteSeries(SeriesError):
    """A shorter-than-96 series for a delivery day that was not yet complete at retrieval."""


def _series(values: Any, label: str, *, complete_day: bool) -> list[Decimal | None]:
    if not isinstance(values, list):
        raise SeriesError(f"{label} is {'missing' if values is None else type(values).__name__}, expected a list")
    if len(values) < EXPECTED_BLOCKS and not complete_day:
        raise IncompleteSeries(f"{label} has {len(values)} of {EXPECTED_BLOCKS} values")
    if len(values) != EXPECTED_BLOCKS:
        raise SeriesError(f"{label} has {len(values)} values, expected {EXPECTED_BLOCKS}")
    out: list[Decimal | None] = []
    for index, value in enumerate(values, start=1):
        if value is None:
            out.append(None)
        elif isinstance(value, bool):
            raise SeriesError(f"{label}[{index}] is a boolean")
        elif isinstance(value, (int, Decimal)):
            out.append(Decimal(value))
        elif isinstance(value, float):
            out.append(Decimal(repr(value)))
        else:
            raise SeriesError(f"{label}[{index}] is not a number: {str(value)[:30]!r}")
    return out


def _int_or_none(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = Decimal(str(value))
    except InvalidOperation:
        return None
    return int(number) if number == number.to_integral_value() else None


def _text(value: Any) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None


def item_key(part: str, item: dict[str, Any]) -> tuple | None:
    """Item identity (natural key without delivery date/exchange/market/block); None if not identifiable."""
    if part == "region":
        region = _int_or_none(item.get("Region"))
        return None if region is None else (region,)
    if part == "link":
        name = _text(item.get("LinkName"))
        return None if name is None else (name,)
    acronym = _text(item.get("EntityAcronym"))
    return None if acronym is None else (acronym, _text(item.get("EntityUnitAcronym")), _text(item.get("EntityPxCode")))


@dataclass
class RevisionInfo:
    revision_no: int
    power_ex_id: str | None
    created_on_raw: Any
    created_on: datetime | None
    availability_basis: str
    available_at: datetime


@dataclass
class PxParseResult:
    records: dict[str, list[ParsedRecord]] = field(default_factory=lambda: {p: [] for p in PARTS})
    failures: dict[str, list[ParseFailure]] = field(default_factory=lambda: {p: [] for p in PARTS})
    revisions: list[RevisionInfo] = field(default_factory=list)
    # part -> revision -> keys of every item published in that revision (loaded, rejected or incomplete)
    published_keys: dict[str, dict[int, set[tuple]]] = field(default_factory=lambda: {p: {} for p in PARTS})
    warnings: list[tuple[str, str, str, str]] = field(default_factory=list)   # (part, code, description, key)
    info: list[tuple[str, str, str, str]] = field(default_factory=list)

    def add_warning(self, part: str, code: str, description: str, key: str) -> None:
        self.warnings.append((part, code, description, key))

    def add_info(self, part: str, code: str, description: str, key: str) -> None:
        self.info.append((part, code, description, key))


@dataclass(frozen=True)
class PxContext:
    delivery_date: date
    power_exchange: str
    power_exchange_type_id: Any
    market: str
    market_type_id: Any
    retrieved_at: datetime
    naive_timezone: str = "unverified"

    @property
    def complete_day(self) -> bool:
        """True if the delivery day had ended (IST) when the response was retrieved."""
        return self.delivery_date < self.retrieved_at.astimezone(IST).date()


def parse_px_records(records: list[dict[str, Any]], ctx: PxContext) -> PxParseResult:
    """Parse every revision record of one PX response, in ascending revision order."""
    result = PxParseResult()
    base = f"{ctx.delivery_date}|{ctx.power_exchange}|{ctx.market}"
    ordered: list[tuple[int, dict[str, Any]]] = []
    for position, record in enumerate(records):
        revision = _int_or_none(record.get("RevisionNo"))
        key = f"{base}|record{position + 1}"
        if revision is None:
            for part in PARTS:
                result.failures[part].append(ParseFailure(key, {"RevisionNo": record.get("RevisionNo")},
                                                          "PX record without an integer RevisionNo; not loaded"))
            continue
        ordered.append((revision, record))
    revisions = [r for r, _ in ordered]
    for duplicate in sorted({r for r in revisions if revisions.count(r) > 1}):
        for part in PARTS:
            result.failures[part].append(ParseFailure(f"{base}|rev{duplicate}", {"RevisionNo": duplicate},
                                                      f"RevisionNo {duplicate} appears more than once in one response; "
                                                      "none of those records loaded"))
    ordered = [(r, rec) for r, rec in ordered if revisions.count(r) == 1]
    ordered.sort(key=lambda item: item[0])

    previous_keys: dict[str, set[tuple]] = {}
    for revision, record in ordered:
        rkey = f"{base}|rev{revision}"
        effective = _text(record.get("EffectiveForDate"))
        try:
            effective_date = datetime.strptime(effective or "", "%d-%m-%Y").date()
        except ValueError:
            effective_date = None
        if effective_date != ctx.delivery_date:
            for part in PARTS:
                result.failures[part].append(ParseFailure(rkey, {"EffectiveForDate": effective},
                                                          f"EffectiveForDate {effective!r} does not match the requested "
                                                          f"delivery date {ctx.delivery_date}; not loaded"))
            continue
        created_raw = record.get("CreatedOn")
        created, note = resolve_created_on(created_raw, ctx.retrieved_at, ctx.naive_timezone)
        basis, available = "RETRIEVED_AT", ctx.retrieved_at
        if created is None:
            result.add_warning("all", "CREATED_ON_UNPARSED",
                               f"Revision {revision}: CreatedOn {str(created_raw)[:40]!r} not recognised; "
                               "availability = retrieval time", rkey)
        elif created > ctx.retrieved_at + CREATED_ON_FUTURE_TOLERANCE:
            result.add_warning("all", "CREATED_ON_IN_FUTURE",
                               f"Revision {revision}: CreatedOn {created.isoformat()} is after retrieval "
                               f"{ctx.retrieved_at.isoformat()}; availability = retrieval time", rkey)
            created = None
        else:
            basis, available = "SOURCE_CREATED_ON", created
            if note:
                result.add_info("all", "CREATED_ON_TIMEZONE_UNVERIFIED",
                                f"Revision {revision}: {note} ({str(created_raw)[:40]!r} -> {created.isoformat()})",
                                rkey)
        info = RevisionInfo(revision, _text(record.get("PowerExId")), created_raw, created, basis, available)
        result.revisions.append(info)
        common = {
            "delivery_date": ctx.delivery_date, "power_exchange": ctx.power_exchange,
            "power_exchange_type_id": _int_or_none(ctx.power_exchange_type_id), "market": ctx.market,
            "market_type_id": _int_or_none(ctx.market_type_id), "revision_no": revision,
            "power_ex_id": info.power_ex_id, "retrieved_at": ctx.retrieved_at, "source_published_at": created,
            "source_available_at": available, "availability_basis": basis,
        }
        keys_now: dict[str, set[tuple]] = {}
        for part, outer, inner, builder in (
            ("region", "AllRegionPowerExJsonData", "RegionWiseJsonData", _region_rows),
            ("link", "AllLinkPowerExJsonData", "LinkWiseJsonData", _link_rows),
            ("entity", "AllEntityPowerExJsonData", "EntityWiseJsonData", _entity_rows),
        ):
            try:
                items = _section(record, outer, inner, result, rkey, part)
            except _SectionError as exc:
                result.failures[part].append(ParseFailure(f"{rkey}|{part}", {"section": outer}, f"{exc}; "
                                                          f"no {part} rows loaded for revision {revision}"))
                keys_now[part] = previous_keys.get(part, set())
                continue      # the section is unreadable: its published items are unknown (no absence check)
            published: set[tuple] = set()
            built = []      # (item key tuple, [ParsedRecord])
            for position, item in enumerate(items, start=1):
                ikey = f"{rkey}|{part}{position}"
                if not isinstance(item, dict):
                    result.failures[part].append(ParseFailure(ikey, {"item": str(item)[:120]},
                                                              f"{part} item {position} is not an object"))
                    continue
                identity = item_key(part, item)
                if identity is not None:
                    published.add(identity)
                try:
                    built.append(builder(item, common, ikey, result, ctx.complete_day))
                except IncompleteSeries as exc:
                    result.add_info(part, "INCOMPLETE_SERIES",
                                    f"{part} item {position} {identity}: {exc} for a delivery day not yet complete "
                                    "at retrieval; not loaded now (no values assumed), loads once complete", ikey)
                except SeriesError as exc:
                    result.failures[part].append(ParseFailure(ikey, _raw_item(item), f"{part} item {position}: {exc}; "
                                                              "item not loaded (arrays are never padded)"))
                except _ItemError as exc:
                    result.failures[part].append(ParseFailure(ikey, _raw_item(item), f"{part} item {position}: {exc}"))
            result.published_keys[part][revision] = published
            counts: dict[tuple, int] = defaultdict(int)
            for identity, _ in built:
                counts[identity] += 1
            for identity, rows in built:
                if counts[identity] > 1:
                    for row in rows:
                        row.raw["duplicate_key"] = True
                result.records[part].extend(rows)
            keys_now[part] = published
            dropped = previous_keys.get(part, set()) - keys_now[part]
            if dropped:
                result.add_info(part, "ABSENT_IN_REVISION",
                                f"Revision {revision}: {len(dropped)} {part} item(s) of the previous revision in this "
                                f"response are not published any more: {sorted(map(str, dropped))[:5]}", rkey)
        previous_keys = keys_now
    return result


class _ItemError(ValueError):
    pass


def _raw_item(item: dict[str, Any]) -> dict[str, Any]:
    """Identifying fields of an item for the rejects file (the full item stays in the raw response)."""
    return {k: v for k, v in item.items() if not isinstance(v, list)}


def _section(record: dict[str, Any], outer: str, inner: str, result: PxParseResult, rkey: str,
             part: str) -> list[Any]:
    container = record.get(outer)
    if container is None:
        result.add_info(part, "SECTION_ABSENT", f"{outer} not present in the record", rkey)
        return []
    if not isinstance(container, dict):
        raise _SectionError(f"{outer} is not an object")
    items = container.get(inner)
    if items is None:
        return []
    if not isinstance(items, list):
        raise _SectionError(f"{outer}.{inner} is not a list")
    return items


class _SectionError(ValueError):
    pass


def _nulls(result: PxParseResult, part: str, ikey: str, label: str, series: list[Decimal | None],
           complete_day: bool) -> None:
    missing = [i for i, v in enumerate(series, start=1) if v is None]
    if not missing:
        return
    where = f"{len(missing)} null value(s) at blocks {missing[:10]}{'...' if len(missing) > 10 else ''}; stored as NULL"
    if complete_day:
        result.add_warning(part, "MISSING_VALUE", f"{label}: {where}", ikey)
    else:
        result.add_info(part, "BLOCKS_NOT_YET_PUBLISHED", f"{label}: {where} (delivery day not yet complete)", ikey)


def _region_check(result: PxParseResult, part: str, region_id: int | None, ikey: str, label: str) -> None:
    if region_id is not None and not is_known_region(region_id):
        result.add_warning(part, "UNKNOWN_REGION", f"{label}: WBES region id {region_id} is not a known region "
                           "(1-6); stored with region_code NULL", ikey)


def _region_rows(item: dict[str, Any], common: dict[str, Any], ikey: str, result: PxParseResult,
                 complete_day: bool) -> tuple[tuple, list[ParsedRecord]]:
    region_id = _int_or_none(item.get("Region"))
    if region_id is None:
        raise _ItemError(f"Region {item.get('Region')!r} is not an integer id; not loaded")
    injection = _series(item.get("Injection"), "Injection", complete_day=complete_day)
    drawal = _series(item.get("Drawal"), "Drawal", complete_day=complete_day)
    _region_check(result, "region", region_id, ikey, f"region {region_id}")
    _nulls(result, "region", ikey, f"region {region_id} Injection", injection, complete_day)
    _nulls(result, "region", ikey, f"region {region_id} Drawal", drawal, complete_day)
    raw = {"Region": item.get("Region")}
    rows = [ParsedRecord(f"{ikey}|b{block}", {**common, "wbes_region_id": region_id,
                                              "region_code": region_code_for(region_id), "time_block": block,
                                              "injection_value": inj, "drawal_value": drw}, dict(raw))
            for block, (inj, drw) in enumerate(zip(injection, drawal), start=1)]
    return (region_id,), rows


def _link_rows(item: dict[str, Any], common: dict[str, Any], ikey: str, result: PxParseResult,
               complete_day: bool) -> tuple[tuple, list[ParsedRecord]]:
    name = _text(item.get("LinkName"))
    if name is None:
        raise _ItemError(f"link without LinkName (LinkNumber {item.get('LinkNumber')!r}); not loaded")
    amounts = _series(item.get("NetAmount"), "NetAmount", complete_day=complete_day)
    _nulls(result, "link", ikey, f"link {name} NetAmount", amounts, complete_day)
    raw = {"LinkNumber": item.get("LinkNumber"), "LinkName": name}
    rows = [ParsedRecord(f"{ikey}|b{block}", {**common, "link_number": _int_or_none(item.get("LinkNumber")),
                                              "link_name": name, "time_block": block, "net_amount_value": value},
                         dict(raw))
            for block, value in enumerate(amounts, start=1)]
    return (name,), rows


def _entity_rows(item: dict[str, Any], common: dict[str, Any], ikey: str, result: PxParseResult,
                 complete_day: bool) -> tuple[tuple, list[ParsedRecord]]:
    acronym = _text(item.get("EntityAcronym"))
    if acronym is None:
        raise _ItemError("entity without EntityAcronym; not loaded")
    unit, px_code = _text(item.get("EntityUnitAcronym")), _text(item.get("EntityPxCode"))
    region_id = _int_or_none(item.get("EntityRegionId"))
    injection = _series(item.get("Injection"), "Injection", complete_day=complete_day)
    drawal = _series(item.get("Drawal"), "Drawal", complete_day=complete_day)
    label = f"entity {acronym}/{unit}/{px_code}"
    _region_check(result, "entity", region_id, ikey, label)
    _nulls(result, "entity", ikey, f"{label} Injection", injection, complete_day)
    _nulls(result, "entity", ikey, f"{label} Drawal", drawal, complete_day)
    raw = {"EntityAcronym": acronym, "EntityUnitAcronym": unit, "EntityPxCode": px_code,
           "EntityRegionId": item.get("EntityRegionId")}
    rows = [ParsedRecord(f"{ikey}|b{block}", {**common, "entity_acronym": acronym, "entity_unit_acronym": unit,
                                              "entity_px_code": px_code, "wbes_region_id": region_id,
                                              "region_code": region_code_for(region_id), "time_block": block,
                                              "injection_value": inj, "drawal_value": drw}, dict(raw))
            for block, (inj, drw) in enumerate(zip(injection, drawal), start=1)]
    return (acronym, unit, px_code), rows
