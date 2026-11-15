"""WBES PX parsing, response envelopes and the lineage hook (synthetic data, no network)."""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from src.ingestion.base import BaseIngestionPipeline
from src.loaders.table_loader import DEFAULT_LINEAGE_COLUMNS
from src.pipelines.wbes import api, reference
from src.pipelines.wbes.client import decode_json
from src.pipelines.wbes.errors import WbesResponseError
from src.pipelines.wbes.parse import PxContext, parse_created_on, parse_px_records, resolve_created_on
from src.pipelines.wbes.pipeline import PX_LINEAGE_COLUMNS, PX_PIPELINES
from tests.support.wbes import envelope, px_record, series

IST = ZoneInfo("Asia/Kolkata")
D = date(2026, 11, 10)
RETRIEVED = datetime(2026, 11, 11, 12, 0, tzinfo=timezone.utc)        # the delivery day D is complete


def ctx(day: date = D, retrieved: datetime = RETRIEVED, naive: str = "Asia/Kolkata") -> PxContext:
    return PxContext(day, "IEX", 1, "DAM", 11, retrieved, naive)


def parse(records: list, **kwargs: object) -> object:
    # Round-trip through JSON exactly as the client decodes responses (numbers as Decimal).
    return parse_px_records(decode_json(json.dumps(records).encode()), ctx(**kwargs))


# ------------------------------------------------------------------ CreatedOn

@pytest.mark.parametrize(("value", "expected"), [
    ("2026-11-09T14:05:31", datetime(2026, 11, 9, 14, 5, 31, tzinfo=IST)),            # no offset -> IST
    ("2026-11-09T14:05:31.123", datetime(2026, 11, 9, 14, 5, 31, 123000, tzinfo=IST)),
    ("2026-11-09T08:35:31Z", datetime(2026, 11, 9, 8, 35, 31, tzinfo=timezone.utc)),
    ("2026-11-09T14:05:31+05:30", datetime(2026, 11, 9, 8, 35, 31, tzinfo=timezone.utc)),
    ("09-11-2026 14:05:31", datetime(2026, 11, 9, 14, 5, 31, tzinfo=IST)),
    ("09/11/2026 14:05", datetime(2026, 11, 9, 14, 5, tzinfo=IST)),
    ("/Date(1762677331000)/", datetime.fromtimestamp(1762677331, tz=timezone.utc)),
])
def test_created_on_formats(value: str, expected: datetime) -> None:
    assert parse_created_on(value, RETRIEVED, "Asia/Kolkata") == expected       # configured: naive = IST


def test_created_on_without_offset_is_conservative_while_unverified() -> None:
    # Retrieved well after both readings: the later (UTC) reading is used -> never earlier than the truth.
    value, note = resolve_created_on("2026-11-09T14:05:31", RETRIEVED, "unverified")
    assert value == datetime(2026, 11, 9, 14, 5, 31, tzinfo=timezone.utc) and "later reading (UTC)" in note
    # UTC reading would be after retrieval (impossible) -> IST reading.
    retrieved = datetime(2026, 11, 9, 10, 0, tzinfo=timezone.utc)                # 15:30 IST
    value, note = resolve_created_on("2026-11-09T14:05:31", retrieved, "unverified")
    assert value == datetime(2026, 11, 9, 14, 5, 31, tzinfo=IST) and "IST reading" in note
    # Explicit offsets and epoch values are exact; configured zones are applied without a note.
    assert resolve_created_on("2026-11-09T14:05:31+05:30", RETRIEVED, "unverified") == (
        datetime(2026, 11, 9, 14, 5, 31, tzinfo=IST), None)
    assert resolve_created_on("2026-11-09T14:05:31", RETRIEVED, "UTC") == (
        datetime(2026, 11, 9, 14, 5, 31, tzinfo=timezone.utc), None)


def test_unverified_created_on_is_reported_and_drives_availability() -> None:
    result = parse([px_record(D, 1)], naive="unverified")
    assert ("all", "CREATED_ON_TIMEZONE_UNVERIFIED") in [(i[0], i[1]) for i in result.info]
    first = result.records["region"][0].values
    assert first["source_available_at"] == datetime(2026, 11, 9, 14, 5, 31, tzinfo=timezone.utc)


@pytest.mark.parametrize("value", [None, "", "yesterday", True, "31-31-2026 10:00"])
def test_unrecognised_created_on_is_none(value: object) -> None:
    assert parse_created_on(value) is None


# ------------------------------------------------------------------- envelopes

def test_exchange_and_market_discovery() -> None:
    bare = [{"PowerExchangeTypeName": "IEX", "PowerExchangeTypeId": 1}, {"PowerExchangeTypeName": " HPX ",
                                                                         "PowerExchangeTypeId": 3}]
    assert [(i.name, i.item_id) for i in api.parse_exchanges(bare)] == [("IEX", 1), ("HPX", 3)]
    wrapped = envelope([{"PowerExTransactionTypeName": "RTM", "PowerExTransactionTypeId": 12}])
    assert [i.name for i in api.parse_markets(wrapped)] == ["RTM"]
    assert api.parse_markets(envelope([])) == []


@pytest.mark.parametrize("payload", [
    "text", [{"PowerExchangeTypeName": "IEX"}], [{"PowerExchangeTypeName": "IEX", "PowerExchangeTypeId": 1}] * 2,
    envelope([], code="WBES-500", message="Server error"), {"no": "body"},
])
def test_bad_exchange_lists_are_errors(payload: object) -> None:
    with pytest.raises(WbesResponseError):
        api.parse_exchanges(payload)


def test_details_envelope_success_empty_missing_status_and_errors() -> None:
    assert api.parse_details(envelope([{"RevisionNo": 1}])).status_code == "WBES-200"
    assert api.parse_details(envelope([])).records == []
    assert api.parse_details({"ResponseBody": None}).records == []
    missing = api.parse_details(envelope([{"RevisionNo": 1}], code=None))
    assert missing.status_code is None and len(missing.records) == 1
    with pytest.raises(WbesResponseError, match="WBES-401: Invalid request"):
        api.parse_details(envelope([], code="WBES-401", message="Invalid request"))
    for bad in ([], {"ResponseBody": {"x": 1}}, {"ResponseBody": [1, 2]}):
        with pytest.raises(WbesResponseError):
            api.parse_details(bad)


def test_details_request_and_date_range() -> None:
    assert api.details_request(D, 1, 11) == {"InputDate": "10-11-2026", "PowerExchangeTypeId": 1,
                                             "PowerExTransactionTypeId": 11, "RevisionNo": -1}
    api.validate_date_range(D, D + timedelta(days=1), today=D)       # day-ahead allowed
    for start, end in ((D, D + timedelta(days=2)), (D, D - timedelta(days=1)), (D - timedelta(days=40), D)):
        with pytest.raises(ValueError):
            api.validate_date_range(start, end, today=D)


# ---------------------------------------------------------------------- parsing

def test_all_three_parts_96_blocks_exact_values_zeros_and_lineage() -> None:
    result = parse([px_record(D, 4)])
    assert {p: len(result.records[p]) for p in result.records} == {"region": 6 * 96, "link": 2 * 96, "entity": 3 * 96}
    assert not any(result.failures.values()) and not result.warnings
    region1 = [r.values for r in result.records["region"] if r.values["wbes_region_id"] == 1]
    assert [v["time_block"] for v in region1] == list(range(1, 97))
    assert (region1[0]["injection_value"], region1[1]["injection_value"]) == (Decimal("100.111"), Decimal("100.628"))
    assert region1[0]["region_code"] is None                         # mapping unverified: never assigned
    entity = next(r.values for r in result.records["entity"] if r.values["entity_acronym"] == "TEST_GEN_A")
    assert entity["drawal_value"] == Decimal("0.0") and entity["drawal_value"] is not None   # zero kept
    first = result.records["link"][0].values
    assert (first["revision_no"], first["power_ex_id"], first["availability_basis"]) == (4, "PXID-SYN-1",
                                                                                        "SOURCE_CREATED_ON")
    assert first["source_published_at"] == first["source_available_at"] == datetime(2026, 11, 9, 14, 5, 31, tzinfo=IST)
    assert first["net_amount_value"] == Decimal("-12.345")
    discom = next(r.values for r in result.records["entity"] if r.values["entity_acronym"] == "TEST_DISCOM_B")
    assert discom["entity_unit_acronym"] is None and discom["entity_px_code"] == "PXB1"


def test_region_code_only_when_mapping_verified(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(reference, "REGION_MAPPING_VERIFIED", True)
    result = parse([px_record(D, 1)])
    codes = {r.values["wbes_region_id"]: r.values["region_code"] for r in result.records["region"]}
    assert codes == {1: "ER", 2: "WR", 3: "NR", 4: "SR", 5: "NER", 6: None}     # NLDC stays NULL


@pytest.mark.parametrize(("item", "message"), [
    ({"Region": 1, "Injection": series(1.111)[:95], "Drawal": series(2.222)}, "has 95 values"),
    ({"Region": 1, "Injection": series(1.111), "Drawal": series(2.222) + [3.333]}, "has 97 values"),
    ({"Region": 1, "Injection": series(1.111)}, "Drawal is missing"),
    ({"Region": 1, "Injection": "1,2,3", "Drawal": series(2.222)}, "Injection is str"),
    ({"Region": 1, "Injection": ["x"] + series(1.111)[1:], "Drawal": series(2.222)}, "not a number"),
    ({"Region": 1, "Injection": [True] + series(1.111)[1:], "Drawal": series(2.222)}, "boolean"),
    ({"Region": "ER", "Injection": series(1.111), "Drawal": series(2.222)}, "not an integer id"),
])
def test_malformed_arrays_reject_the_item_never_padded(item: dict, message: str) -> None:
    result = parse([px_record(D, 1, regions=[item])])
    assert result.records["region"] == []
    assert len(result.failures["region"]) == 1 and message in result.failures["region"][0].description


def test_null_values_are_kept_as_null_and_reported() -> None:
    values = series(5.555)
    values[9] = None
    result = parse([px_record(D, 1, links=[{"LinkNumber": 3, "LinkName": "TEST L", "NetAmount": values}])])
    assert result.records["link"][9].values["net_amount_value"] is None
    assert [w[1] for w in result.warnings] == ["MISSING_VALUE"]


def test_unknown_region_is_reported_and_kept() -> None:
    result = parse([px_record(D, 1, regions=[{"Region": 9, "Injection": series(1.111), "Drawal": series(2.222)}])])
    assert {r.values["wbes_region_id"] for r in result.records["region"]} == {9}
    assert [w[1] for w in result.warnings] == ["UNKNOWN_REGION"]


def test_revisions_sorted_duplicates_missing_and_wrong_date_rejected() -> None:
    result = parse([px_record(D, 3), px_record(D, 1), px_record(D, 2)])
    assert [r.revision_no for r in result.revisions] == [1, 2, 3]
    assert [r.values["revision_no"] for r in result.records["link"][::96]] == [1, 1, 2, 2, 3, 3]
    bad = parse([px_record(D, 5), px_record(D, 5), px_record(D, 6, RevisionNo="x"),
                 px_record(D + timedelta(days=1), 7)])
    assert bad.records["region"] == [] and len(bad.failures["region"]) == 3
    assert {f.description.split(" ")[0] for f in bad.failures["region"]} == {"RevisionNo", "PX", "EffectiveForDate"}


def test_duplicate_entity_key_within_a_revision_is_marked() -> None:
    twins = [{"EntityAcronym": "TEST_X", "EntityUnitAcronym": "U", "EntityPxCode": "P", "EntityRegionId": 1,
              "Injection": series(1.111), "Drawal": series(2.222)}] * 2
    result = parse([px_record(D, 1, entities=twins)])
    assert len(result.records["entity"]) == 192 and all(r.raw["duplicate_key"] for r in result.records["entity"])


def test_items_absent_in_a_later_revision_are_reported() -> None:
    from tests.support.wbes import entity_items
    result = parse([px_record(D, 1), px_record(D, 2, entities=entity_items()[:2])])
    assert [(i[0], i[1]) for i in result.info] == [("entity", "ABSENT_IN_REVISION")]


def test_missing_link_name_and_entity_acronym_are_rejected() -> None:
    result = parse([px_record(D, 1, links=[{"LinkNumber": 9, "NetAmount": series(1.111)}],
                              entities=[{"EntityUnitAcronym": "U", "Injection": series(1.111),
                                         "Drawal": series(2.222)}])])
    assert "LinkName" in result.failures["link"][0].description
    assert "EntityAcronym" in result.failures["entity"][0].description


def test_created_on_unparsed_or_in_future_uses_retrieval_time() -> None:
    result = parse([px_record(D, 1, created_on="not a time"), px_record(D, 2, created_on="2026-11-12T10:00:00+05:30")])
    assert [w[1] for w in result.warnings] == ["CREATED_ON_UNPARSED", "CREATED_ON_IN_FUTURE"]
    assert {(r.availability_basis, r.available_at) for r in result.revisions} == {("RETRIEVED_AT", RETRIEVED)}
    assert result.records["region"][0].values["source_published_at"] is None


def test_section_shapes_and_absence() -> None:
    absent = parse([{k: v for k, v in px_record(D, 1).items() if k != "AllLinkPowerExJsonData"}])
    assert absent.records["link"] == [] and ("link", "SECTION_ABSENT") in [(i[0], i[1]) for i in absent.info]
    broken = parse([px_record(D, 1, AllRegionPowerExJsonData=[1, 2])])
    assert broken.records["region"] == [] and "not an object" in broken.failures["region"][0].description


# ------------------------------------------------------------------ lineage hook

def test_lineage_columns_never_include_measurements_or_keys() -> None:
    for cls in PX_PIPELINES.values():
        assert not set(cls.measurement_columns) & (PX_LINEAGE_COLUMNS | DEFAULT_LINEAGE_COLUMNS)
        assert not set(cls.natural_key) & PX_LINEAGE_COLUMNS
        assert cls.extra_lineage_columns == PX_LINEAGE_COLUMNS


def test_base_hook_extends_ignored_columns_and_rejects_key_overlap() -> None:
    from src.ingestion.catalog import SOURCES
    from src.ingestion.models import DatasetDefinition

    class Dummy(BaseIngestionPipeline):
        source = SOURCES["WBES"]
        dataset = DatasetDefinition("DUMMY", "dummy", None, None, None)
        target_table = "wbes_px_link_schedule"
        natural_key = ("delivery_date", "link_name")
        extra_lineage_columns = frozenset({"revision_no"})

        def fetch(self, context):  # pragma: no cover
            return []

        def parse(self, document, context):  # pragma: no cover
            raise NotImplementedError

    pipeline = Dummy(database=object(), raw_storage=object())
    assert pipeline.loader.ignore_columns == DEFAULT_LINEAGE_COLUMNS | {"revision_no"}

    class Bad(Dummy):
        extra_lineage_columns = frozenset({"link_name"})
    with pytest.raises(TypeError, match="overlap the natural key"):
        Bad(database=object(), raw_storage=object())


# ------------------------------------------------- days not yet complete (I-3)

def test_short_series_for_a_day_not_yet_complete_is_skipped_not_rejected() -> None:
    today_retrieval = datetime(2026, 11, 10, 9, 0, tzinfo=timezone.utc)          # D is still in progress
    regions = [{"Region": 1, "Injection": series(1.111)[:40], "Drawal": series(2.222)[:40]},
               {"Region": 2, "Injection": series(3.333), "Drawal": series(4.444)}]
    result = parse([px_record(D, 1, regions=regions)], retrieved=today_retrieval)
    assert result.failures["region"] == []
    assert {r.values["wbes_region_id"] for r in result.records["region"]} == {2}
    assert [(i[0], i[1]) for i in result.info] == [("region", "INCOMPLETE_SERIES")]
    assert result.published_keys["region"][1] == {(1,), (2,)}                  # still counts as published
    too_long = parse([px_record(D, 1, regions=[{"Region": 1, "Injection": series(1.111) + [1.0],
                                                "Drawal": series(2.222)}])], retrieved=today_retrieval)
    assert "has 97 values" in too_long.failures["region"][0].description         # longer is always malformed
    complete = parse([px_record(D, 1, regions=regions[:1])])                    # same short arrays, complete day
    assert "has 40 values" in complete.failures["region"][0].description


def test_null_blocks_for_a_day_not_yet_complete_are_info() -> None:
    values = series(5.555)
    values[90:] = [None] * 6
    result = parse([px_record(D, 1, links=[{"LinkNumber": 3, "LinkName": "TEST L", "NetAmount": values}])],
                   retrieved=datetime(2026, 11, 10, 9, 0, tzinfo=timezone.utc))
    assert result.warnings == [] and [(i[0], i[1]) for i in result.info] == [("link", "BLOCKS_NOT_YET_PUBLISHED")]
    assert result.records["link"][95].values["net_amount_value"] is None


def test_published_keys_include_rejected_items() -> None:
    regions = [{"Region": 1, "Injection": series(1.111)[:95], "Drawal": series(2.222)},
               {"Region": 2, "Injection": series(3.333), "Drawal": series(4.444)}]
    result = parse([px_record(D, 7, regions=regions)])
    assert result.published_keys["region"][7] == {(1,), (2,)} and len(result.failures["region"]) == 1
