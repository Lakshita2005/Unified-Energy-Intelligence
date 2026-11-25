"""Tests for the IEX snapshot volume parser, its validation and reading stored snapshot files (no network, no
database)."""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from src.common.exceptions import StorageError
from src.common.raw_storage import LocalFileSystemBackend, RawStorageManager
from src.ingestion.validation import validate_records
from src.pipelines.iex import api
from src.pipelines.iex.errors import InvalidResponseError
from src.pipelines.iex.parser import SNAPSHOT_BREAKDOWN_COLUMNS, parse_snapshot_volumes
from src.pipelines.iex.pipeline import snapshot_validator
from src.pipelines.iex.runner import stored_snapshot_documents
from tests.support.iex import DAY, dam_record, envelope, gdam_record, snapshot


def _volumes(kind: str, content: bytes, start: date = DAY, end: date = DAY):
    return parse_snapshot_volumes(content, market=api.MARKETS[kind], start=start, end=end)


@pytest.mark.parametrize("kind", ["dam", "rtm"])
def test_dam_and_rtm_volumes_parse_exactly(kind: str) -> None:
    result = _volumes(kind, snapshot(kind))

    assert result.failures == [] and len(result.records) == 96
    values = result.records[0].values
    assert values["market_code"] == api.MARKETS[kind].market_code
    assert (values["delivery_date"], values["time_block"]) == (DAY, 1)
    assert (values["purchase_bid_mw"], values["sell_bid_mw"], values["mcv_mw"], values["final_scheduled_volume_mw"]) \
        == (Decimal("10000.00"), Decimal("15000.00"), Decimal("4400.00"), Decimal("4400.00"))
    assert values["congestion"] is False
    assert all(values[column] is None for column in SNAPSHOT_BREAKDOWN_COLUMNS)
    assert "price_inr_per_mwh" not in values and "area_code" not in values


def test_gdam_totals_and_breakdown_after_2026_07_08() -> None:
    values = _volumes("gdam", snapshot("gdam")).records[0].values

    assert (values["sell_bid_mw"], values["mcv_mw"], values["final_scheduled_volume_mw"]) \
        == (Decimal("410.9"), Decimal("385.19"), Decimal("385.19"))
    assert (values["wind_sell_bid_mw"], values["wind_mcv_mw"], values["wind_cleared_volume_mw"]) \
        == (Decimal(10), Decimal("9.5"), Decimal("9.5"))
    assert values["solar_sell_bid_mw"] is None and values["non_solar_mcv_mw"] is None


def test_gdam_breakdown_before_2026_07_08() -> None:
    day = date(2026, 7, 7)
    content = envelope([gdam_record(day, block, era="before") for block in range(1, 97)])
    values = _volumes("gdam", content, day, day).records[0].values

    assert values["solar_sell_bid_mw"] == Decimal(10) and values["non_solar_cleared_volume_mw"] == Decimal("9.5")
    assert values["wind_sell_bid_mw"] is None and values["dre_mcv_mw"] is None


def test_congestion_yes_is_true_and_other_values_are_parse_failures() -> None:
    content = envelope([dam_record(DAY, 1, congestion="YES"), dam_record(DAY, 2, congestion="MAYBE"),
                        dam_record(DAY, 3, mcv=None)])
    result = _volumes("dam", content)

    assert [r.values["congestion"] for r in result.records] == [True]
    assert [f.key for f in result.failures] == ["IEX_DAM|MKT|10-11-2026|00:15 - 00:30",
                                                "IEX_DAM|MKT|10-11-2026|00:30 - 00:45"]
    assert "congestion" in result.failures[0].description and "mcv" in result.failures[1].description


def test_unexpected_fields_still_stop_the_document() -> None:
    with pytest.raises(InvalidResponseError, match="API may have changed"):
        _volumes("dam", envelope([dam_record(DAY, 1, extra="x")]))


def test_negative_volume_is_rejected_by_validation() -> None:
    result = _volumes("dam", envelope([dam_record(DAY, 1, sell_bid="-1.00"), dam_record(DAY, 2)]))
    outcome = validate_records(result.records, snapshot_validator())

    assert len(outcome.accepted) == 1
    assert [i.code for i in outcome.rejected[0][1]] == ["OUT_OF_RANGE"]


# ------------------------------------------------------------------ stored files

def _store(storage: RawStorageManager, kind: str, area: str, start: date, end: date, content: bytes,
           at: datetime) -> str:
    name = f"iex_{kind}__{area}__{start}__{end}.json"
    return storage.save_bytes(content, source="iex", dataset=f"iex_{kind}", extension="json", ingested_at=at,
                              partition=f"{area}_{start}_{end}",
                              metadata={"document_name": name, "content_type": "application/json",
                                        "source_url": "https://iex.test"}).key


def test_stored_snapshot_files_are_verified_ordered_and_deduplicated(tmp_path: Path) -> None:
    storage = RawStorageManager(LocalFileSystemBackend(tmp_path))
    t = lambda h: datetime(2026, 11, 12, h, tzinfo=timezone.utc)  # noqa: E731
    later = _store(storage, "dam", "mkt", DAY, DAY, snapshot("dam", price=Decimal("1")), t(3))
    earlier = _store(storage, "dam", "mkt", DAY, DAY, snapshot("dam"), t(1))
    _store(storage, "dam", "mkt", DAY, DAY, snapshot("dam"), t(2))                     # identical re-fetch
    _store(storage, "dam", "n1", DAY, DAY, b"{}", t(1))                                # area price: not a snapshot
    _store(storage, "dam", "mkt", date(2026, 10, 1), date(2026, 10, 1), snapshot("dam"), t(4))

    documents, identical = stored_snapshot_documents(storage, api.DAM, start=DAY, end=DAY)

    assert [stored.key for _, stored in documents] == [earlier, later] and identical == 1
    document = documents[0][0]
    assert document.retrieved_at == t(1) and document.source_report_date == DAY
    assert document.document_name == "iex_dam__mkt__2026-11-10__2026-11-10.json"


def test_a_tampered_stored_file_is_refused(tmp_path: Path) -> None:
    storage = RawStorageManager(LocalFileSystemBackend(tmp_path))
    key = _store(storage, "rtm", "mkt", DAY, DAY, snapshot("rtm"), datetime(2026, 11, 12, tzinfo=timezone.utc))
    (tmp_path / key).write_bytes(snapshot("rtm", price=Decimal("2")))

    with pytest.raises(StorageError, match="SHA-256"):
        stored_snapshot_documents(storage, api.RTM)
