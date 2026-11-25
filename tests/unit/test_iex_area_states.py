"""Tests for the IEX bid area -> state rows (no network, no database)."""

from __future__ import annotations

from dataclasses import replace
from datetime import date

import pytest

from src.pipelines.cea.geography import STATES
from src.pipelines.iex.api import BID_AREAS
from src.pipelines.iex.area_states import ROWS, AreaState, validate_rows


def _area_on(state: str, day: date, part: str | None = None) -> str | None:
    matches = [r.area_code for r in ROWS if r.state_code == state and r.state_part == part
               and r.effective_from <= day and (r.effective_to is None or day < r.effective_to)]
    assert len(matches) <= 1
    return matches[0] if matches else None


def test_seed_rows_are_valid() -> None:
    validate_rows()


def test_every_bid_area_and_only_known_states_are_mapped() -> None:
    assert {r.area_code for r in ROWS} == set(BID_AREAS)
    assert {r.state_code for r in ROWS} <= {code for code, *_ in STATES}
    assert {r.state_code for r in ROWS}.isdisjoint({"LD", "AN"})


def test_no_row_claims_to_be_official() -> None:
    assert {r.verification_status for r in ROWS} <= {"SECONDARY", "INFERRED"}
    assert all(r.notes for r in ROWS if r.verification_status == "INFERRED")


def test_current_mapping() -> None:
    today = date(2026, 11, 25)
    assert _area_on("PB", today) == "N3"
    assert _area_on("TS", today) == "S1"
    assert _area_on("KL", today) == "S3"
    assert _area_on("GA", today, "North Goa") == "W2"
    assert _area_on("GA", today, "South Goa") == "S1"
    assert _area_on("GA", today) is None


def test_kerala_is_unmapped_where_no_source_covers_it() -> None:
    assert _area_on("KL", date(2017, 1, 1)) == "S2"
    assert _area_on("KL", date(2019, 6, 1)) is None
    assert _area_on("KL", date(2020, 7, 10)) == "S3"


def _row(area: str = "N1", start: date = date(2020, 1, 1), end: date | None = None) -> AreaState:
    return replace(ROWS[0], area_code=area, state_code="HR", state_part=None, effective_from=start, effective_to=end)


@pytest.mark.parametrize("rows, message", [
    ((_row(area="MKT"),), "not an IEX bid area"),
    ((_row(start=date(2020, 1, 1), end=date(2020, 1, 1)),), "effective_to must be after"),
    ((_row(end=date(2021, 1, 1)), _row(area="N2", start=date(2020, 6, 1))), "overlaps"),
    ((_row(), _row(area="N2", start=date(2021, 1, 1))), "overlaps"),
    ((replace(_row(), verification_status="GUESS"),), "verification_status"),
])
def test_invalid_rows_are_refused(rows: tuple[AreaState, ...], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        validate_rows(rows)


def test_adjacent_periods_are_allowed() -> None:
    validate_rows((_row(end=date(2021, 1, 1)), _row(area="N2", start=date(2021, 1, 1))))
