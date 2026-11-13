"""Tests for Open-Meteo location configuration (no database)."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from config.settings import PROJECT_ROOT
from src.pipelines.open_meteo.locations import LocationError, load_location_config, parse_location_config

VALID = """
[[location]]
code = "SITE_A"
name = "Site A"
latitude = 10.1234567
longitude = 20.5
coordinate_source = "Test fixture"
elevation_m = 120

[[location]]
code = "SITE-B"
name = "Site B"
latitude = -5
longitude = 100
coordinate_source = "Test fixture"
"""


def _write(tmp_path: Path, content: str) -> Path:
    path = tmp_path / "locations.toml"
    path.write_text(content, encoding="utf-8")
    return path


def test_loads_multiple_locations_with_exact_coordinates(tmp_path: Path) -> None:
    locations = load_location_config(_write(tmp_path, VALID))

    assert [loc.code for loc in locations] == ["SITE_A", "SITE-B"]
    assert locations[0].latitude == Decimal("10.1234567")
    assert locations[0].elevation_m == Decimal("120")
    assert locations[1].plant_code is None


def test_missing_file_means_no_locations(tmp_path: Path) -> None:
    assert load_location_config(tmp_path / "absent.toml") == []


def test_example_file_contains_no_active_locations() -> None:
    assert load_location_config(PROJECT_ROOT / "config" / "weather_locations.example.toml") == []


@pytest.mark.parametrize(
    ("entry", "message"),
    [
        ({"code": "site_a"}, "'code' must match"),
        ({"coordinate_source": None}, "required"),
        ({"latitude": Decimal("91")}, "outside"),
        ({"longitude": "20"}, "must be a number"),
        ({"latitude": Decimal("10.12345678")}, "decimal places"),
        ({"name": "  "}, "non-empty"),
    ],
)
def test_invalid_entries_are_rejected(entry: dict, message: str) -> None:
    base = {"code": "SITE_A", "name": "Site A", "latitude": Decimal("10"), "longitude": Decimal("20"),
            "coordinate_source": "Test fixture"}
    base.update(entry)
    base = {k: v for k, v in base.items() if v is not None}

    with pytest.raises(LocationError, match=message):
        parse_location_config({"location": [base]})


def test_test_locations_cannot_reference_a_plant() -> None:
    entry = {"code": "TEST_ONLY_X", "name": "X", "latitude": 1, "longitude": 1, "coordinate_source": "x",
             "plant_code": "P1"}

    with pytest.raises(LocationError, match="must not reference a plant"):
        parse_location_config({"location": [entry]})


def test_duplicate_codes_are_rejected() -> None:
    entry = {"code": "SITE_A", "name": "A", "latitude": 1, "longitude": 1, "coordinate_source": "x"}

    with pytest.raises(LocationError, match="duplicate"):
        parse_location_config({"location": [entry, entry]})


def test_invalid_toml_is_reported(tmp_path: Path) -> None:
    with pytest.raises(LocationError, match="invalid TOML"):
        load_location_config(_write(tmp_path, "[[location]\ncode ="))
