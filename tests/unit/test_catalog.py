"""Consistency checks for the source/dataset catalogue (no database)."""

from __future__ import annotations

import re

from config.expected_schema import EXPECTED_TABLES
from scripts.register_sources import render_mapping_document
from src.ingestion.catalog import COMMON_TABLES, DATASETS, IST, PENDING, PENDING_DATASETS, SOURCES, UTC

# Column limits in pi.data_source / pi.source_dataset.
LIMITS = {"source_code": 40, "dataset_code": 80, "native_granularity": 30, "expected_frequency": 40,
          "default_timezone": 50}
CODE = re.compile(r"^[A-Z][A-Z0-9_]*$")


def test_codes_are_unique_stable_identifiers() -> None:
    codes = [entry.definition.code for entry in DATASETS]
    pending = [p.proposed_code for p in PENDING_DATASETS]

    assert len(codes) == len(set(codes))
    assert not set(codes) & set(pending), "a pending dataset must not also be registered"
    assert all(CODE.fullmatch(code) for code in [*codes, *pending, *SOURCES])


def test_values_fit_column_limits() -> None:
    assert all(len(code) <= LIMITS["source_code"] for code in SOURCES)
    for entry in DATASETS:
        d = entry.definition
        assert len(d.code) <= LIMITS["dataset_code"]
        assert len(d.native_granularity or "") <= LIMITS["native_granularity"]
        assert len(d.expected_frequency or "") <= LIMITS["expected_frequency"]
        assert len(d.default_timezone) <= LIMITS["default_timezone"]


def test_every_dataset_has_known_source_and_existing_tables() -> None:
    for entry in DATASETS:
        assert entry.source_code in SOURCES
        assert entry.destination_tables
        assert set(entry.destination_tables) <= EXPECTED_TABLES
    assert set(COMMON_TABLES) <= EXPECTED_TABLES
    for pending in PENDING_DATASETS:
        assert set(pending.destination_tables) <= EXPECTED_TABLES


def test_every_dataset_records_a_reference_or_explicit_pending_marker() -> None:
    for entry in DATASETS:
        d = entry.definition
        assert d.endpoint_or_report and d.native_granularity and d.expected_frequency
        assert d.default_timezone in {IST, UTC}
        has_url = "https://" in d.endpoint_or_report
        assert has_url or d.endpoint_or_report.startswith(PENDING), d.code


def test_unverified_dsm_publishers_are_not_registered() -> None:
    registered = {entry.definition.code: entry for entry in DATASETS}

    assert registered["DSM_SETTLEMENT_REGIONAL"].source_code == "RPC"
    assert registered["DSM_SCHEDULE_INTERSTATE"].definition.endpoint_or_report.startswith(PENDING)
    assert not any("PPA" in code or "ACTUAL" in code for code in registered)
    assert "INTERNAL_CONTRACTS" not in SOURCES


def test_mapping_document_covers_everything() -> None:
    document = render_mapping_document()

    for entry in DATASETS:
        assert f"`{entry.definition.code}`" in document
        assert all(f"`{table}`" in document for table in entry.destination_tables)
    for pending in PENDING_DATASETS:
        assert f"`{pending.proposed_code}`" in document
