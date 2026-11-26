"""End-to-end Open-Meteo tests against the real ``pi`` schema with a mocked API.

Every test runs inside one outer transaction that is rolled back (nothing
persists). Locations use synthetic test coordinates. No live API calls.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.engine import Connection

from config.settings import Settings
from src.common.checksums import sha256_bytes
from src.common.database import DatabaseManager, SavepointDatabase
from src.common.exceptions import ConfigurationError
from src.common.http_client import HttpClient
from src.common.raw_storage import LocalFileSystemBackend, RawStorageManager
from src.ingestion.models import RunStatus
from src.ingestion.repositories import IngestionRepository
from src.pipelines.open_meteo.runner import RunSummary, run_open_meteo
from tests.support.open_meteo import MockOpenMeteo, response_bytes

pytestmark = pytest.mark.integration

IST = timezone(timedelta(hours=5, minutes=30))
LOCATIONS = """
[[location]]
code = "TEST_SITE_A"
name = "Test site A (synthetic)"
latitude = 10.0
longitude = 20.0
coordinate_source = "Synthetic test fixture"

[[location]]
code = "TEST_SITE_B"
name = "Test site B (synthetic)"
latitude = 11.0
longitude = 21.0
coordinate_source = "Synthetic test fixture"
"""


@pytest.fixture(scope="module")
def manager() -> Iterator[DatabaseManager]:
    try:
        db = DatabaseManager.from_settings(Settings())
    except ConfigurationError as exc:
        pytest.skip(f"PostgreSQL not configured: {exc}")
    yield db
    db.dispose()


@pytest.fixture
def conn(manager: DatabaseManager) -> Iterator[Connection]:
    with manager.engine.connect() as connection:
        outer = connection.begin()
        try:
            yield connection
        finally:
            outer.rollback()


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    locations = tmp_path / "weather_locations.toml"
    locations.write_text(LOCATIONS, encoding="utf-8")
    return Settings(weather_locations_file=locations, raw_data_dir=tmp_path / "raw", log_to_file=False,
                    open_meteo_api_key=None)


@pytest.fixture
def storage(tmp_path: Path) -> RawStorageManager:
    return RawStorageManager(LocalFileSystemBackend(tmp_path / "raw"))


def _run(conn: Connection, settings: Settings, storage: RawStorageManager, mock: MockOpenMeteo, kind: str = "forecast",
         **kwargs: Any) -> RunSummary:
    client = HttpClient(settings=settings, transport=httpx.MockTransport(mock), sleep=lambda _: None)
    kwargs.setdefault("locations", ["TEST_SITE_A"])
    if kind == "historical":
        kwargs.setdefault("start_date", date(2026, 11, 1))
        kwargs.setdefault("end_date", date(2026, 11, 1))
    return run_open_meteo(kind, settings=settings, http_client=client,  # type: ignore[arg-type]
                          database=SavepointDatabase(conn), raw_storage=storage, **kwargs)


def _rows(conn: Connection, code: str = "TEST_SITE_A", is_forecast: bool | None = None) -> list[Any]:
    sql = ("SELECT w.* FROM pi.open_meteo_weather w JOIN pi.weather_location l USING (weather_location_id) "
           "WHERE l.location_code = :code" + ("" if is_forecast is None else " AND w.is_forecast = :f") +
           " ORDER BY w.valid_time, w.id")
    return conn.execute(text(sql), {"code": code, "f": is_forecast}).all()


def _issues(conn: Connection, run_id: int) -> list[tuple[str, str]]:
    return [tuple(r) for r in conn.execute(text(
        "SELECT issue_code, severity FROM pi.data_quality_issue WHERE ingestion_run_id = :r ORDER BY 1"),
        {"r": run_id})]


# ------------------------------------------------------------ happy path


def test_forecast_ingestion_end_to_end(conn: Connection, settings: Settings, storage: RawStorageManager) -> None:
    payload = response_bytes()
    mock = MockOpenMeteo(payload)

    summary = _run(conn, settings, storage, mock, forecast_days=3)

    outcome = summary.outcomes[0]
    assert summary.ok and outcome.status is RunStatus.SUCCESS
    result = outcome.result
    assert (result.records_read, result.records_inserted, result.records_rejected) == (3, 3, 0)
    assert summary.registered_locations == ["TEST_SITE_A"]

    # Request: documented parameters, explicit units and timezone.
    params = mock.requests[0].url.params
    assert mock.requests[0].url.host == "api.open-meteo.com"
    assert (params["latitude"], params["longitude"], params["forecast_days"]) == ("10.0", "20.0", "3")
    assert (params["timezone"], params["wind_speed_unit"], params["timeformat"]) == ("Asia/Kolkata", "ms", "iso8601")

    # Rows: exact values, instants, forecast flags; issue time unknown (NULL), retrieval time recorded.
    rows = _rows(conn)
    assert [r.valid_time for r in rows] == [datetime(2026, 11, 12, h, tzinfo=IST) for h in range(3)]
    first = rows[0]
    assert (first.temperature_2m_c, first.wind_speed_10m_ms, first.cloud_cover_pct, first.relative_humidity_2m_pct) \
        == (Decimal("21.400"), Decimal("2.350"), Decimal("12.00"), Decimal("61.00"))
    assert rows[2].precipitation_mm == Decimal("0.200")
    assert all(r.is_forecast and r.forecast_issued_at is None and r.ghi_wm2 is None for r in rows)
    assert all(r.ingestion_run_id == result.ingestion_run_id for r in rows)

    # Document metadata: checksum, raw file location (source/dataset/location/date), retrieval vs publication.
    doc = conn.execute(text("SELECT * FROM pi.source_document WHERE document_id = :d"),
                       {"d": result.document_ids[0]}).one()
    raw_key = result.raw_storage_keys[0]
    assert raw_key.startswith("open_meteo/open_meteo_forecast/test_site_a/")
    assert (storage.backend.base_dir / raw_key).read_bytes() == payload
    assert doc.checksum_sha256 == sha256_bytes(payload)
    assert doc.raw_storage_uri == (storage.backend.base_dir / raw_key).as_uri()
    assert doc.published_at is None and doc.raw_payload is None
    assert all(r.retrieved_at == doc.retrieved_at for r in rows)
    assert "apikey" not in doc.source_document_url

    run = conn.execute(text("SELECT run_status, records_inserted, parameters FROM pi.ingestion_run "
                            "WHERE ingestion_run_id = :r"), {"r": result.ingestion_run_id}).one()
    assert run.run_status == "SUCCESS" and run.records_inserted == 3
    assert run.parameters["location_code"] == "TEST_SITE_A" and run.parameters["forecast_days"] == 3


# ------------------------------------------------- duplicates & vintages


def test_identical_response_is_not_reloaded(conn: Connection, settings: Settings, storage: RawStorageManager) -> None:
    _run(conn, settings, storage, MockOpenMeteo(response_bytes()))

    second = _run(conn, settings, storage, MockOpenMeteo(response_bytes())).outcomes[0].result

    assert second.duplicate_documents == 1 and second.records_inserted == 0
    assert len(_rows(conn)) == 3


def test_same_values_in_a_new_response_create_no_duplicates(
    conn: Connection, settings: Settings, storage: RawStorageManager
) -> None:
    _run(conn, settings, storage, MockOpenMeteo(response_bytes(generation_ms=0.05)))

    # Live responses always differ (generationtime_ms), so document dedupe cannot help here.
    second = _run(conn, settings, storage, MockOpenMeteo(response_bytes(generation_ms=0.09))).outcomes[0].result

    assert (second.records_inserted, second.records_unchanged, second.duplicate_documents) == (0, 3, 0)
    assert len(_rows(conn)) == 3


def test_revised_forecast_keeps_previous_vintage(conn: Connection, settings: Settings, storage: RawStorageManager) -> None:
    first = _run(conn, settings, storage, MockOpenMeteo(response_bytes())).outcomes[0].result
    revised = response_bytes(values={"temperature_2m": [21.4, 22.6, 20.5]}, generation_ms=0.07)

    second = _run(conn, settings, storage, MockOpenMeteo(revised)).outcomes[0].result

    assert (second.records_inserted, second.records_unchanged) == (1, 2)
    hour_one = [r for r in _rows(conn) if r.valid_time == datetime(2026, 11, 12, 1, tzinfo=IST)]
    assert [(r.temperature_2m_c, r.ingestion_run_id) for r in hour_one] == [
        (Decimal("20.900"), first.ingestion_run_id), (Decimal("22.600"), second.ingestion_run_id)]
    assert hour_one[0].retrieved_at < hour_one[1].retrieved_at


# ---------------------------------------------- historical & validation


def test_historical_with_missing_values(conn: Connection, settings: Settings, storage: RawStorageManager) -> None:
    _run(conn, settings, storage, MockOpenMeteo(response_bytes()))  # forecast for the same hours
    nulls = {name: [None, None, None] for name in ("shortwave_radiation", "direct_radiation", "diffuse_radiation",
                                                    "wind_speed_10m", "cloud_cover", "relative_humidity_2m")}
    payload = response_bytes(values={**nulls, "temperature_2m": [25.0, None, None], "precipitation": [None, 0.0, None]})
    mock = MockOpenMeteo(payload)

    summary = _run(conn, settings, storage, mock, kind="historical")

    result = summary.outcomes[0].result
    assert mock.requests[0].url.host == "archive-api.open-meteo.com"
    assert (mock.requests[0].url.params["start_date"], mock.requests[0].url.params["end_date"]) == \
        ("2026-11-01", "2026-11-01")
    assert result.status is RunStatus.PARTIAL
    assert (result.records_inserted, result.records_rejected) == (2, 1)
    assert _issues(conn, result.ingestion_run_id) == [("ALL_MEASUREMENTS_NULL", "ERROR")]
    historical = _rows(conn, is_forecast=False)
    assert historical[0].temperature_2m_c == Decimal("25.000") and historical[0].precipitation_mm is None
    assert historical[1].temperature_2m_c is None and historical[1].precipitation_mm == Decimal("0.000")
    assert len(_rows(conn, is_forecast=True)) == 3  # forecast and historical rows coexist
    rejects_key = next(k for k in result.raw_storage_keys if "-rejected/" in k)
    assert rejects_key.startswith("open_meteo/open_meteo_historical-rejected/test_site_a/")


def test_out_of_range_values_are_rejected_not_loaded(
    conn: Connection, settings: Settings, storage: RawStorageManager
) -> None:
    payload = response_bytes(values={"relative_humidity_2m": [61, 140, 66]})

    result = _run(conn, settings, storage, MockOpenMeteo(payload)).outcomes[0].result

    assert result.status is RunStatus.PARTIAL
    assert (result.records_inserted, result.records_rejected) == (2, 1)
    assert _issues(conn, result.ingestion_run_id) == [("OUT_OF_RANGE", "ERROR")]


# -------------------------------------------------------- failure kinds


@pytest.mark.parametrize(
    ("response", "error_type"),
    [
        (httpx.Response(400, json={"error": True, "reason": "Invalid coordinates"}), "OpenMeteoApiError"),
        (response_bytes(times=[], values={n: [] for n in ("temperature_2m", "shortwave_radiation",
                                                          "direct_radiation", "diffuse_radiation", "wind_speed_10m",
                                                          "cloud_cover", "relative_humidity_2m", "precipitation")}),
         "EmptyResponseError"),
        (response_bytes(timezone="GMT", offset=0), "InvalidResponseError"),
    ],
)
def test_failures_are_classified_and_load_nothing(
    conn: Connection, settings: Settings, storage: RawStorageManager, response: Any, error_type: str
) -> None:
    result = _run(conn, settings, storage, MockOpenMeteo(response)).outcomes[0].result

    assert result.status is RunStatus.FAILED and result.error.startswith(error_type)
    run = conn.execute(text("SELECT run_status, error_message FROM pi.ingestion_run WHERE ingestion_run_id = :r"),
                       {"r": result.ingestion_run_id}).one()
    assert run.run_status == "FAILED" and run.error_message.startswith(error_type)
    assert _rows(conn) == []
    assert conn.execute(text("SELECT count(*) FROM pi.source_document WHERE ingestion_run_id = :r"),
                        {"r": result.ingestion_run_id}).scalar_one() == 0
    if error_type != "OpenMeteoApiError":  # a response was received: raw file kept
        assert result.raw_storage_keys and (storage.backend.base_dir / result.raw_storage_keys[0]).exists()


def test_database_failure_rolls_back_the_whole_document(
    conn: Connection, settings: Settings, storage: RawStorageManager, monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken(*args: Any, **kwargs: Any) -> int:
        raise RuntimeError("simulated failure after rows were inserted")

    monkeypatch.setattr(IngestionRepository, "record_issues", broken)

    result = _run(conn, settings, storage, MockOpenMeteo(response_bytes())).outcomes[0].result

    assert result.status is RunStatus.FAILED and "simulated failure" in result.error
    assert _rows(conn) == []
    assert conn.execute(text("SELECT count(*) FROM pi.source_document WHERE ingestion_run_id = :r"),
                        {"r": result.ingestion_run_id}).scalar_one() == 0
    assert (storage.backend.base_dir / result.raw_storage_keys[0]).exists()


# ---------------------------------------------------- multiple locations


def test_one_failing_location_does_not_affect_others(
    conn: Connection, settings: Settings, storage: RawStorageManager
) -> None:
    def by_location(request: httpx.Request) -> httpx.Response:
        if request.url.params["latitude"] == "11.0":
            return httpx.Response(400, json={"error": True, "reason": "simulated"})
        return httpx.Response(200, content=response_bytes())

    # The two locations explicitly: an all-locations run would also include the production plants registered in
    # pi.weather_location since 2026-11-20 (and this mock returns one identical body for every location).
    summary = _run(conn, settings, storage, MockOpenMeteo(by_location), locations=["TEST_SITE_A", "TEST_SITE_B"])

    statuses = {o.location_code: o.status for o in summary.outcomes}
    assert statuses == {"TEST_SITE_A": RunStatus.SUCCESS, "TEST_SITE_B": RunStatus.FAILED}
    assert not summary.ok
    assert len(_rows(conn, "TEST_SITE_A")) == 3 and _rows(conn, "TEST_SITE_B") == []
    run_ids = {o.result.ingestion_run_id for o in summary.outcomes}
    assert len(run_ids) == 2  # separate audit record per location


def test_relocated_coordinates_are_refused(conn: Connection, settings: Settings, storage: RawStorageManager) -> None:
    conn.execute(text("INSERT INTO pi.weather_location (location_code, location_name, latitude, longitude) "
                      "VALUES ('TEST_SITE_A', 'moved', 10.5, 20.0)"))

    summary = _run(conn, settings, storage, MockOpenMeteo(response_bytes()), locations=["TEST_SITE_A", "TEST_SITE_B"])

    outcome_a, outcome_b = summary.outcomes
    assert outcome_a.status is RunStatus.FAILED and "never moved" in outcome_a.error
    assert outcome_b.status is RunStatus.SUCCESS


def test_test_locations_run_only_when_requested(conn: Connection, tmp_path: Path, storage: RawStorageManager) -> None:
    locations = tmp_path / "with_test.toml"
    locations.write_text(LOCATIONS + """
[[location]]
code = "TEST_ONLY_POINT"
name = "Technical test point (synthetic)"
latitude = 12.0
longitude = 22.0
coordinate_source = "Synthetic test fixture"
""", encoding="utf-8")
    settings = Settings(weather_locations_file=locations, raw_data_dir=tmp_path / "raw", log_to_file=False,
                        open_meteo_api_key=None)

    explicit = _run(conn, settings, storage, MockOpenMeteo(response_bytes()), locations=["TEST_ONLY_POINT"])
    default = _run(conn, settings, storage, MockOpenMeteo(response_bytes()), locations=None)

    default_codes = [o.location_code for o in default.outcomes]
    # registered production plants may be included; the technical test point never is, unless requested
    assert [c for c in default_codes if c.startswith("TEST_")] == ["TEST_SITE_A", "TEST_SITE_B"]
    assert "TEST_ONLY_POINT" not in default_codes
    assert explicit.outcomes[0].status is RunStatus.SUCCESS and len(_rows(conn, "TEST_ONLY_POINT")) == 3


def test_unknown_location_is_reported(conn: Connection, settings: Settings, storage: RawStorageManager) -> None:
    summary = _run(conn, settings, storage, MockOpenMeteo(response_bytes()), locations=["NOT_CONFIGURED"])

    assert summary.outcomes[0].status is RunStatus.FAILED and "neither configured" in summary.outcomes[0].error
