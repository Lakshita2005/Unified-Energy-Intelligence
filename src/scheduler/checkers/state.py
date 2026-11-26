"""What is already stored, per source (read-only queries for the availability checkers).

:class:`SourceState` is the interface checkers use; :class:`PostgresSourceState` reads ``pi`` tables in a
READ ONLY transaction. Tests use an in-memory fake with the same methods. Nothing here writes.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Protocol

from sqlalchemy import text


class SourceState(Protocol):
    def document_checksums(self, dataset_code: str, since: date, url_marker: str | None = None
                           ) -> dict[date, set[str]]:
        """``source_document`` SHA-256s per ``source_report_date`` (optionally only URLs containing a marker)."""

    def document_names(self, dataset_codes: list[str], since: date) -> dict[str, set[tuple[date, str]]]:
        """Registered ``(source_report_date, document_name)`` per dataset."""

    def cea_psp_months(self, measure: str, since: date) -> dict[date, set[str]]:
        """Stored ``report_status`` values per report month for ENERGY / PEAK."""

    def iex_mkt_prices(self, market_code: str, start: date, end: date) -> dict[date, dict[int, Decimal]]:
        """Latest stored MKT price per delivery date and time block."""

    def npp_max_updated_ms(self, series: tuple[str, ...], days: list[date]) -> dict[date, int]:
        """Highest stored ``source_updated_on_ms`` per IST day for the given series."""

    def mosdac_latest_series(self, location_code: str, kind: str = "solar") -> datetime | None:
        """Latest stored ``series_start_at`` for a location and kind (``solar`` or ``wind``)."""

    def mosdac_series_locations(self, kind: str, series_start: datetime) -> set[str]:
        """Locations that already have rows of the forecast series starting at ``series_start``."""


class PostgresSourceState:
    def __init__(self, database: Any, schema: str = "pi") -> None:
        self.database = database
        self.s = schema

    def _rows(self, sql: str, params: dict[str, Any]) -> list[Any]:
        with self.database.read_only_connection() as c:
            return c.execute(text(sql), params).all()

    def document_checksums(self, dataset_code: str, since: date, url_marker: str | None = None
                           ) -> dict[date, set[str]]:
        rows = self._rows(
            f"SELECT d.source_report_date, d.checksum_sha256 FROM {self.s}.source_document d "
            f"JOIN {self.s}.source_dataset s USING (dataset_id) WHERE s.dataset_code = :ds "
            "AND d.source_report_date >= :since AND (CAST(:m AS text) IS NULL OR d.source_document_url LIKE :pattern)",
            {"ds": dataset_code, "since": since, "m": url_marker, "pattern": f"%{url_marker}%"})
        result: dict[date, set[str]] = defaultdict(set)
        for day, checksum in rows:
            result[day].add(checksum)
        return dict(result)

    def document_names(self, dataset_codes: list[str], since: date) -> dict[str, set[tuple[date, str]]]:
        rows = self._rows(
            f"SELECT s.dataset_code, d.source_report_date, d.document_name FROM {self.s}.source_document d "
            f"JOIN {self.s}.source_dataset s USING (dataset_id) WHERE s.dataset_code = ANY(:ds) "
            "AND d.source_report_date >= :since", {"ds": list(dataset_codes), "since": since})
        result: dict[str, set[tuple[date, str]]] = {code: set() for code in dataset_codes}
        for code, day, name in rows:
            result[code].add((day, name))
        return result

    def cea_psp_months(self, measure: str, since: date) -> dict[date, set[str]]:
        rows = self._rows(
            f"SELECT report_month, coalesce(report_status, 'UNKNOWN') FROM {self.s}.cea_power_supply_position "
            "WHERE measure = :m AND report_month >= :since GROUP BY 1, 2", {"m": measure, "since": since})
        result: dict[date, set[str]] = defaultdict(set)
        for month, status in rows:
            result[month].add(status)
        return dict(result)

    def iex_mkt_prices(self, market_code: str, start: date, end: date) -> dict[date, dict[int, Decimal]]:
        rows = self._rows(
            f"SELECT DISTINCT ON (delivery_date, time_block) delivery_date, time_block, price_inr_per_mwh "
            f"FROM {self.s}.iex_market_price WHERE market_code = :m AND area_code = 'MKT' "
            "AND delivery_date BETWEEN :a AND :b ORDER BY delivery_date, time_block, id DESC",
            {"m": market_code, "a": start, "b": end})
        result: dict[date, dict[int, Decimal]] = defaultdict(dict)
        for day, block, price in rows:
            result[day][block] = price
        return dict(result)

    def npp_max_updated_ms(self, series: tuple[str, ...], days: list[date]) -> dict[date, int]:
        if not days:
            return {}
        rows = self._rows(
            f"SELECT (observed_at AT TIME ZONE 'Asia/Kolkata')::date AS d, max(source_updated_on_ms) "
            f"FROM {self.s}.npp_demand_generation WHERE name_of_data = ANY(:series) "
            "AND observed_at >= (CAST(:first AS date) - interval '1 day') "
            "AND observed_at < (CAST(:last AS date) + interval '2 day') GROUP BY 1",
            {"series": list(series), "first": min(days), "last": max(days)})
        return {day: int(value) for day, value in rows if day in set(days) and value is not None}

    MOSDAC_TABLES = {"solar": "mosdac_solar_forecast", "wind": "mosdac_wind_forecast"}

    def mosdac_latest_series(self, location_code: str, kind: str = "solar") -> datetime | None:
        table = self.MOSDAC_TABLES[kind]       # fixed whitelist: never interpolate caller text into SQL
        rows = self._rows(f"SELECT max(series_start_at) FROM {self.s}.{table} WHERE location_code = :c",
                          {"c": location_code})
        return rows[0][0] if rows else None

    def mosdac_series_locations(self, kind: str, series_start: datetime) -> set[str]:
        table = self.MOSDAC_TABLES[kind]       # fixed whitelist
        rows = self._rows(f"SELECT DISTINCT location_code FROM {self.s}.{table} WHERE series_start_at = :s",
                          {"s": series_start})
        return {r[0] for r in rows}
