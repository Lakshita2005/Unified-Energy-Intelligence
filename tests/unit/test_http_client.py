"""Tests for src.common.http_client using httpx.MockTransport (no network)."""

from __future__ import annotations

from collections.abc import Callable

import httpx
import pytest

from src.common.exceptions import HttpRequestError, HttpResponseDecodeError, HttpStatusError
from src.common.http_client import HttpClient, parse_retry_after
from tests.conftest import SettingsFactory

Handler = Callable[[httpx.Request], httpx.Response]


class _SleepRecorder:
    def __init__(self) -> None:
        self.calls: list[float] = []

    def __call__(self, seconds: float) -> None:
        self.calls.append(seconds)


def _sequence(*responses: httpx.Response | Exception) -> tuple[Handler, list[httpx.Request]]:
    """Return a handler replaying ``responses`` in order, plus the list of received requests."""
    queue = list(responses)
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        item = queue.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    return handler, seen


def _client(
    make_settings: SettingsFactory, handler: Handler, sleep: _SleepRecorder, **kwargs: object
) -> HttpClient:
    return HttpClient(
        "https://api.example.com",
        settings=make_settings(http_max_retries=3, http_backoff_factor_seconds=1.0, http_backoff_max_seconds=10.0),
        transport=httpx.MockTransport(handler),
        sleep=sleep,
        **kwargs,  # type: ignore[arg-type]
    )


def test_successful_get_json(make_settings: SettingsFactory) -> None:
    handler, seen = _sequence(httpx.Response(200, json={"price": 42}))
    sleep = _SleepRecorder()

    with _client(make_settings, handler, sleep) as client:
        assert client.get_json("/prices", params={"date": "2026-01-01"}) == {"price": 42}

    assert seen[0].url == "https://api.example.com/prices?date=2026-01-01"
    assert seen[0].headers["User-Agent"].startswith("price-intelligence-ingestion")
    assert sleep.calls == []


def test_retries_on_retryable_status_then_succeeds(make_settings: SettingsFactory) -> None:
    handler, seen = _sequence(httpx.Response(503), httpx.Response(502), httpx.Response(200, text="ok"))
    sleep = _SleepRecorder()

    with _client(make_settings, handler, sleep) as client:
        response = client.get("/prices")

    assert response.text == "ok"
    assert len(seen) == 3
    assert len(sleep.calls) == 2
    assert 1.0 <= sleep.calls[0] <= 1.25 and 2.0 <= sleep.calls[1] <= 2.5


def test_raises_status_error_after_exhausting_retries(make_settings: SettingsFactory) -> None:
    handler, seen = _sequence(*[httpx.Response(500, text="server down")] * 4)
    sleep = _SleepRecorder()

    with _client(make_settings, handler, sleep) as client, pytest.raises(HttpStatusError) as excinfo:
        client.get("/prices", params={"api_key": "SECRET"})

    assert excinfo.value.status_code == 500
    assert excinfo.value.attempts == 4
    assert excinfo.value.response_text == "server down"
    assert "SECRET" not in str(excinfo.value) and "SECRET" not in (excinfo.value.url or "")
    assert len(seen) == 4


def test_non_retryable_status_fails_immediately(make_settings: SettingsFactory) -> None:
    handler, seen = _sequence(httpx.Response(404))
    sleep = _SleepRecorder()

    with _client(make_settings, handler, sleep) as client, pytest.raises(HttpStatusError):
        client.get("/missing")

    assert len(seen) == 1 and sleep.calls == []


def test_raise_for_status_false_returns_error_response(make_settings: SettingsFactory) -> None:
    handler, _ = _sequence(httpx.Response(404))

    with _client(make_settings, handler, _SleepRecorder()) as client:
        assert client.get("/missing", raise_for_status=False).status_code == 404


def test_retries_transport_errors_then_raises(make_settings: SettingsFactory) -> None:
    errors = [httpx.ConnectTimeout("timed out") for _ in range(4)]
    handler, seen = _sequence(*errors)
    sleep = _SleepRecorder()

    with _client(make_settings, handler, sleep) as client, pytest.raises(HttpRequestError) as excinfo:
        client.get("/prices")

    assert excinfo.value.attempts == 4
    assert isinstance(excinfo.value.__cause__, httpx.ConnectTimeout)
    assert len(seen) == 4 and len(sleep.calls) == 3


def test_post_is_not_retried_by_default(make_settings: SettingsFactory) -> None:
    handler, seen = _sequence(httpx.Response(503))

    with _client(make_settings, handler, _SleepRecorder()) as client, pytest.raises(HttpStatusError):
        client.post("/submit", json={"a": 1})

    assert len(seen) == 1


def test_post_can_opt_in_to_retries(make_settings: SettingsFactory) -> None:
    handler, seen = _sequence(httpx.Response(503), httpx.Response(200))

    with _client(make_settings, handler, _SleepRecorder()) as client:
        assert client.post("/submit", json={"a": 1}, retry=True).status_code == 200

    assert len(seen) == 2


def test_retry_after_header_is_honoured_and_capped(make_settings: SettingsFactory) -> None:
    handler, _ = _sequence(
        httpx.Response(429, headers={"Retry-After": "3"}),
        httpx.Response(429, headers={"Retry-After": "999"}),
        httpx.Response(200),
    )
    sleep = _SleepRecorder()

    with _client(make_settings, handler, sleep) as client:
        client.get("/prices")

    assert sleep.calls == [3.0, 10.0]


def test_invalid_json_raises_decode_error(make_settings: SettingsFactory) -> None:
    handler, _ = _sequence(httpx.Response(200, text="<html>"))

    with _client(make_settings, handler, _SleepRecorder()) as client, pytest.raises(HttpResponseDecodeError):
        client.get_json("/prices")


def test_safe_url_strips_query_string(make_settings: SettingsFactory) -> None:
    handler, _ = _sequence()
    with _client(make_settings, handler, _SleepRecorder()) as client:
        assert client._safe_url("/v1/prices?token=abc") == "https://api.example.com/v1/prices"
        assert client._safe_url("https://other.example.com/x?k=1#f") == "https://other.example.com/x"


@pytest.mark.parametrize(
    ("header", "expected"),
    [(None, None), ("", None), ("5", 5.0), ("-2", 0.0), ("not-a-date", None),
     ("Wed, 21 Oct 2015 07:28:00 GMT", 0.0)],
)
def test_parse_retry_after(header: str | None, expected: float | None) -> None:
    assert parse_retry_after(header) == expected
