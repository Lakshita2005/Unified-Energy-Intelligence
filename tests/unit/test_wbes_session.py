"""WBES session storage, token refresh, client behaviour and interactive login (no network)."""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import pytest

from config.settings import PROJECT_ROOT, Settings
from src.common.http_client import HttpClient
from src.pipelines.wbes import session as session_module
from src.pipelines.wbes.client import WbesClient
from src.pipelines.wbes.errors import LoginNeeded, WbesError, WbesResponseError, WbesSessionError
from src.pipelines.wbes.lock import advisory_key
from src.pipelines.wbes.login import interactive_login
from src.pipelines.wbes.session import SessionTokens, TokenStore, default_session_path, jwt_expiry, validate_session_path
from tests.support.wbes import API, PORTAL, UTILITY, MockWbes, make_jwt


def _settings(tmp_path: Path, **kwargs: Any) -> Settings:
    return Settings(raw_data_dir=tmp_path / "raw", log_dir=tmp_path / "logs", log_to_file=False,
                    wbes_api_base_url=API, wbes_portal_url=PORTAL, http_max_retries=0, **kwargs)


def _tokens(access: str, refresh: str = "refresh-0") -> SessionTokens:
    return SessionTokens(access, refresh, UTILITY, datetime(2026, 11, 14, 9, 0, tzinfo=timezone.utc))


def _client(tmp_path: Path, mock: MockWbes, store: TokenStore, **kwargs: Any) -> WbesClient:
    settings = _settings(tmp_path, **kwargs)
    http = HttpClient(settings=settings, transport=httpx.MockTransport(mock), sleep=lambda _: None)
    return WbesClient(store, settings=settings, http_client=http)


# ----------------------------------------------------------------- location rules

def test_session_path_must_be_outside_project_raw_logs_and_onedrive(tmp_path: Path,
                                                                    monkeypatch: pytest.MonkeyPatch) -> None:
    settings = _settings(tmp_path)
    for bad in (PROJECT_ROOT / "wbes_session.json", tmp_path / "raw" / "s.json", tmp_path / "logs" / "s.json",
                tmp_path / "OneDrive - Contoso" / "s.json"):
        with pytest.raises(WbesSessionError, match="must not be inside"):
            validate_session_path(bad, settings)
    monkeypatch.setenv("OneDriveCommercial", str(tmp_path / "corp"))
    with pytest.raises(WbesSessionError, match="OneDriveCommercial"):
        validate_session_path(tmp_path / "corp" / "x" / "s.json", settings)
    assert validate_session_path(tmp_path / "private" / "session.json", settings) == \
        (tmp_path / "private" / "session.json").resolve()


def test_default_session_path_is_local_app_data(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "lad"))
    assert default_session_path() == tmp_path / "lad" / "pi" / "wbes" / "session.json"


# ------------------------------------------------------------------- token store

def test_token_store_round_trip_atomic_and_never_shows_tokens(tmp_path: Path) -> None:
    store = TokenStore(tmp_path / "s" / "session.json")
    tokens = _tokens(make_jwt(time.time() + 600), "secret-refresh-value")
    store.save(tokens)
    assert store.load() == tokens
    assert [p.name for p in store.path.parent.iterdir()] == ["session.json"]      # no temp files left behind
    for text in (repr(tokens), str(tokens), repr(store)):
        assert "secret-refresh-value" not in text and tokens.access_token not in text
    assert json.loads(store.path.read_text())["version"] == 1


def test_failed_save_keeps_the_previous_session(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = TokenStore(tmp_path / "session.json")
    store.save(_tokens("a1", "r1"))

    def broken_replace(src: str, dst: str) -> None:
        raise OSError("disk full")
    monkeypatch.setattr(session_module.os, "replace", broken_replace)
    with pytest.raises(OSError):
        store.save(_tokens("a2", "r2"))
    monkeypatch.undo()
    assert store.load().refresh_token == "r1"
    assert [p.name for p in tmp_path.iterdir()] == ["session.json"]


def test_unreadable_session_file_is_reported_without_its_content(tmp_path: Path) -> None:
    store = TokenStore(tmp_path / "session.json")
    store.path.write_text('{"access_token": "do-not-echo"')
    with pytest.raises(WbesSessionError) as info:
        store.load()
    assert "do-not-echo" not in str(info.value)
    assert TokenStore(tmp_path / "missing.json").load() is None


def test_jwt_expiry() -> None:
    assert jwt_expiry(make_jwt(1_800_000_000)) == datetime.fromtimestamp(1_800_000_000, tz=timezone.utc)
    assert jwt_expiry("not-a-jwt") is None


# ------------------------------------------------------------------------ client

def test_fresh_token_is_used_without_refresh(tmp_path: Path) -> None:
    mock = MockWbes()
    store = TokenStore(tmp_path / "session.json")
    store.save(_tokens(mock.access))
    response = _client(tmp_path, mock, store).request("GET", "/powerexchange/api/PowerExchange/GetPxTypeList/date/10-11-2026")
    assert [n["PowerExchangeTypeName"] for n in response.payload] == ["IEX", "PXI", "HPX"]
    assert mock.refreshes == 0


def test_expiring_token_is_refreshed_and_rotation_persisted_first(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    mock = MockWbes(access=make_jwt(time.time() + 60))           # inside the 150 s margin
    store = TokenStore(tmp_path / "session.json")
    store.save(_tokens(mock.access))
    with caplog.at_level(logging.DEBUG):
        client = _client(tmp_path, mock, store)
        client.request("GET", "/powerexchange/api/PowerExchange/GetPxTransTypeList/date/10-11-2026")
    saved = store.load()
    assert mock.refreshes == 1 and saved.refresh_token == mock.refresh == "refresh-1"
    assert saved.access_token == mock.access and saved.refreshed_at is not None
    assert mock.refresh not in caplog.text and mock.access not in caplog.text and "refresh-0" not in caplog.text
    # The refresh token is only ever sent to the refresh endpoint.
    assert all("refresh-" not in body for (method, path), body in zip(mock.calls, mock.seen_bodies)
               if not path.endswith("refresh-token"))


def test_401_triggers_one_refresh_and_retry(tmp_path: Path) -> None:
    mock = MockWbes(force_401=1)
    store = TokenStore(tmp_path / "session.json")
    store.save(_tokens(mock.access))
    _client(tmp_path, mock, store).request("GET", "/powerexchange/api/PowerExchange/GetPxTypeList/date/10-11-2026")
    assert mock.refreshes == 1


def test_second_401_after_refresh_is_login_needed(tmp_path: Path) -> None:
    mock = MockWbes(force_401=2)
    store = TokenStore(tmp_path / "session.json")
    store.save(_tokens(mock.access))
    with pytest.raises(LoginNeeded, match="after a token refresh"):
        _client(tmp_path, mock, store).request("GET", "/powerexchange/api/PowerExchange/GetPxTypeList/date/10-11-2026")


@pytest.mark.parametrize("mock_kwargs", [{"refusals": 1}, {"expire_on_refresh": True}])
def test_refused_refresh_is_login_needed_and_keeps_the_session_file(tmp_path: Path, mock_kwargs: dict) -> None:
    mock = MockWbes(access=make_jwt(time.time() - 10), **mock_kwargs)
    store = TokenStore(tmp_path / "session.json")
    store.save(_tokens(mock.access))
    before = store.path.read_text()
    with pytest.raises(LoginNeeded) as info:
        _client(tmp_path, mock, store).request("GET", "/powerexchange/api/PowerExchange/GetPxTypeList/date/10-11-2026")
    assert "leak-check" not in str(info.value) and "ingest_wbes login" in str(info.value)
    assert store.path.read_text() == before
    assert not any("/powerexchange/" in path for _, path in mock.calls)      # no data call was attempted


def test_no_session_is_login_needed(tmp_path: Path) -> None:
    with pytest.raises(LoginNeeded, match="no saved WBES session"):
        _client(tmp_path, MockWbes(), TokenStore(tmp_path / "none.json")).request("GET", "/x")


def test_server_error_is_a_response_error(tmp_path: Path) -> None:
    mock = MockWbes()
    store = TokenStore(tmp_path / "session.json")
    store.save(_tokens(mock.access))
    with pytest.raises(WbesResponseError, match="HTTP 404"):
        _client(tmp_path, mock, store).request("GET", "/unknown")


def test_advisory_key_is_stable_and_case_insensitive() -> None:
    assert advisory_key(UTILITY) == advisory_key(UTILITY.lower()) != advisory_key("F" * 32)
    assert -(2 ** 63) <= advisory_key(UTILITY) < 2 ** 63


# ------------------------------------------------------------------------- login

class FakePage:
    def __init__(self, sessions: list[Any]) -> None:
        self.sessions, self.visited, self.waits = list(sessions), [], 0

    def goto(self, url: str, **kwargs: Any) -> None:
        self.visited.append(url)

    def evaluate(self, script: str) -> Any:
        assert "localStorage" in script and "currentUser" in script
        return self.sessions.pop(0) if self.sessions else None

    def wait_for_timeout(self, milliseconds: float) -> None:
        self.waits += 1


def _launcher(page: FakePage, closed: list[bool]):
    return lambda: (page, lambda: closed.append(True))


def test_login_waits_for_the_person_then_saves_without_printing_tokens(tmp_path: Path) -> None:
    page = FakePage([None, {"access": None, "refresh": None, "utility": None},
                     {"access": "acc-token-x", "refresh": "ref-token-y", "utility": UTILITY.lower()}])
    closed: list[bool] = []
    printed: list[str] = []
    store = TokenStore(tmp_path / "session.json")
    tokens = interactive_login(_settings(tmp_path), store, launcher=_launcher(page, closed), out=printed.append,
                               poll_seconds=0)
    assert page.visited == [PORTAL] and closed == [True] and page.waits == 2
    assert (tokens.utility_id, store.load().refresh_token) == (UTILITY, "ref-token-y")
    assert not any("acc-token-x" in p or "ref-token-y" in p for p in printed)


def test_login_uses_configured_utility_and_save_false_does_not_write(tmp_path: Path) -> None:
    page = FakePage([{"access": "a", "refresh": "r", "utility": None}])
    store = TokenStore(tmp_path / "session.json")
    tokens = interactive_login(_settings(tmp_path, wbes_utility_id=UTILITY), store, launcher=_launcher(page, []),
                               out=lambda _: None, poll_seconds=0, save=False)
    assert tokens.utility_id == UTILITY and not store.exists()


def test_login_timeout_and_missing_utility(tmp_path: Path) -> None:
    ticks = iter(range(0, 10_000, 30))
    with pytest.raises(WbesError, match="not completed"):
        interactive_login(_settings(tmp_path), TokenStore(tmp_path / "s.json"), launcher=_launcher(FakePage([]), []),
                          out=lambda _: None, poll_seconds=0, wait_minutes=1, clock=lambda: next(ticks))
    ticks = iter(range(0, 10_000, 40))
    page = FakePage([{"access": "a", "refresh": "r", "utility": None}] * 5)
    with pytest.raises(WbesError, match="WBES_UTILITY_ID"):
        interactive_login(_settings(tmp_path), TokenStore(tmp_path / "s.json"), launcher=_launcher(page, []),
                          out=lambda _: None, poll_seconds=0, wait_minutes=5, clock=lambda: next(ticks))
