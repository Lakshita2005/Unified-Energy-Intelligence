"""Tests for src.common.checksums and src.common.retry."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from src.common.checksums import sha256_bytes, sha256_file
from src.common.exceptions import HttpRequestError
from src.common.retry import retry_call


def test_sha256_bytes_and_file_agree(tmp_path: Path) -> None:
    data = b"price intelligence" * 100_000
    path = tmp_path / "blob.bin"
    path.write_bytes(data)

    expected = hashlib.sha256(data).hexdigest()
    assert sha256_bytes(data) == expected
    assert sha256_file(path) == expected
    assert len(expected) == 64


def test_retry_succeeds_after_transient_failures() -> None:
    calls: list[int] = []
    sleeps: list[float] = []

    def flaky() -> str:
        calls.append(1)
        if len(calls) < 3:
            raise HttpRequestError("connection reset")
        return "ok"

    assert retry_call(flaky, attempts=3, backoff_factor_seconds=0.5, sleep=sleeps.append) == "ok"
    assert sleeps == [0.5, 1.0]


def test_retry_gives_up_and_reraises() -> None:
    sleeps: list[float] = []

    with pytest.raises(ConnectionError):
        retry_call(lambda: (_ for _ in ()).throw(ConnectionError("down")), attempts=2, sleep=sleeps.append)
    assert len(sleeps) == 1


def test_non_transient_errors_are_not_retried() -> None:
    calls: list[int] = []

    def broken() -> None:
        calls.append(1)
        raise ValueError("bug")

    with pytest.raises(ValueError):
        retry_call(broken, attempts=5, sleep=lambda _: None)
    assert len(calls) == 1
