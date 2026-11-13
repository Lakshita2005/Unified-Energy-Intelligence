"""Tests for src.common.raw_storage."""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from src.common.exceptions import StorageError
from src.common.raw_storage import (
    LocalFileSystemBackend,
    RawStorageManager,
    create_raw_storage_manager,
)
from tests.conftest import SettingsFactory

FIXED_TIME = datetime(2026, 3, 4, 5, 6, 7, 890123, tzinfo=timezone.utc)


@pytest.fixture
def storage(tmp_path: Path) -> RawStorageManager:
    return RawStorageManager(LocalFileSystemBackend(tmp_path / "raw"), clock=lambda: FIXED_TIME)


def test_build_key_layout() -> None:
    key = RawStorageManager.build_key("example_api", "daily-prices", ".csv.gz", FIXED_TIME)

    assert key == "example_api/daily-prices/2026/03/04/20260304T050607890123Z.csv.gz"


def test_build_key_with_partition() -> None:
    key = RawStorageManager.build_key("open_meteo", "forecast", "json", FIXED_TIME, partition="site_a")

    assert key == "open_meteo/forecast/site_a/2026/03/04/20260304T050607890123Z.json"
    with pytest.raises(ValueError):
        RawStorageManager.build_key("s", "d", "json", FIXED_TIME, partition="../x")


def test_deep_keys_beyond_windows_path_limit(tmp_path: Path) -> None:
    storage = RawStorageManager(LocalFileSystemBackend(tmp_path / ("deep" * 30)), clock=lambda: FIXED_TIME)

    stored = storage.save_text("x", source="s" * 40, dataset="d" * 60, extension="json", partition="p" * 40)

    assert len(str(storage.backend.base_dir / stored.key)) > 260
    assert storage.read_text(stored.key) == "x" and storage.read_metadata(stored.key)["size_bytes"] == 1
    assert storage.list_keys("s" * 40) == [stored.key]


def test_build_key_converts_to_utc() -> None:
    local = FIXED_TIME.astimezone(timezone(timedelta(hours=5, minutes=30)))

    assert RawStorageManager.build_key("s", "d", "json", local) == RawStorageManager.build_key(
        "s", "d", "json", FIXED_TIME
    )


@pytest.mark.parametrize("bad", ["", "../etc", "a/b", "has space", "_leading", "a.b"])
def test_invalid_names_are_rejected(bad: str) -> None:
    with pytest.raises(ValueError):
        RawStorageManager.build_key(bad, "dataset", "json", FIXED_TIME)


def test_naive_timestamp_is_rejected() -> None:
    with pytest.raises(ValueError):
        RawStorageManager.build_key("s", "d", "json", datetime(2026, 1, 1))


def test_save_and_read_json_with_metadata(storage: RawStorageManager, tmp_path: Path) -> None:
    payload = {"price": 12.5, "currency": "EUR"}

    stored = storage.save_json(payload, source="src", dataset="prices", metadata={"page": 1})

    assert storage.read_json(stored.key) == payload
    assert (tmp_path / "raw" / stored.key).is_file()
    assert stored.uri.startswith("file:")
    assert stored.sha256 == hashlib.sha256(storage.read_bytes(stored.key)).hexdigest()

    meta = storage.read_metadata(stored.key)
    assert meta["sha256"] == stored.sha256
    assert meta["size_bytes"] == stored.size_bytes
    assert meta["metadata"] == {"page": 1}


def test_objects_are_write_once(storage: RawStorageManager) -> None:
    storage.save_text("a", source="src", dataset="prices", extension="txt")

    with pytest.raises(StorageError, match="write-once"):
        storage.save_text("b", source="src", dataset="prices", extension="txt")


def test_list_keys_excludes_metadata_and_filters(tmp_path: Path) -> None:
    times = iter(FIXED_TIME + timedelta(seconds=i) for i in range(10))
    storage = RawStorageManager(LocalFileSystemBackend(tmp_path / "raw"), clock=lambda: next(times))
    a = storage.save_text("1", source="alpha", dataset="x", extension="txt")
    b = storage.save_text("2", source="alpha", dataset="y", extension="txt")
    c = storage.save_text("3", source="beta", dataset="x", extension="txt")

    assert storage.list_keys() == sorted([a.key, b.key, c.key])
    assert storage.list_keys("alpha") == sorted([a.key, b.key])
    assert storage.list_keys("alpha", "y") == [b.key]
    assert storage.list_keys("missing") == []


def test_metadata_can_be_disabled(tmp_path: Path) -> None:
    storage = RawStorageManager(
        LocalFileSystemBackend(tmp_path / "raw"), write_metadata=False, clock=lambda: FIXED_TIME
    )
    stored = storage.save_text("x", source="s", dataset="d", extension="txt")

    with pytest.raises(StorageError):
        storage.read_metadata(stored.key)


def test_backend_blocks_path_traversal(tmp_path: Path) -> None:
    backend = LocalFileSystemBackend(tmp_path / "raw")

    with pytest.raises(StorageError):
        backend.write("../outside.txt", b"x")
    assert not (tmp_path / "outside.txt").exists()


def test_reading_missing_object_raises(storage: RawStorageManager) -> None:
    with pytest.raises(StorageError, match="does not exist"):
        storage.read_bytes("nope/missing.json")


def test_no_temp_files_left_behind(storage: RawStorageManager, tmp_path: Path) -> None:
    storage.save_text("x", source="s", dataset="d", extension="txt")

    assert not [p for p in (tmp_path / "raw").rglob("*.tmp")]


def test_factory_uses_settings(make_settings: SettingsFactory, tmp_path: Path) -> None:
    manager = create_raw_storage_manager(
        make_settings(raw_data_dir=tmp_path / "configured", raw_storage_write_metadata=False)
    )

    assert isinstance(manager.backend, LocalFileSystemBackend)
    assert manager.backend.base_dir == (tmp_path / "configured").resolve()
    assert manager.write_metadata is False
