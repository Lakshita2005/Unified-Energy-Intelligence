"""Configurable storage for immutable raw payloads.

Raw data is stored exactly as received, before any transformation, so that
pipelines can be replayed and audited. Objects are write-once: saving to an
existing key raises :class:`StorageError`.

Key layout (``/``-separated regardless of OS)::

    <source>/<dataset>/[<partition>/]<YYYY>/<MM>/<DD>/<YYYYMMDDTHHMMSSffffff>Z.<ext>

``partition`` is optional, e.g. a location code when one dataset is fetched
separately for several locations.

Unless disabled, each object gets a ``<key>.meta.json`` sidecar recording its
size, SHA-256 checksum, ingestion timestamp and any caller-supplied metadata.

Usage::

    storage = create_raw_storage_manager()
    stored = storage.save_json(payload, source="example_api", dataset="daily_prices",
                               metadata={"request_date": "2026-01-01"})

The storage medium is abstracted behind :class:`RawStorageBackend`; only a local
filesystem backend is provided for now.
"""

from __future__ import annotations

import json
import os
import re
import sys
import tempfile
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config.settings import Settings, get_settings
from src.common.checksums import sha256_bytes
from src.common.exceptions import ConfigurationError, StorageError
from src.common.logging_config import get_logger

logger = get_logger(__name__)

METADATA_SUFFIX = ".meta.json"
_NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_\-]*$")
_EXTENSION_PATTERN = re.compile(r"^[A-Za-z0-9]+(\.[A-Za-z0-9]+)*$")


@dataclass(frozen=True)
class StoredObject:
    """Description of an object written to raw storage."""

    key: str
    uri: str
    source: str
    dataset: str
    size_bytes: int
    sha256: str
    ingested_at: datetime
    metadata: dict[str, Any] = field(default_factory=dict)


class RawStorageBackend(ABC):
    """Interface for raw storage media (local disk, object storage, ...).

    Keys are relative, ``/``-separated paths.
    """

    @abstractmethod
    def write(self, key: str, data: bytes) -> str:
        """Write ``data`` to ``key`` and return a URI for the stored object."""

    @abstractmethod
    def read(self, key: str) -> bytes:
        """Return the bytes stored at ``key``."""

    @abstractmethod
    def exists(self, key: str) -> bool:
        """Return True if an object exists at ``key``."""

    @abstractmethod
    def list_keys(self, prefix: str = "") -> list[str]:
        """Return all keys under ``prefix``, sorted."""


def _long_path(path: Path) -> Path:
    """Return a Windows extended-length path so deep keys are not limited to 260 characters."""
    prefix = "\\\\?\\"
    if sys.platform == "win32" and not str(path).startswith(prefix):
        return Path(prefix + str(path))
    return path


class LocalFileSystemBackend(RawStorageBackend):
    """Stores objects as files under a base directory, using atomic writes."""

    def __init__(self, base_dir: Path | str) -> None:
        self.base_dir = Path(base_dir).resolve()
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _path_for(self, key: str) -> Path:
        path = (self.base_dir / key).resolve()
        if not path.is_relative_to(self.base_dir):
            raise StorageError(f"Key {key!r} resolves outside the storage directory")
        return path

    def write(self, key: str, data: bytes) -> str:
        path = self._path_for(key)
        fs_path = _long_path(path)
        try:
            fs_path.parent.mkdir(parents=True, exist_ok=True)
            # Write to a temp file in the same directory, then rename, so readers
            # never observe a partially written file.
            fd, tmp_name = tempfile.mkstemp(dir=fs_path.parent, prefix=f".{path.name}.", suffix=".tmp")
        except OSError as exc:
            raise StorageError(f"Could not prepare storage location for {key!r}: {exc}") from exc
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_name, fs_path)
        except OSError as exc:
            Path(tmp_name).unlink(missing_ok=True)
            raise StorageError(f"Failed to write {key!r}: {exc}") from exc
        return path.as_uri()

    def read(self, key: str) -> bytes:
        path = self._path_for(key)
        try:
            return _long_path(path).read_bytes()
        except FileNotFoundError as exc:
            raise StorageError(f"Raw object {key!r} does not exist") from exc
        except OSError as exc:
            raise StorageError(f"Failed to read {key!r}: {exc}") from exc

    def exists(self, key: str) -> bool:
        return _long_path(self._path_for(key)).is_file()

    def list_keys(self, prefix: str = "") -> list[str]:
        base = _long_path(self.base_dir)
        root = _long_path(self._path_for(prefix)) if prefix else base
        if not root.exists():
            return []
        return sorted(
            path.relative_to(base).as_posix()
            for path in root.rglob("*")
            if path.is_file() and not path.name.startswith(".")
        )


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _validate_name(value: str, label: str) -> str:
    if not _NAME_PATTERN.fullmatch(value):
        raise ValueError(
            f"{label} {value!r} is invalid: use letters, digits, '_' or '-', starting with a letter or digit"
        )
    return value


class RawStorageManager:
    """High-level API for saving and retrieving raw payloads.

    Args:
        backend: Storage medium to write to.
        write_metadata: Write a ``.meta.json`` sidecar next to each object.
        clock: Returns the current UTC time; injectable for tests.
    """

    def __init__(
        self,
        backend: RawStorageBackend,
        *,
        write_metadata: bool = True,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        self.backend = backend
        self.write_metadata = write_metadata
        self._clock = clock

    @staticmethod
    def build_key(
        source: str, dataset: str, extension: str, ingested_at: datetime, *, partition: str | None = None
    ) -> str:
        """Build the storage key for an object.

        Raises:
            ValueError: If a name or extension is invalid, or ``ingested_at`` is naive.
        """
        _validate_name(source, "source")
        _validate_name(dataset, "dataset")
        prefix = f"{source}/{dataset}"
        if partition is not None:
            prefix = f"{prefix}/{_validate_name(partition, 'partition')}"
        extension = extension.lstrip(".")
        if not _EXTENSION_PATTERN.fullmatch(extension):
            raise ValueError(f"extension {extension!r} is invalid")
        if ingested_at.tzinfo is None:
            raise ValueError("ingested_at must be timezone-aware")
        ts = ingested_at.astimezone(timezone.utc)
        return (
            f"{prefix}/{ts:%Y}/{ts:%m}/{ts:%d}/"
            f"{ts:%Y%m%dT%H%M%S%f}Z.{extension}"
        )

    def save_bytes(
        self,
        data: bytes,
        *,
        source: str,
        dataset: str,
        extension: str,
        metadata: dict[str, Any] | None = None,
        ingested_at: datetime | None = None,
        partition: str | None = None,
    ) -> StoredObject:
        """Store raw bytes and return a description of the stored object.

        Raises:
            StorageError: If the key already exists or the write fails.
            ValueError: If names/extension are invalid.
        """
        ingested_at = ingested_at or self._clock()
        key = self.build_key(source, dataset, extension, ingested_at, partition=partition)
        if self.backend.exists(key):
            raise StorageError(f"Raw object {key!r} already exists; raw data is write-once")

        uri = self.backend.write(key, data)
        stored = StoredObject(
            key=key,
            uri=uri,
            source=source,
            dataset=dataset,
            size_bytes=len(data),
            sha256=sha256_bytes(data),
            ingested_at=ingested_at.astimezone(timezone.utc),
            metadata=dict(metadata or {}),
        )
        if self.write_metadata:
            document = json.dumps(asdict(stored), indent=2, default=str, ensure_ascii=False)
            self.backend.write(key + METADATA_SUFFIX, document.encode("utf-8"))

        logger.info(
            "Stored raw object %s (%d bytes, sha256=%s)", key, stored.size_bytes, stored.sha256[:12]
        )
        return stored

    def save_text(self, text: str, *, encoding: str = "utf-8", **kwargs: Any) -> StoredObject:
        """Encode and store text. Keyword arguments as for :meth:`save_bytes`."""
        return self.save_bytes(text.encode(encoding), **kwargs)

    def save_json(self, payload: Any, *, extension: str = "json", **kwargs: Any) -> StoredObject:
        """Serialise ``payload`` to JSON and store it. Keyword arguments as for :meth:`save_bytes`."""
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        return self.save_bytes(data, extension=extension, **kwargs)

    def read_bytes(self, key: str) -> bytes:
        """Return the raw bytes stored at ``key``."""
        return self.backend.read(key)

    def read_text(self, key: str, *, encoding: str = "utf-8") -> str:
        """Return the stored object decoded as text."""
        return self.read_bytes(key).decode(encoding)

    def read_json(self, key: str) -> Any:
        """Return the stored object parsed as JSON."""
        return json.loads(self.read_bytes(key))

    def read_metadata(self, key: str) -> dict[str, Any]:
        """Return the metadata sidecar for ``key``."""
        return json.loads(self.backend.read(key + METADATA_SUFFIX))

    def list_keys(self, source: str | None = None, dataset: str | None = None) -> list[str]:
        """List stored object keys (excluding metadata sidecars), optionally filtered."""
        if dataset is not None and source is None:
            raise ValueError("dataset filter requires source")
        prefix = ""
        if source is not None:
            prefix = _validate_name(source, "source")
            if dataset is not None:
                prefix = f"{prefix}/{_validate_name(dataset, 'dataset')}"
        return [key for key in self.backend.list_keys(prefix) if not key.endswith(METADATA_SUFFIX)]


def create_raw_storage_manager(settings: Settings | None = None) -> RawStorageManager:
    """Build a :class:`RawStorageManager` from ``RAW_STORAGE_*`` settings.

    Raises:
        ConfigurationError: If the configured backend is not supported.
    """
    settings = settings or get_settings()
    if settings.raw_storage_backend == "local":
        backend: RawStorageBackend = LocalFileSystemBackend(settings.raw_data_dir)
    else:  # pragma: no cover - guarded by the Settings Literal type
        raise ConfigurationError(f"Unsupported RAW_STORAGE_BACKEND {settings.raw_storage_backend!r}")
    return RawStorageManager(backend, write_metadata=settings.raw_storage_write_metadata)
