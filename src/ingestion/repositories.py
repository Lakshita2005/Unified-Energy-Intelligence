"""SQL access to the framework tables in the ``pi`` schema.

Covers ``data_source``, ``source_dataset``, ``ingestion_run``,
``source_document`` and ``data_quality_issue``. Every method takes a
:class:`~sqlalchemy.engine.Connection` and runs inside the caller's
transaction; this module never commits, and never deletes or alters rows it
did not create in the current run.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

from src.common.exceptions import ConfigurationError, IngestionError
from src.common.logging_config import get_logger
from src.common.raw_storage import StoredObject
from src.ingestion.models import (
    DataQualityIssue,
    DatasetDefinition,
    RawDocument,
    RunStatus,
    SourceDefinition,
)

logger = get_logger(__name__)

_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")


class IngestionRepository:
    """Reads and writes the ingestion framework tables.

    Args:
        schema: Schema containing the framework tables (validated identifier).
    """

    def __init__(self, schema: str) -> None:
        if not _IDENTIFIER.fullmatch(schema):
            raise ValueError(f"Invalid schema name {schema!r}")
        self.schema = schema

    # ------------------------------------------------------ registration

    def ensure_source(self, connection: Connection, source: SourceDefinition) -> int:
        """Return the ``source_id`` for ``source.code``, registering it if absent.

        Existing rows are never modified; if their name/URL differ from the
        definition, a warning is logged and the stored values are kept.

        Raises:
            IngestionError: If the source exists but is inactive.
        """
        inserted = connection.execute(
            text(f"INSERT INTO {self.schema}.data_source (source_code, source_name, source_url) "
                 "VALUES (:code, :name, :url) ON CONFLICT (source_code) DO NOTHING RETURNING source_id"),
            {"code": source.code, "name": source.name, "url": source.url},
        ).scalar_one_or_none()
        if inserted is not None:
            logger.info("Registered data source %s (source_id=%d)", source.code, inserted)
            return int(inserted)

        row = connection.execute(
            text(f"SELECT source_id, source_name, source_url, is_active FROM {self.schema}.data_source "
                 "WHERE source_code = :code"),
            {"code": source.code},
        ).one()
        if not row.is_active:
            raise IngestionError(f"Data source {source.code!r} is inactive")
        if (row.source_name, row.source_url) != (source.name, source.url):
            logger.warning(
                "Data source %s already registered with name=%r url=%r; keeping stored values",
                source.code, row.source_name, row.source_url,
            )
        return int(row.source_id)

    def ensure_dataset(self, connection: Connection, source_id: int, dataset: DatasetDefinition) -> int:
        """Return the ``dataset_id`` for ``dataset.code``, registering it if absent.

        Raises:
            ConfigurationError: If the code is registered under a different source.
            IngestionError: If the dataset exists but is inactive.
        """
        inserted = connection.execute(
            text(f"INSERT INTO {self.schema}.source_dataset (source_id, dataset_code, dataset_name, "
                 "endpoint_or_report, native_granularity, expected_frequency, default_timezone) "
                 "VALUES (:source_id, :code, :name, :endpoint, :granularity, :frequency, :tz) "
                 "ON CONFLICT (dataset_code) DO NOTHING RETURNING dataset_id"),
            {"source_id": source_id, "code": dataset.code, "name": dataset.name,
             "endpoint": dataset.endpoint_or_report, "granularity": dataset.native_granularity,
             "frequency": dataset.expected_frequency, "tz": dataset.default_timezone},
        ).scalar_one_or_none()
        if inserted is not None:
            logger.info("Registered dataset %s (dataset_id=%d)", dataset.code, inserted)
            return int(inserted)

        row = connection.execute(
            text(f"SELECT dataset_id, source_id, is_active FROM {self.schema}.source_dataset "
                 "WHERE dataset_code = :code"),
            {"code": dataset.code},
        ).one()
        if row.source_id != source_id:
            raise ConfigurationError(
                f"Dataset {dataset.code!r} is registered under source_id={row.source_id}, not {source_id}"
            )
        if not row.is_active:
            raise IngestionError(f"Dataset {dataset.code!r} is inactive")
        return int(row.dataset_id)

    # ----------------------------------------------------------- runs

    def start_run(
        self,
        connection: Connection,
        dataset_id: int,
        *,
        pipeline_version: str | None,
        parameters: Mapping[str, Any],
    ) -> int:
        """Create an ``ingestion_run`` row with status RUNNING and return its id."""
        run_id = connection.execute(
            text(f"INSERT INTO {self.schema}.ingestion_run (dataset_id, run_status, pipeline_version, parameters) "
                 "VALUES (:dataset_id, :status, :version, CAST(:parameters AS jsonb)) RETURNING ingestion_run_id"),
            {"dataset_id": dataset_id, "status": RunStatus.RUNNING.value, "version": pipeline_version,
             "parameters": json.dumps(dict(parameters), default=str, sort_keys=True)},
        ).scalar_one()
        return int(run_id)

    def finish_run(
        self,
        connection: Connection,
        run_id: int,
        *,
        status: RunStatus,
        records_read: int,
        records_inserted: int,
        records_updated: int,
        records_rejected: int,
        source_published_at: datetime | None,
        error_message: str | None,
    ) -> None:
        """Set the final status and counters of a RUNNING run.

        Raises:
            IngestionError: If the run does not exist or is no longer RUNNING.
        """
        if status is RunStatus.RUNNING:
            raise ValueError("A run cannot finish with status RUNNING")
        updated = connection.execute(
            text(f"UPDATE {self.schema}.ingestion_run SET run_status = :status, completed_at = now(), "
                 "records_read = :read, records_inserted = :inserted, records_updated = :updated, "
                 "records_rejected = :rejected, source_published_at = :published, error_message = :error "
                 "WHERE ingestion_run_id = :run_id AND run_status = 'RUNNING'"),
            {"status": status.value, "read": records_read, "inserted": records_inserted,
             "updated": records_updated, "rejected": records_rejected, "published": source_published_at,
             "error": error_message, "run_id": run_id},
        ).rowcount
        if updated != 1:
            raise IngestionError(f"Ingestion run {run_id} not found or not RUNNING")

    # ------------------------------------------------------ documents

    def register_document(
        self,
        connection: Connection,
        *,
        dataset_id: int,
        run_id: int,
        document: RawDocument,
        stored: StoredObject,
        raw_payload: Any | None = None,
    ) -> tuple[int, bool]:
        """Insert a ``source_document`` row unless the same content is already registered.

        Returns:
            ``(document_id, is_new)``. ``is_new`` is False when a document with
            the same ``(dataset_id, checksum_sha256)`` already exists, in which
            case the existing row's id is returned and nothing is written.
        """
        params = {
            "dataset_id": dataset_id, "run_id": run_id, "name": document.document_name,
            "url": document.source_url, "report_date": document.source_report_date,
            "published_at": document.published_at, "retrieved_at": document.retrieved_at,
            "content_type": document.content_type, "checksum": stored.sha256, "uri": stored.uri,
            "payload": None if raw_payload is None else json.dumps(raw_payload, default=str),
        }
        document_id = connection.execute(
            text(f"INSERT INTO {self.schema}.source_document (dataset_id, ingestion_run_id, document_name, "
                 "source_document_url, source_report_date, published_at, retrieved_at, content_type, "
                 "checksum_sha256, raw_storage_uri, raw_payload) VALUES (:dataset_id, :run_id, :name, :url, "
                 ":report_date, :published_at, :retrieved_at, :content_type, :checksum, :uri, "
                 "CAST(:payload AS jsonb)) ON CONFLICT (dataset_id, checksum_sha256) DO NOTHING "
                 "RETURNING document_id"),
            params,
        ).scalar_one_or_none()
        if document_id is not None:
            return int(document_id), True

        existing = connection.execute(
            text(f"SELECT document_id FROM {self.schema}.source_document "
                 "WHERE dataset_id = :dataset_id AND checksum_sha256 = :checksum"),
            {"dataset_id": dataset_id, "checksum": stored.sha256},
        ).scalar_one()
        return int(existing), False

    # -------------------------------------------------- data quality

    def record_issues(self, connection: Connection, run_id: int, issues: Sequence[DataQualityIssue]) -> int:
        """Insert ``data_quality_issue`` rows for a run. Returns the number written."""
        if not issues:
            return 0
        connection.execute(
            text(f"INSERT INTO {self.schema}.data_quality_issue (ingestion_run_id, table_name, source_record_key, "
                 "issue_code, issue_description, severity) VALUES (:run_id, :table_name, :record_key, "
                 ":code, :description, :severity)"),
            [
                {"run_id": run_id, "table_name": issue.table_name, "record_key": issue.source_record_key,
                 "code": issue.issue_code, "description": issue.issue_description,
                 "severity": issue.severity.value}
                for issue in issues
            ],
        )
        return len(issues)
