"""Ingestion framework and source extractors.

Framework modules:

* ``base``         - :class:`BaseIngestionPipeline`, the template every source pipeline subclasses
* ``models``       - shared data structures (RawDocument, ParsedRecord, RunStatus, ...)
* ``validation``   - standard validator interface and reusable rules
* ``repositories`` - SQL for pi.data_source, source_dataset, ingestion_run,
  source_document and data_quality_issue

Source-specific pipelines are added here (one module or sub-package per source).
"""
