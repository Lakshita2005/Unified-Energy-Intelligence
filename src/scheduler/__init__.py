"""Availability-based scheduler (``pi-scheduler``) and job worker (``pi-worker``).

The scheduler periodically runs source-specific availability checks
(:mod:`src.scheduler.checkers`) and turns newly detected data into jobs in the
PostgreSQL queue (schema ``pi_jobs``, migration ``0011_pi_jobs``). Workers claim
jobs with ``FOR UPDATE SKIP LOCKED``, hold a lease and an advisory lock per
source group, and execute them by calling the existing ingestion runners
(:mod:`src.scheduler.executors`). Pipelines are not modified: their checksum and
natural-key versioning remain the last line of duplicate protection.

See ``docs/scheduler.md``.
"""
