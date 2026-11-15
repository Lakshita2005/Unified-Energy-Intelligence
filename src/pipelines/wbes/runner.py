"""Run the WBES PX Dashboard ingestion for a range of delivery dates.

1. Load the saved session (none -> every run is recorded as FAILED with "login required").
2. Hold the PostgreSQL advisory lock for the utility for the whole command.
3. For each date: list exchanges and markets (each list response stored raw), then fetch
   ``GetPowerExchangeByDetails`` for every exchange x market **once** and store it **once**
   exactly as received (partition ``px-response``: write-once, SHA-256; request parameters
   in the metadata, never tokens).
4. Register, per response, a *request-bound document* (partition ``px``): a deterministic
   JSON record of the request (delivery date, exchange, market and their WBES ids) plus the
   exact original response text and its SHA-256. Documents are de-duplicated by checksum,
   and different requests can return byte-identical bodies (e.g. empty responses), so the
   original bytes alone would merge distinct requests into one document. A re-fetch of the
   same request with the same response still produces the same document (duplicate).
5. Run the three pipelines (region, link, entity) on the same documents, each with its own run.

A failure while fetching (login needed, HTTP error, unsuccessful ``ResponseStatus``) is
recorded on every requested run as FAILED; responses fetched before it stay in raw storage.
Dry run: database changes are rolled back and raw files go to a temporary directory;
token refreshes are still saved, because the previous refresh token is invalid once rotated.
No browser is ever opened here.

Reprocessing (``reprocess=True``) makes **no WBES call** and needs no session: the
request-bound documents already in raw storage for the date range are re-read in
retrieval order, their checksums verified (document file against its metadata, the
embedded response against the document and the metadata, and the separately stored
original response against both), and parsed again. Records already stored are skipped
by the pipelines; items that were rejected before (no stored rows) are loaded.
"""

from __future__ import annotations

import base64
import json
import tempfile
import time
from collections.abc import Callable, Iterator, Sequence
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, field, fields
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy.engine import Engine

from config.settings import Settings, get_settings
from src.common.database import DatabaseManager, SavepointDatabase
from src.common.checksums import sha256_bytes
from src.common.http_client import HttpClient
from src.common.logging_config import get_logger
from src.common.raw_storage import LocalFileSystemBackend, RawStorageManager, StoredObject, create_raw_storage_manager
from src.ingestion.base import TransactionalDatabase
from src.ingestion.models import IngestionResult, RawDocument, RunStatus
from src.pipelines.wbes import api
from src.pipelines.wbes.client import WbesClient, WbesResponse, decode_json
from src.pipelines.wbes.errors import LoginNeeded, WbesError
from src.pipelines.wbes.lock import wbes_session_lock
from src.pipelines.wbes.parse import parse_created_on
from src.pipelines.wbes.pipeline import PX_PIPELINES, PreparedResponse
from src.pipelines.wbes.session import TokenStore, session_path

logger = get_logger(__name__)

RAW_SOURCE = "wbes"
RAW_DATASET = "wbes_px"


@dataclass
class PartSummary:
    part: str
    status: RunStatus = RunStatus.FAILED
    result: IngestionResult | None = None
    error: str | None = None


@dataclass
class PxRunSummary:
    start_date: date
    end_date: date
    dry_run: bool
    parts: dict[str, PartSummary] = field(default_factory=dict)
    responses: list[str] = field(default_factory=list)
    not_listed: list[str] = field(default_factory=list)
    fetch_error: str | None = None
    login_required: bool = False
    raw_data_dir: Path | None = None

    @property
    def ok(self) -> bool:
        return bool(self.parts) and all(p.status is RunStatus.SUCCESS for p in self.parts.values())


@contextmanager
def _execution_context(settings: Settings, dry_run: bool, database: TransactionalDatabase | None,
                       raw_storage: RawStorageManager | None,
                       ) -> Iterator[tuple[TransactionalDatabase, RawStorageManager, Path | None, Engine | None]]:
    if database is not None:
        yield database, raw_storage or create_raw_storage_manager(settings), None, None
        return
    manager = DatabaseManager.from_settings(settings)
    try:
        if not dry_run:
            yield manager, create_raw_storage_manager(settings), None, manager.engine
            return
        raw_dir = Path(tempfile.mkdtemp(prefix="wbes_px_dry_run_"))
        with manager.engine.connect() as connection:
            outer = connection.begin()
            try:
                yield (SavepointDatabase(connection), RawStorageManager(LocalFileSystemBackend(raw_dir)), raw_dir,
                       manager.engine)
            finally:
                outer.rollback()
                logger.info("Dry run: all database changes rolled back; raw files kept in %s", raw_dir)
    finally:
        manager.dispose()


def _store(storage: RawStorageManager, response: WbesResponse, *, partition: str,
           metadata: dict[str, Any]) -> Any:
    return storage.save_bytes(response.content, source=RAW_SOURCE, dataset=RAW_DATASET, extension="json",
                              ingested_at=response.retrieved_at, partition=partition,
                              metadata={"source_url": response.url, "http_status": response.status_code,
                                        "retrieved_at": response.retrieved_at.isoformat(), **metadata})


def request_bound_document(request: dict[str, Any], response_content: bytes) -> bytes:
    """Deterministic document for one request: the request plus the exact original response.

    The response text is embedded verbatim (base64 if it is not UTF-8) with its SHA-256,
    so the original bytes are recoverable exactly from the document as well.
    """
    try:
        embedded: dict[str, Any] = {"response_text": response_content.decode("utf-8")}
    except UnicodeDecodeError:
        embedded = {"response_base64": base64.b64encode(response_content).decode("ascii")}
    record = {"document_type": "wbes_px_request_response", "format_version": 1, "request": request,
              "response_sha256": sha256_bytes(response_content), **embedded}
    return json.dumps(record, sort_keys=True, ensure_ascii=False, default=str).encode("utf-8")


def _published_at(records: list[dict[str, Any]], retrieved_at: datetime, naive_timezone: str) -> datetime | None:
    created = [c for c in (parse_created_on(r.get("CreatedOn"), retrieved_at, naive_timezone) for r in records) if c]
    created = [c for c in created if c <= retrieved_at + timedelta(minutes=5)]
    return max(created, default=None)


def fetch_px(client: WbesClient, storage: RawStorageManager, start: date, end: date, *,
             exchanges: Sequence[str] | None, markets: Sequence[str] | None, pause: float,
             sleep: Callable[[float], None], not_listed: list[str],
             naive_timezone: str = "unverified") -> list[PreparedResponse]:
    """Discover exchanges and markets per date and fetch every PX response once. Raises on any failure."""
    prepared: list[PreparedResponse] = []
    wanted_px = {n.upper() for n in exchanges} if exchanges else None
    wanted_mkt = {n.upper() for n in markets} if markets else None
    calls = 0

    def call(method: str, path: str, body: dict[str, Any] | None = None) -> WbesResponse:
        nonlocal calls
        if calls:
            sleep(pause)
        calls += 1
        return client.request(method, path, body=body)

    for offset in range((end - start).days + 1):
        day = start + timedelta(days=offset)
        ds = api.wbes_date(day)
        px_response = call("GET", api.EXCHANGES_PATH.format(day=ds))
        _store(storage, px_response, partition="list-exchanges", metadata={"delivery_date": day.isoformat()})
        mkt_response = call("GET", api.MARKETS_PATH.format(day=ds))
        _store(storage, mkt_response, partition="list-markets", metadata={"delivery_date": day.isoformat()})
        listed_px = api.parse_exchanges(px_response.payload)
        listed_mkt = api.parse_markets(mkt_response.payload)
        for name in sorted((wanted_px or set()) - {i.name.upper() for i in listed_px}):
            not_listed.append(f"{day} exchange {name}")
        for name in sorted((wanted_mkt or set()) - {i.name.upper() for i in listed_mkt}):
            not_listed.append(f"{day} market {name}")
        for exchange in listed_px:
            if wanted_px and exchange.name.upper() not in wanted_px:
                continue
            for market in listed_mkt:
                if wanted_mkt and market.name.upper() not in wanted_mkt:
                    continue
                body = api.details_request(day, exchange.item_id, market.item_id)
                response = call("POST", api.DETAILS_PATH, body)
                name = f"{exchange.name}_{market.name}_{day.isoformat()}"
                request = {"path": api.DETAILS_PATH, "body": body, "delivery_date": day.isoformat(),
                           "power_exchange": exchange.name, "power_exchange_type_id": exchange.item_id,
                           "market": market.name, "market_type_id": market.item_id}
                original = _store(storage, response, partition="px-response", metadata={"request": request})
                envelope = api.parse_details(response.payload)     # raises on an unsuccessful ResponseStatus
                content = request_bound_document(request, response.content)
                stored = storage.save_bytes(content, source=RAW_SOURCE, dataset=RAW_DATASET, extension="json",
                                            ingested_at=response.retrieved_at, partition="px", metadata={
                                                "request": request, "response_raw_key": original.key,
                                                "response_sha256": original.sha256,
                                                "response_status": envelope.status_code,
                                                "records": len(envelope.records),
                                                "revisions": sorted(str(r.get("RevisionNo")) for r in envelope.records)})
                document = RawDocument(content=content, retrieved_at=response.retrieved_at, extension="json",
                                       content_type=response.content_type,
                                       published_at=_published_at(envelope.records, response.retrieved_at,
                                                                  naive_timezone),
                                       source_url=response.url, document_name=name, source_report_date=day)
                logger.info("WBES PX %s: %d record(s), sha256=%s", name, len(envelope.records), stored.sha256)
                prepared.append(PreparedResponse(day, exchange, market, envelope.records, envelope.status_code,
                                                 document, stored, body))
    return prepared


def _stored_object(meta: dict[str, Any]) -> StoredObject:
    values = {f.name: meta[f.name] for f in fields(StoredObject) if f.name in meta}
    values["ingested_at"] = datetime.fromisoformat(str(meta["ingested_at"]))
    return StoredObject(**values)


def load_stored_responses(source: RawStorageManager, start: date, end: date, *, exchanges: Sequence[str] | None,
                          markets: Sequence[str] | None, naive_timezone: str = "unverified") -> list[PreparedResponse]:
    """Re-read stored request-bound documents for ``start``..``end`` (no WBES call), verifying checksums.

    Raises:
        WbesError: A raw file does not match its recorded checksum, or is not a WBES PX document.
    """
    wanted_px = {n.upper() for n in exchanges} if exchanges else None
    wanted_mkt = {n.upper() for n in markets} if markets else None
    candidates = []
    for key in source.list_keys(RAW_SOURCE, RAW_DATASET):
        if not key.startswith(f"{RAW_SOURCE}/{RAW_DATASET}/px/"):
            continue
        meta = source.read_metadata(key)
        request = (meta.get("metadata") or {}).get("request") or {}
        try:
            day = date.fromisoformat(str(request.get("delivery_date")))
        except ValueError:
            raise WbesError(f"raw document {key} has no delivery date in its metadata") from None
        if not start <= day <= end:
            continue
        if wanted_px and str(request.get("power_exchange", "")).upper() not in wanted_px:
            continue
        if wanted_mkt and str(request.get("market", "")).upper() not in wanted_mkt:
            continue
        candidates.append((datetime.fromisoformat(str(meta["ingested_at"])), key, meta))
    prepared: list[PreparedResponse] = []
    seen: set[str] = set()
    for _, key, meta in sorted(candidates):          # retrieval order, so revisions arrive as they did
        content = source.read_bytes(key)
        if sha256_bytes(content) != meta.get("sha256"):
            raise WbesError(f"raw document {key}: SHA-256 does not match its metadata; not reprocessed")
        if meta["sha256"] in seen:                   # an identical re-fetch: the same document
            continue
        seen.add(meta["sha256"])
        bound = json.loads(content)
        if bound.get("document_type") != "wbes_px_request_response":
            raise WbesError(f"raw document {key} is not a WBES PX request-bound document")
        if "response_text" in bound:
            response_bytes = bound["response_text"].encode("utf-8")
        else:
            response_bytes = base64.b64decode(bound["response_base64"])
        response_sha = sha256_bytes(response_bytes)
        if response_sha != bound.get("response_sha256") or response_sha != meta["metadata"].get("response_sha256"):
            raise WbesError(f"raw document {key}: embedded response does not match its recorded SHA-256")
        original_key = meta["metadata"].get("response_raw_key")
        if original_key and sha256_bytes(source.read_bytes(original_key)) != response_sha:
            raise WbesError(f"raw response {original_key} does not match the SHA-256 recorded for {key}")
        request = bound["request"]
        envelope = api.parse_details(decode_json(response_bytes))
        stored = _stored_object(meta)
        day = date.fromisoformat(request["delivery_date"])
        exchange = api.ListedItem(request.get("power_exchange_type_id"), request["power_exchange"])
        market = api.ListedItem(request.get("market_type_id"), request["market"])
        document = RawDocument(content=content, retrieved_at=stored.ingested_at, extension="json",
                               content_type="application/json",
                               published_at=_published_at(envelope.records, stored.ingested_at, naive_timezone),
                               source_url=meta["metadata"].get("source_url"),
                               document_name=f"{exchange.name}_{market.name}_{day.isoformat()}",
                               source_report_date=day)
        prepared.append(PreparedResponse(day, exchange, market, envelope.records, envelope.status_code, document,
                                         stored, request.get("body") or {}))
    return prepared


def run_wbes_px(
    *,
    start_date: date,
    end_date: date | None = None,
    parts: Sequence[str] | None = None,
    exchanges: Sequence[str] | None = None,
    markets: Sequence[str] | None = None,
    dry_run: bool = False,
    reprocess: bool = False,
    raw_source: RawStorageManager | None = None,
    settings: Settings | None = None,
    http_client: HttpClient | None = None,
    database: TransactionalDatabase | None = None,
    raw_storage: RawStorageManager | None = None,
    token_store: TokenStore | None = None,
    lock_engine: Engine | None = None,
    use_lock: bool = True,
    today: date | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> PxRunSummary:
    """Fetch the PX Dashboard for ``start_date``..``end_date`` and load the requested parts.

    With ``reprocess=True`` nothing is fetched: stored documents are re-read from
    ``raw_source`` (default: the configured raw storage, also during a dry run).

    Raises:
        ValueError: Invalid range (more than 31 days, beyond tomorrow), unknown part, or
            nothing stored to reprocess.
        WbesError: A stored raw file fails checksum verification (reprocess).
        WbesSessionLocked: Another process holds the WBES session lock.
        WbesSessionError: The session file is unreadable or in a forbidden location.
    """
    settings = settings or get_settings()
    end_date = end_date or start_date
    api.validate_date_range(start_date, end_date, today=today or api.today_ist())
    parts = list(parts) if parts is not None else list(PX_PIPELINES)
    unknown = [p for p in parts if p not in PX_PIPELINES]
    if unknown or not parts:
        raise ValueError(f"unknown part(s) {unknown}; available: {list(PX_PIPELINES)}")
    summary = PxRunSummary(start_date, end_date, dry_run)
    naive_timezone = settings.wbes_created_on_naive_timezone
    if reprocess:
        source = raw_source or raw_storage or create_raw_storage_manager(settings)
        stored_responses = load_stored_responses(source, start_date, end_date, exchanges=exchanges, markets=markets,
                                                 naive_timezone=naive_timezone)
        if not stored_responses:
            raise ValueError(f"no stored WBES PX documents for {start_date}..{end_date} to reprocess")
        tokens = None
    else:
        store = token_store or TokenStore(session_path(settings))
        tokens = store.load()

    with _execution_context(settings, dry_run, database, raw_storage) as (db, storage, raw_dir, engine):
        summary.raw_data_dir = raw_dir
        with ExitStack() as guard:
            if use_lock and tokens is not None and not reprocess:
                lock_target = lock_engine or engine
                if lock_target is None:
                    lock_manager = DatabaseManager.from_settings(settings)
                    guard.callback(lock_manager.dispose)
                    lock_target = lock_manager.engine
                guard.enter_context(wbes_session_lock(lock_target, tokens.utility_id))
            responses: list[PreparedResponse] = []
            fetch_error: BaseException | None = None
            if reprocess:
                responses = stored_responses
            else:
                client = WbesClient(store, settings=settings, http_client=http_client)
                try:
                    if tokens is None:
                        raise LoginNeeded("no saved WBES session")
                    responses = fetch_px(client, storage, start_date, end_date, exchanges=exchanges, markets=markets,
                                         pause=settings.wbes_request_pause_seconds, sleep=sleep,
                                         not_listed=summary.not_listed, naive_timezone=naive_timezone)
                except Exception as exc:   # recorded on every part's run below
                    fetch_error = exc
                    summary.fetch_error = f"{type(exc).__name__}: {exc}"
                    summary.login_required = isinstance(exc, LoginNeeded)
                    logger.error("Fetching WBES PX failed: %s", summary.fetch_error)
                finally:
                    client.close()
            summary.responses = [r.document.document_name or "" for r in responses]
            parameters = {"start_date": start_date.isoformat(), "end_date": end_date.isoformat(), "parts": parts,
                          "exchanges": list(exchanges or []), "markets": list(markets or []), "dry_run": dry_run,
                          "reprocess": reprocess, "responses": [
                              {"name": r.document.document_name, "raw_key": r.stored.key, "sha256": r.stored.sha256,
                               "records": len(r.records)} for r in responses]}
            for part in parts:
                pipeline_class = PX_PIPELINES[part]
                pipeline = pipeline_class(database=db, raw_storage=storage, settings=settings, http_client=http_client,
                                          sleep=sleep, responses=responses, fetch_error=fetch_error)
                pipeline.reload_duplicate_documents = reprocess
                result = pipeline.run({**parameters, "part": part})
                summary.parts[part] = PartSummary(part, result.status, result, result.error)
    return summary
