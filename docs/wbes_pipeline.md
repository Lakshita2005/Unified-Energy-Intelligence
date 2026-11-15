# WBES ingestion pipeline: PX Dashboard (milestone 1)

Grid-India's WBES (Web Based Energy Scheduling) portal publishes power-exchange schedules behind a personal login. This milestone ingests the **PX Dashboard** into three tables. It is an **independent implementation** (decision W1): the reference repository has no licence and none of its code is used.

> **Live WBES access is not yet authorised (W2).** Everything below has been run only against mocked HTTP. No login, API call or download against WBES has been made. Live verification is a separate, approved step (see the end of this document).

## Status

| Item | State |
|---|---|
| Alembic `0007_wbes_px` (three PX tables + lookup indexes) | ✅ applied 2026-11-15 after a rolled-back dry run (20/20 checks) |
| Source `WBES`, datasets `WBES_PX_REGION`, `WBES_PX_LINK`, `WBES_PX_ENTITY` | ✅ registered |
| Interactive login, session store, token refresh, advisory lock | ✅ implemented, tested with mocks only |
| PX pipelines (region, link, entity) | ✅ implemented, tested with mocks only |
| Live verification | ⛔ not started; requires W2 clearance |
| Net Schedule | not in this milestone (approved future design: per-block rows keeping zeros, per-region JSON-lines raw bundles) |
| Scheduling | not configured |
| Review fixes I-1 to I-4 and M-1 (2026-11-15) | ✅ implemented, tested with mocks only; no schema change |
| Tests | 62 unit (`test_wbes_session.py`, `test_wbes_parse.py`), 3 CLI (`test_wbes_cli.py`), 18 integration (`test_wbes_px_pipeline.py`) |

## Authentication (decision W9)

| Concern | Implementation |
|---|---|
| Login | `python -m scripts.ingest_wbes login` opens a visible browser window with an empty profile on `WBES_PORTAL_URL`. **You** enter the username, OTP and CAPTCHA; nothing is filled in, solved or bypassed. The code waits until the portal page stores its session (`localStorage.currentUser`), copies the access and refresh tokens, closes the window and saves them. Playwright is needed only for this command. |
| Utility id | `WBES_UTILITY_ID` in `.env` (32 hex characters, not a secret); otherwise it is read from the portal page and you are advised to set it. |
| Session file | `WBES_SESSION_FILE`, default `<local app data>/pi/wbes/session.json`. It is **refused** inside the project, `RAW_DATA_DIR`, `LOG_DIR` or any OneDrive folder (the project itself syncs to OneDrive). Owner-only permissions where the OS supports them. `*_session.json` and `session.json` are git-ignored as a second guard. |
| Rotation | The refresh token rotates on every refresh, so the new pair is written atomically (same-directory temp file, fsync, `os.replace`) **before** it is used. A failed write leaves the previous file intact. |
| Refresh | Before each call, refresh when less than `WBES_REFRESH_MARGIN_SECONDS` (150 s) remain or the expiry is unreadable. A 401/403 on a data call triggers one refresh and one retry. A refused refresh (non-200, `IsExpired`, missing tokens) raises `LoginNeeded`. |
| No browser during ingestion | Ingestion never opens a browser. Without a usable session, all three runs are recorded as FAILED with "WBES login required … run python -m scripts.ingest_wbes login", and the CLI exits with code 3. |
| No leakage | Tokens never appear in logs, errors, `repr`, raw files, metadata or database tables. The refresh response body is never included in an error. `python -m scripts.ingest_wbes session` shows the status without tokens. |
| Concurrency | Every WBES command holds `pg_try_advisory_lock` keyed on the utility id, on a dedicated connection, for its whole duration (`src/pipelines/wbes/lock.py`). A second process (or machine using this database) fails fast with `WbesSessionLocked` (exit code 2). |

## How one response feeds three tables

```
GetPxTypeList / GetPxTransTypeList (per date) ──► raw list-exchanges/, list-markets/ (write-once)
        │  every listed exchange x market (or --exchanges/--markets)
        ▼
GetPowerExchangeByDetails {InputDate, PowerExchangeTypeId, PowerExTransactionTypeId, RevisionNo: -1}
        │  fetched ONCE
        ├──► raw px-response/  exactly as received (SHA-256 in .meta.json, request parameters, never tokens)
        └──► raw px/           request-bound document = request + exact response text + its SHA-256
                 │  registered once per dataset (de-duplicated by checksum)
                 ├──► PxRegionPipeline (WBES_PX_REGION, own run) ─► wbes_px_region_schedule
                 ├──► PxLinkPipeline   (WBES_PX_LINK,   own run) ─► wbes_px_link_schedule
                 └──► PxEntityPipeline (WBES_PX_ENTITY, own run) ─► wbes_px_entity_schedule
```

**Why the request-bound document:** documents are de-duplicated per dataset by checksum, and different requests can return byte-identical bodies (all empty responses are identical). Registering the original bytes would merge distinct exchange × market requests into one document and could skip data. The request-bound document is deterministic (no timestamps), so re-fetching the same request with the same response is still recognised as a duplicate. The original bytes are kept as their own file and are also recoverable exactly from the document.

## Tables (migration `0007_wbes_px`)

Common columns: `id`, `delivery_date`, `power_exchange` and `market` (names as published), `power_exchange_type_id`, `market_type_id`, `time_block` (1–96, CHECK), `revision_no`, `power_ex_id`, `retrieved_at`, `source_published_at` (parsed `CreatedOn`), `source_available_at`, `availability_basis` (`SOURCE_CREATED_ON` or `RETRIEVED_AT`, with a CHECK that ties it to the timestamps), `ingestion_run_id`, `document_id`.

| Table | Specific columns | Natural key (loader-enforced; non-unique index) |
|---|---|---|
| `wbes_px_region_schedule` | `wbes_region_id` (NOT NULL), `region_code` (FK region), `injection_value`, `drawal_value` | delivery_date, power_exchange, market, wbes_region_id, time_block |
| `wbes_px_link_schedule` | `link_number`, `link_name`, `net_amount_value` | delivery_date, power_exchange, market, link_name, time_block |
| `wbes_px_entity_schedule` | `entity_acronym`, `entity_unit_acronym`, `entity_px_code`, `wbes_region_id`, `region_code`, `injection_value`, `drawal_value` | delivery_date, power_exchange, market, entity_acronym, entity_unit_acronym, entity_px_code, time_block |

- **Units unverified (W6):** value columns are neutral (`*_value`) and unconstrained `numeric`. JSON numbers are decoded as `Decimal`, so published digits are kept exactly. Nothing assumes MW, and nothing assumes a sign convention for `NetAmount`.
- **Regions:** the WBES region id is always stored. The documented mapping (1 ER, 2 WR, 3 NR, 4 SR, 5 NER, 6 NLDC) is **not verified**, so `REGION_MAPPING_VERIFIED = False` and `region_code` stays NULL for now. NLDC is always NULL. Unknown ids are stored and reported (`UNKNOWN_REGION`).
- **No unique constraint (W7):** PX entity-key uniqueness is unverified. A key occurring twice within one revision rejects both items (`DUPLICATE_KEY_IN_REVISION`), so ambiguous data is never loaded.

## Revisions (decision W4)

- **Lineage, not data:** `revision_no`, `power_ex_id`, `power_exchange_type_id`, `market_type_id` and `availability_basis` are declared as `extra_lineage_columns`. The shared base hook (`BaseIngestionPipeline.extra_lineage_columns`, the only shared-code change) adds them to the loader's ignored columns. A test guarantees that measurement columns can never be listed.
- **Versions:** VERSION policy. A changed value gets a new row tied to the new revision and run; an unchanged value is not copied. Nothing is deleted.
- **Several revisions in one response** (`RevisionNo: -1`): they are sorted and loaded **one revision at a time, in ascending order**, inside the document's transaction. Issues and rejected records are written once, with the last batch, so a document produces at most one rejects file (M-1).
- **Older revisions (item level, I-4):** a record is skipped (`REVISION_ALREADY_STORED` INFO) when its revision is not newer than the latest revision stored for that delivery date × exchange × market **and** its item already has stored rows, because loading it would put an older value above a newer one. An item with no stored rows at all (for example rejected before a parser fix) is loaded (`PREVIOUSLY_MISSING_ITEMS_LOADED` INFO). An item rejected only at some revisions cannot be back-filled out of order.
- **Withdrawn items (I-1):** after loading, the items stored for that delivery date × exchange × market are compared with the items **published** in the newest new revision of the response (including items rejected or incomplete in it). Stored items it no longer publishes are reported as `STORED_ITEM_ABSENT` (WARNING, one issue per document and table), because their last stored values otherwise silently remain the latest rows. No values are invented or deleted. Within one response, items dropped between consecutive revisions are also noted (`ABSENT_IN_REVISION` INFO).
- **Point-in-time:** `source_available_at` = the revision's `CreatedOn` when parseable and not after retrieval; otherwise `retrieved_at`. Because availability is lineage, an unchanged value keeps the availability of the revision in which it first appeared.
- **CreatedOn without an offset (I-2):** its time zone is set by `WBES_CREATED_ON_NAIVE_TIMEZONE`. While it is `unverified` (the default), the **later** plausible reading is used, so availability is never earlier than the truth (no look-ahead): the UTC reading, unless that would be after retrieval, then the IST reading (`CREATED_ON_TIMEZONE_UNVERIFIED` INFO). After live verification, set `Asia/Kolkata` or `UTC`. Values with an offset, or `/Date(ms)/`, are exact.

## Data-quality rules

| Situation | Handling |
|---|---|
| Value array not a list of exactly 96 entries, or an injection/drawal array missing (delivery day complete at retrieval) | item rejected (`PARSE_ERROR`); never padded |
| Value array **shorter** than 96 for a delivery day not yet complete at retrieval (today, tomorrow) (I-3) | item skipped, `INCOMPLETE_SERIES` INFO; no values stored or assumed; loads in full on a later run. Longer than 96 is always rejected |
| Non-numeric or boolean entry | item rejected |
| Null entry | stored as NULL, `MISSING_VALUE` WARNING (`BLOCKS_NOT_YET_PUBLISHED` INFO for a day not yet complete) |
| Zero | stored (a published value) |
| Unknown region id | stored with `region_code` NULL, `UNKNOWN_REGION` WARNING |
| Missing `LinkName` / `EntityAcronym`, or a non-integer `Region` | item rejected |
| Duplicate key within one revision | all copies rejected (`DUPLICATE_KEY_IN_REVISION` ERROR) |
| Item present in revision n but not in n+1 | `ABSENT_IN_REVISION` INFO (its last stored values remain) |
| `RevisionNo` missing or duplicated; `EffectiveForDate` ≠ requested date | those records rejected |
| `CreatedOn` not recognised / in the future | availability = retrieval time; `CREATED_ON_UNPARSED` / `CREATED_ON_IN_FUTURE` WARNING |
| Empty `ResponseBody` | document registered, nothing loaded, `NOT_PUBLISHED` INFO |
| No `ResponseStatus` | body used, `RESPONSE_STATUS_MISSING` WARNING |
| Unsuccessful `ResponseStatus` (≠ `WBES-200`), HTTP error, non-JSON body | all three runs FAILED with the code and message; responses fetched before stay in raw storage |
| Unknown response fields | not parsed, preserved in the raw files |

## Running (only once W2 is cleared)

```bash
python -m scripts.ingest_wbes login                      # you complete username, OTP, CAPTCHA
python -m scripts.ingest_wbes session                    # status (never tokens); --delete to remove
python -m scripts.ingest_wbes px --date 2026-11-10 --exchanges IEX --markets DAM --dry-run
python -m scripts.ingest_wbes px --start-date 2026-11-08 --end-date 2026-11-10   # max 31 days, up to tomorrow
python -m scripts.ingest_wbes px --date 2026-11-10 --reprocess   # offline: re-parses stored raw documents
```

**Reprocessing (I-4)** makes **no WBES call** and needs no login. The stored request-bound documents for the range are re-read in retrieval order. Before parsing, their checksums are verified: the document file against its metadata, the embedded response against the document and the metadata, and the separately stored original response against both. A mismatch stops the command (`WbesError`). Identical re-fetches are processed once. In a dry run, documents are read from the configured raw storage, and nothing is written there.

**Settings (`.env`):** `WBES_API_BASE_URL`, `WBES_PORTAL_URL`, `WBES_UTILITY_ID` (set it before the first login), `WBES_SESSION_FILE`, `WBES_REFRESH_MARGIN_SECONDS`, `WBES_REQUEST_PAUSE_SECONDS` (0.5 s between calls), `WBES_LOGIN_WAIT_MINUTES`, `WBES_LOGIN_BROWSER`, `WBES_CREATED_ON_NAIVE_TIMEZONE` (`unverified` until confirmed).

**Exit codes:** 0 success; 1 a part PARTIAL/FAILED; 2 invalid arguments, configuration, session location or session lock; 3 login required.

**Concurrency rule:** don't use the same WBES user in a browser while ingestion runs, and never run two WBES commands at once (the lock enforces this).

## Raw storage

```
RAW_DATA_DIR/wbes/wbes_px/list-exchanges/YYYY/MM/DD/<retrieved>Z.json   (+ .meta.json with SHA-256)
RAW_DATA_DIR/wbes/wbes_px/list-markets/...
RAW_DATA_DIR/wbes/wbes_px/px-response/...   original details response, exactly as received
RAW_DATA_DIR/wbes/wbes_px/px/...            request-bound document (registered in source_document)
RAW_DATA_DIR/wbes/wbes_px-rejected/...      rejected records per run
```

## Unverified assumptions (to confirm in the approved live verification)

1. Units of `Injection`, `Drawal` and `NetAmount` (MW is likely but unconfirmed), and the `NetAmount` sign convention.
2. `CreatedOn` format and time zone (a value without an offset is read conservatively while `WBES_CREATED_ON_NAIVE_TIMEZONE=unverified`; set it once confirmed).
3. Whether `RevisionNo: -1` returns every revision or only the latest (both are handled).
4. The region-id mapping for PX data (and whether it equals the Net Schedule mapping).
5. PX entity-key uniqueness `(entity_acronym, entity_unit_acronym, entity_px_code)`.
6. The response shapes of the two list endpoints (a bare list, or an envelope; both are accepted).
7. Whether the `Origin`/`Referer` headers sent with calls are needed.
8. Market names returned beyond DAM, RTM, GDAM and HPDAM (any listed market is ingested).
9. How a day that is not yet complete is published (shorter arrays, or 96 entries with nulls); both are handled without assuming block positions.
10. Whether exchange/market/link ids are integers (non-integer ids are stored as NULL; the raw files keep them; review item M-3).

**Live verification plan (each step separately approved):** interactive login → PX dry run for one date → confirm items 1–8 → load two or three dates → re-run and revision check.
