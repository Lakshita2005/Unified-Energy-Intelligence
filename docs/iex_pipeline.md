# IEX ingestion pipeline

15-minute clearing prices from the Indian Energy Exchange (DAM, GDAM, RTM), loaded into `pi.iex_market_price`. Source assessment: `docs/iex_source_assessment.md`.

> **Terms of use.** IEX's website terms permit personal, non-commercial use only, unless IEX authorises otherwise. Confirm authorisation before running this regularly. No schedule is configured.

## Status

| Check | State |
|---|---|
| Unit tests (parser, endpoints, date guards, price caps) | ✅ |
| Integration tests (real `pi` schema, mocked API, rolled back) | ✅ |
| Live dry run, DAM / GDAM / RTM | ✅ 2026-11-12 |
| Live ingestion: 2026-11-10 (all three) and 2026-11-07 to 2026-11-09 (DAM); re-run | ✅ 2026-11-12, see the [record](#live-verification-record) |
| Scheduling | not configured |

## What is loaded

| Market | `market_code` | Areas (`area_code`) | Requests per run (any range up to 31 days) |
|---|---|---|---|
| Day-Ahead | `IEX_DAM` | `MKT` (unconstrained MCP) + 13 bid areas `A1 A2 E1 E2 N1 N2 N3 S1 S2 S3 W1 W2 W3` | 14 |
| Green Day-Ahead | `IEX_GDAM` | `MKT` + 13 bid areas | 14 |
| Real-Time | `IEX_RTM` | `MKT` only | 1 |

**Why RTM has no area prices:** `rtm/area-price` returned no rows for any bid area (checked 2026-11-10, a day with congested RTM blocks). RTM area prices are therefore not requested, and RTM prices in congested blocks are the system-wide MCP only.

## Endpoints

These are the undocumented JSON API used by the iexindia.com market data pages. The base URL is set by `IEX_API_BASE_URL`, default `https://www.iexindia.com/api/v1`.

```
GET {base}/{dam|gdam|rtm}/market-snapshot?interval=ONE_FOURTH_HOUR&fromDate=DD-MM-YYYY&toDate=DD-MM-YYYY
GET {base}/{dam|gdam}/area-price?interval=ONE_FOURTH_HOUR&fromDate=...&toDate=...&bidArea=<area>
```

No authentication is required. Requests within one run are spaced 0.5 s apart. Timeouts and HTTP 408/425/429/5xx are retried by the shared HTTP client.

## Field mapping → `pi.iex_market_price`

| Column | Source | Notes |
|---|---|---|
| `market_code` | endpoint | `IEX_DAM`, `IEX_GDAM`, `IEX_RTM` (in `market_master`, operator "Indian Energy Exchange") |
| `area_code` | `MKT` for the snapshot; `bidArea` for area prices | In `market_area`; `country_code` is NULL |
| `delivery_date` | `date` (`DD-MM-YYYY`) | IST delivery day |
| `time_block` | `period` (`"00:00 - 00:15"`; RTM `"00:00-00:15"`) or `timeperiod` (block start) | `start_minutes / 15 + 1`, so block 1 = 00:00–00:15 and block 96 = 23:45–24:00 |
| `price_inr_per_mwh` | DAM/RTM `mcp`; GDAM `unconstrained_m_c_p`; area price under the lower-cased area key | Rs/MWh, parsed exactly as `Decimal` (strings for DAM/RTM, JSON numbers for GDAM and area prices) |
| `retrieved_at` | when the response was fetched | per document |
| `source_published_at` | NULL | The API gives no publication time. Rule-based estimates (such as D-1 13:00) are not stored as source facts. |
| `ingestion_run_id` | the run | The table has no `document_id`; each response is a separate `source_document` of the run |

Purchase and sell bids, MCV, final scheduled volume, congestion and the GDAM source breakdowns are not in `pi.iex_market_price`. They go to `pi.iex_market_snapshot` (see below).

## Snapshot volumes and congestion (`pi.iex_market_snapshot`)

Migration `0013_iex_market_snapshot` (applied 2026-11-26; it follows `0012_area_state_map`) adds the table. One row per market × delivery date × block. Natural key `(market_code, delivery_date, time_block)`, VERSION policy. Datasets `IEX_DAM_SNAPSHOT`, `IEX_GDAM_SNAPSHOT`, `IEX_RTM_SNAPSHOT`. Application privileges: `pi_app` gets **SELECT and INSERT only** (the VERSION loader never updates or deletes). The migration revokes the UPDATE and DELETE that the `pi` schema's default privileges would otherwise grant. Migration chain: `0010_grid_state_ace → 0012_area_state_map → 0013_iex_market_snapshot → 0011_pi_jobs`.

| Column | DAM / RTM field | GDAM field |
|---|---|---|
| `purchase_bid_mw` | `purchase_bid` | `purchase_bid` |
| `sell_bid_mw` | `sell_bid` | `total_sell_bid` |
| `mcv_mw` | `mcv` | `total_unconstrained_m_c_v` |
| `final_scheduled_volume_mw` | `final_scheduled_volume` | `total_cleared_volume` |
| `congestion` | `congestion` (YES/NO → boolean) | `congestion` |
| `<source>_sell_bid_mw`, `<source>_mcv_mw`, `<source>_cleared_volume_mw` | NULL | `<source>_sell_bid`, `<source>_unconstrained_m_c_v`, `<source>_cleared_volume` for solar, non_solar, hydro (to 2026-07-07) and hydro, wind, other_re, dre (from 2026-07-08); NULL when not published |

- Volumes are MW, stored exactly as published (unbounded `numeric`). The price is not repeated; join `iex_market_price` on area `MKT`.
- **Loading:** after each price run, `run_iex` starts a second run that loads the snapshot document the price run just stored. It makes no second request and writes no second raw file. While the table does not exist, the second run is skipped and the CLI prints why; price ingestion is unaffected.
- **Rejections:** a non-numeric or missing volume, or a congestion value other than YES/NO, rejects that block's record (`PARSE_ERROR`); a negative volume is `OUT_OF_RANGE`. An unexpected field set still stops the whole document, as for prices.
- **Never older over newer:** blocks that already have a row retrieved after the document being loaded are not loaded from it (`NEWER_VERSION_STORED`, INFO).
- **Congestion flag:** stored as published. It is not a complete indicator of market splitting. On DAM 2026-11-07 it is YES for 11 blocks, while MCV differs from FSV in 42 blocks and some area price differs from the MCP in 45. All 11 flagged blocks are among both.

### Backfill from stored raw files

```
python -m scripts.ingest_iex backfill-snapshot all --dry-run
python -m scripts.ingest_iex backfill-snapshot all
```

- Optional `--start-date` / `--end-date` select files whose requested range overlaps those days.
- No IEX call is made. Every `iex_<market>__mkt__*` file is checked against the size and SHA-256 in its metadata (a mismatch stops the command), replayed in retrieval order, and byte-identical re-fetches are loaded once.
- One run per market. Content already registered by the snapshot dataset is skipped, so the backfill can be repeated safely.
- Stored files on 2026-11-25: DAM 3 distinct (2 identical re-fetches), GDAM 2 (1), RTM 2 (1); 4,608 records, all of which parse and pass validation.

**GDAM field change:** GDAM's field set changed with delivery date **2026-07-08**:
- before: solar / non-solar / hydro;
- after: hydro / wind / other RE / DRE.

The parser accepts exactly the set that applies to each date.

## Reference data

Seed the reference rows once with `python -m scripts.ingest_iex reference [--dry-run]`.
- It inserts 3 `market_master` and 14 `market_area` rows with `ON CONFLICT DO NOTHING`.
- Existing rows are never changed; a row with different values is reported.
- Real runs refuse to start while rows are missing. A dry run seeds them inside its rolled-back transaction.

### Bid area → state (`pi.area_state_map`)

Migration `0012_area_state_map` (applied 2026-11-26; it follows `0010_grid_state_ace`) adds the table. `pi_app` gets **SELECT and INSERT only**: seeding reads and appends, never updates or deletes, and the migration revokes the UPDATE and DELETE granted by the schema's default privileges. After it is applied, seed it with `python -m scripts.ingest_iex area-states [--dry-run]`. Price runs do not use the table.
- Each row maps a bid area to a state, or to a part of one in `state_part` (Goa and Puducherry are split between areas). Validity is `[effective_from, effective_to)` in delivery dates; `effective_to` NULL means still valid.
- Every row carries `source_name`, `source_url`, `source_published_on`, `verification_status` and `notes`. **No row is OFFICIAL**: the rows rest on IEX's 2016 table as reproduced in an academic paper, a 2020 Regulatory Assistance Project analysis (Telangana in S1; S2 without Kerala), and a 2024 KSERC order (Kerala in S3). `effective_from` is the earliest date a source shows the mapping, not the date IEX introduced it.
- Known gap: Kerala has no row from 2019-03-01 to 2020-07-09, between the last source placing it in S2 and the first placing it in S3.
- Not mapped: Lakshadweep and Andaman & Nicobar (not on the national grid), and `MKT`.
- Seeding is idempotent and never changes stored rows. It reports rows with different values, and rolls back if any validity periods overlap.

Area of a state on a delivery date:

```sql
SELECT area_code, state_part FROM pi.area_state_map
WHERE state_code = 'KL' AND effective_from <= DATE '2026-11-10'
  AND (effective_to IS NULL OR DATE '2026-11-10' < effective_to);
```

## Data quality

| Check | Result |
|---|---|
| HTTP error after retries, or envelope `statusCode` ≠ 200 | Run FAILED (`IexApiError`) |
| `data: []` | Run FAILED (`EmptyResponseError`): unpublished and no-data look the same |
| Not JSON (for example a bot-protection page), no envelope, or record fields not exactly as expected for the date | Run FAILED (`InvalidResponseError`); that document loads nothing |
| Bad date, period, or a null or non-numeric price | `PARSE_ERROR`; the record is rejected, others load; run PARTIAL |
| Delivery date outside the requested range | `PARSE_ERROR` |
| Price < 0 or > 20,000 Rs/MWh | `OUT_OF_RANGE`; rejected |
| Price above the cap in force (20,000 from 2020; 12,000 from 2022-04-03; 10,000 from 2023-04-03) | `ABOVE_PRICE_CAP` WARNING; loaded |
| A day without exactly 96 distinct blocks | `INCOMPLETE_DAY` / `MISSING_DAY`. WARNING for past days; INFO for today's RTM and tomorrow's DAM/GDAM, which are still in progress |
| Repeated block in one response | Loaded once if identical (INFO); not loaded if the copies conflict (ERROR) |

- **Where rejections go:** rejected records are written to `pi.data_quality_issue` and to an `iex_<market>-rejected` raw file. Nothing is silently dropped.
- **Price caps:** the cap schedule is the one recorded, as verified against CERC orders, in `IEX_Price_Forecasting/scripts/prepare_datasets.py`.

## Idempotency and revisions

- **Byte-identical responses** are skipped by their SHA-256 checksum, with an INFO `DUPLICATE_DOCUMENT` note.
- **Record level:** the natural key is (`market_code`, `area_code`, `delivery_date`, `time_block`) with the VERSION policy.
  - An unchanged price is skipped.
  - A revised price inserts a new row tied to the new run; earlier rows are kept, so the price as known at any time can be reconstructed.
  - Today's RTM blocks are added as they clear, without duplicating earlier ones.
- **No database unique constraint** exists on the natural key. Uniqueness is enforced by the pipeline under an advisory lock, and lookups use `ix_iex_market_block`.

## Raw storage

```
RAW_DATA_DIR/iex/<dataset>/<area>_<start>_<end>/<YYYY>/<MM>/<DD>/<retrieved UTC timestamp>Z.json (+ .meta.json)
```

Files are write-once, and every fetch is kept, including duplicates. `source_document` records the URI, the SHA-256 and the document name (`iex_dam__n1__2026-11-10__2026-11-10.json`).

## Running

```bash
python -m scripts.ingest_iex reference                       # once
python -m scripts.ingest_iex dam --date 2026-11-10 --dry-run
python -m scripts.ingest_iex dam --date 2026-11-10
python -m scripts.ingest_iex gdam --start-date 2026-11-01 --end-date 2026-11-10   # max 31 days
python -m scripts.ingest_iex rtm --date 2026-11-12                                 # today: partial day
```

**Allowed dates:** DAM from 2015-01-01, GDAM from 2021-10-27, RTM from 2020-06-01. DAM and GDAM accept up to tomorrow, which is published in the afternoon; RTM accepts up to today.

**Exit codes:** `0` SUCCESS, `1` PARTIAL or FAILED (the price run or, when it ran, the snapshot run), `2` invalid arguments, missing reference rows or a stored file that fails checksum verification.

## Live verification record

Run on 2026-11-12:

| Run | Market | Delivery days | Documents | Read | Inserted | Rejected | Status |
|---|---|---|---|---|---|---|---|
| 836–838 | DAM / GDAM / RTM, dry runs | 2026-11-10 | 14 / 14 / 1 | 1,344 / 1,344 / 96 | rolled back | 0 | SUCCESS |
| 839 | DAM | 2026-11-10 | 14 | 1,344 | 1,344 | 0 | SUCCESS |
| 840 | GDAM | 2026-11-10 | 14 | 1,344 | 1,344 | 0 | SUCCESS |
| 841 | RTM | 2026-11-10 | 1 | 96 | 96 | 0 | SUCCESS |
| 842 | DAM | 2026-11-07 to 11-09 | 14 | 4,032 | 4,032 | 0 | SUCCESS |
| 843–846 | the same four, re-run | | 43 duplicate | | **0** | 0 | SUCCESS |

Checks after the runs:

- 6,816 rows in total. Every (market, area, day) has blocks 1–96 exactly once, and no natural key appears twice.
- 0 quality issues in the loading runs.
- All 43 raw files match their stored SHA-256.
- Reloading every stored raw file through the loader in a rolled-back transaction gave 6,816 unchanged and 0 inserted.
- DAM 2026-11-07 had 38 blocks where bid-area prices differed from the MCP (up to 284.46 Rs/MWh) because of congestion. That day also had blocks at the 10,000 Rs/MWh cap.

## Known limitations

- **Terms of use:** see the note at the top.
- **Undocumented API** behind a bot-protected site: it can change or block requests without notice. Structural changes fail safely, and raw responses are kept.
- **RTM prices:** MCP only, with no area prices.
- **Volumes and congestion** are loaded only after migration `0013_iex_market_snapshot` is applied; until then they stay in the raw files.
- **No publication timestamp** from the source.
- **Empty responses:** `data: []` is ambiguous.
- **Revisions:** IEX doesn't document whether or when published values are revised. Any change is kept as a new version.
