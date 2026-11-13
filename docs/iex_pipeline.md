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

**Not stored in the database:** purchase and sell bids, MCV, final scheduled volume, congestion, and the GDAM source breakdowns. `pi.iex_market_price` has no columns for them. They are preserved in the raw files. Storing them would need a new table, which means proposing an Alembic migration first; none has been applied.

**GDAM field change:** GDAM's field set changed with delivery date **2026-07-08**:
- before: solar / non-solar / hydro;
- after: hydro / wind / other RE / DRE.

The parser accepts exactly the set that applies to each date.

## Reference data

Seed the reference rows once with `python -m scripts.ingest_iex reference [--dry-run]`.
- It inserts 3 `market_master` and 14 `market_area` rows with `ON CONFLICT DO NOTHING`.
- Existing rows are never changed; a row with different values is reported.
- Real runs refuse to start while rows are missing. A dry run seeds them inside its rolled-back transaction.

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

**Exit codes:** `0` SUCCESS, `1` PARTIAL or FAILED, `2` invalid arguments or missing reference rows.

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
- **Only prices are stored:** volumes and congestion need a schema migration.
- **No publication timestamp** from the source.
- **Empty responses:** `data: []` is ambiguous.
- **Revisions:** IEX doesn't document whether or when published values are revised. Any change is kept as a new version.
