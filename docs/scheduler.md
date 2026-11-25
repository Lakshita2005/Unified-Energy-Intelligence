# Availability-based scheduler (`pi-scheduler` / `pi-worker`)

The scheduler keeps the database current by polling each source for newly published data and turning
it into ingestion jobs. A worker executes those jobs by calling the existing ingestion runners. The
pipelines themselves are unchanged.

> **Status (2026-11-25): implemented and tested offline, not active.**
> - Every schedule in `config/scheduler.toml` is `enabled = false`, and `SCHEDULER_GLOBAL_ENABLE=false`.
> - Migration `0011_pi_jobs` is prepared but **not applied**. It needs the schema owner. It is the last step of the approved chain `0009_mosdac → 0010_grid_state_ace → 0012_area_state_map → 0013_iex_market_snapshot → 0011_pi_jobs`; it uses nothing from 0010, 0012 or 0013 (ordering only).
> - No Windows service is installed.
> - Activation candidates (company permission for internal use confirmed 2026-11-25): every implemented
>   dataset. Open-Meteo is set up for its **free endpoint (no API key, non-commercial use only, under
>   10,000 calls a day, CC BY 4.0 attribution)** and may only be activated for non-commercial use. All
>   stay disabled until activation is approved schedule by schedule.
>
> Activation needs separate approvals: [Activation checklist](#activation-checklist).

## Architecture

```
config/scheduler.toml --sync-config--> pi_jobs.schedule
                                            |
pi-scheduler (one instance, advisory lock)  |  every SCHEDULER_TICK_SECONDS
  for each enabled schedule that is due (local windows / intervals):
    checker.check()  -- light probe of the source + read-only compare with pi tables
    -> pi_jobs.availability_check (NEW_DATA / NO_NEW_DATA / NOT_YET_PUBLISHED / LATE / SOURCE_ERROR)
    -> each new or revised item passes the watermark guards -> pi_jobs.job (PENDING, de-duplicated)
       + pi_jobs.source_watermark QUEUED, in the SAME transaction
  every tick: re-verify that this session still holds the scheduler lock (else exit 1)
  every tick: a schedule runs only if its file configuration hash equals the synced one (else refused)
  on start and every SCHEDULER_RECOVERY_INTERVAL_SECONDS: recover expired worker leases
  on start: one immediate check of every enabled schedule (catch-up after downtime)

pi-worker (WORKER_CONCURRENCY slots)
  claim a due job (FOR UPDATE SKIP LOCKED; no other RUNNING job in its lock_group)
  take the advisory lock of the lock_group (one running job per source)
  renew the lease every WORKER_HEARTBEAT_SECONDS while the existing runner executes
  -> pi.ingestion_run / pi.source_document / pi.<tables> / pi.data_quality_issue / RAW_DATA_DIR
  -> SUCCEEDED / PARTIAL / NOT_PUBLISHED / RETRY_WAIT (exponential backoff) / FAILED
  -> job state + job_attempt + pi_jobs.source_watermark in ONE transaction; the watermark changes only
     while it still refers to the job's own version (an old job never relabels a newer revision)
```

**Why not Celery:**
- Celery has no official Windows support, and the server runs Windows Server 2019.
- It needs a broker (Redis or RabbitMQ) that is not installed.
- A PostgreSQL queue gives the same guarantees with the existing database: persistent jobs, `SKIP LOCKED` claiming, leases and advisory locks.

### From publication to PostgreSQL (example: the Grid-India workbook for 24-Nov)

1. **10:24 IST on 25-Nov:** NLDC uploads `24.11.26_NLDC_PSP_597.xls`.
2. **10:30 (next 15-minute check in the 09:00–14:00 window):** `GridIndiaPspChecker` requests the file listing for the neighbouring months and finds the workbook. It reads `source_document` (read-only) and sees that the file is not registered for the nine section datasets.
   - The check result is NEW_DATA.
   - Item `2026-11-24|24.11.26_NLDC_PSP_597.xls`, version = hash(path, CreatedOn).
3. **Guards:** no watermark exists for this version and no active job has the same `dedupe_key`. So a PENDING job is inserted with target `{"runner": "grid_india_psp", "start_date": "2026-11-24", "end_date": "2026-11-24", "sections": [...]}`, and the watermark becomes QUEUED.
4. **Worker:** within `WORKER_POLL_SECONDS`, a worker slot claims the job, takes the `GRID_INDIA` lock and calls `run_grid_india_psp`. The workbook is downloaded once, stored write-once under `RAW_DATA_DIR`, and each section is parsed, validated and loaded in its own `ingestion_run`.
5. **Recording the result:** every section succeeds, so the job becomes SUCCEEDED with its `ingestion_run_ids` and record counts, one `job_attempt` row is written, and the watermark becomes INGESTED.
6. **Next check (10:45):** the file is registered in every section, so there is nothing new and no job.
   - A same-day revision (a new file name) is detected the same way. It is loaded as a new version, and the earlier rows are kept.

## Datasets and availability strategies

| Schedule | Datasets | Detection (probe vs stored state) | Item / version | Default checking |
|---|---|---|---|---|
| `CEA_COAL_DAILY` | CEA_COAL_STOCK_DAILY | For each report date in the catch-up window ending yesterday:<br>• not stored → GET the file. 404 or an HTML page = not published (LATE after 48 h); a workbook = new.<br>• stored → on the first check of the day, the last 2 dates are re-fetched and compared by SHA-256 (revision). | `date\|coal` / file SHA-256 | 30 min in 12:00–24:00, else 2 h |
| `CEA_DGR_DAILY` | CEA_DGR_DAILY | Same, for dgr10 (outages) and dgr2 (plant-daily) separately | `date\|dgr10`, `date\|dgr2` | 30 min in 14:00–24:00, else 2 h |
| `CEA_PSP_MONTHLY` | CEA_PSP_ENERGY/PEAK_MONTHLY | The listing page per measure; a listed month that isn't stored is new. A stored month not all `Revised` → once a day, compare the PDF's SHA-256 (revision). A month listed twice → source error, no job. | `MEASURE\|YYYY-MM` / `listed` or PDF SHA-256 | 10:00 and 18:00 |
| `IEX_DAM`, `IEX_GDAM` | IEX_DAM / IEX_GDAM | One snapshot request for the catch-up window up to tomorrow. Published MKT blocks not stored, or with a changed price, → new or revision. An empty day → not published (LATE after D-1 16:30). | `date` / hash of the published blocks | 15 min in 12:30–16:00, else 6 h |
| `IEX_RTM` | IEX_RTM | Same, up to today. **Partial days are normal:** each growth in published blocks (40 → 52 → 96) is a new version, and the job adds only the new blocks. | `date` / hash of the published blocks | 30 min |

**IEX jobs load two tables.** One `run_iex` call loads `iex_market_price` and then, from the same stored snapshot document, `iex_market_snapshot` (dataset `IEX_*_SNAPSHOT`). The job records both run ids and each load's outcome (`result.components.price` / `.snapshot`):
- **SUCCEEDED** only if both loads succeeded.
- **PARTIAL** if either load rejected records.
- **FAILED**, retried by the error class, if either load failed.
- **FAILED (permanent)** if the price load succeeded but the snapshot was not loaded at all.

A retry is safe: the price document is then a duplicate and skipped by SHA-256, while its stored snapshot document is offered to the snapshot load again.
| `GRID_INDIA_PSP` | the nine Grid-India section datasets | A workbook listed but not registered in every section → job for the missing sections only. A revision has a new file name. Not listed → not published (LATE after D+1 16:00). | `date\|file` / hash(path, CreatedOn) | 15 min in 09:00–14:00, else 1 h |
| `NPP_DEMAND`, `NPP_GENERATION` | NPP_DEMAND / NPP_GENERATION | Request today (and yesterday on the first check of the day, plus days with nothing stored). A newer `updated_on` than stored → new. | `date` / newest `updated_on` | 30 min |
| `MOSDAC_SOLAR` | MOSDAC_SOLAR_FORECAST | Request solar for one reference plant (1 request, not one per plant). A different series start than the newest in `mosdac_solar_forecast` → new series → job `run_mosdac("solar")` for all plants. | `series\|start` | 3 h |
| `MOSDAC_WIND` | MOSDAC_WIND_FORECAST | The same for wind, against `mosdac_wind_forecast`; job `run_mosdac("wind")`. **Independent of solar:** its own checks, jobs, retries and watermarks. A wind outage (HTTP 500) is a SOURCE_ERROR of this schedule only; it never fails or hides a solar ingestion, and wind recovers on its own next check without waiting for a new solar series. Both share the `MOSDAC` lock group (one MOSDAC job at a time). | `series\|start` | 1 h |
| `OPEN_METEO_FORECAST` | OPEN_METEO_FORECAST | No issue time is available, so the forecast is refreshed per slot (01:30, 07:30, 13:30, 19:30). The latest passed slot without an ingested watermark → refresh. Missed slots collapse into the latest. The check makes no request. | `slot\|time` | slots every 6 h |
| `OPEN_METEO_HISTORICAL` | OPEN_METEO_HISTORICAL | today − 6 days (ERA5 delay). **Disabled; catch-up off.** | `date` | 03:00 |

**Open-Meteo plan and usage limits.** `OPEN_METEO_PLAN` makes the endpoint an explicit choice:

| `OPEN_METEO_PLAN` | Endpoint | `OPEN_METEO_API_KEY` | Use |
|---|---|---|---|
| `free` (default) | `https://api.open-meteo.com/v1/forecast`, `https://archive-api.open-meteo.com/v1/archive` | **must be empty** | **non-commercial only**, fewer than 10,000 API calls a day (also 5,000 an hour, 600 a minute) |
| `commercial` (future) | `customer-` hosts of the same APIs | required | paid subscription |

- The scheduler refuses Open-Meteo schedules when plan and key disagree. On the free plan a key would make the pipeline switch to the paid `customer-` host, so the combination is refused rather than changing endpoint silently.
- On the free plan, the scheduler also refuses them when the **worst-case** daily calls of all enabled Open-Meteo schedules exceed `OPEN_METEO_DAILY_CALL_LIMIT` (10,000). Worst case = requests per day (slots, or catch-up days) × locations × `max_attempts` × Open-Meteo's request weight (a request with more than 10 variables or more than 14 days counts as several calls). The shipped configuration with the 8 configured plants: forecast 4 × 8 × 5 = 160, historical 1 × 8 × 5 = 40, so **200 calls a day at most**. `pi_jobs validate-config` prints the estimate. Locations registered only in the database, and manual `ingest_open_meteo` runs, are not counted: keep those well within the remaining budget.
- **Attribution:** Open-Meteo data is licensed under CC BY 4.0. Any report, dashboard or export that shows or shares it must credit "Weather data by Open-Meteo.com" with a link to https://open-meteo.com/ and to the licence, and must say if the data was changed.

**Not scheduled:**
- IMD_AWS_OBS and IMD_CITY_FORECAST: never validated.
- WBES_PX_*: needs a personal interactive login.
- DSM_SCHEDULE_INTERSTATE and DSM_SETTLEMENT_REGIONAL: pending, no pipeline.
- CEA_COAL_STOCK_MONTHLY: no pipeline.

**Adding a dataset:**
1. Write an `AvailabilityChecker` (`src/scheduler/checkers/`) and register it in `CHECKERS`.
2. Add an adapter in `src/scheduler/executors.py` if it uses a new runner.
3. Add a `[[schedule]]` entry.

## Database: schema `pi_jobs` (migration `0011_pi_jobs`)

| Table | Contents |
|---|---|
| `schedule` | The configuration copied from `config/scheduler.toml` (`sync-config`, which never touches `enabled`). `enabled` is the **admin-only** database switch (`pi_jobs enable/disable`); `pi_app` has no UPDATE privilege on it. The stored `config` is hashed and compared with the file on every tick |
| `availability_check` | Every check: time, outcome, number of detections and jobs, details (items, versions, not-published and late items), duration, error and class |
| `job` | The queue:<br>• status, attempts and `max_attempts`, backoff, `next_attempt_at`;<br>• lease owner and expiry;<br>• `ingestion_run_ids` and record counts (read, inserted, updated, unchanged, rejected, quality issues);<br>• last error and class, target, details.<br>**At most one active job per `dedupe_key`**, through a partial unique index. |
| `job_attempt` | Every attempt, including retries, with its outcome, runs, counts and error |
| `source_watermark` | Per (schedule, item): the last version and its state (QUEUED, INGESTED, INGESTED_WITH_REJECTS, NOT_PUBLISHED, FAILED, QUARANTINED) |
| `process_heartbeat` | Liveness of the scheduler and workers (for `health`) |

- **Unchanged:** no existing `pi` table is changed. Jobs refer to `pi.ingestion_run` ids.
- **Owner:** `pi` and the database are owned by `postgres`, and `pi_app` has no CREATE right. So **the migration must be applied by the schema owner** (`postgres` or a role with CREATE on the database): see [Applying migration 0011](#applying-migration-0011-as-the-schema-owner).
- **Application role (`pi_app`, or `PI_JOBS_APP_ROLE`), least privilege:** USAGE on `pi_jobs`, USAGE/SELECT on its identity sequences, and:

  | Table | `pi_app` |
  |---|---|
  | `schedule` | SELECT; INSERT/UPDATE on every column **except `enabled`** |
  | `availability_check` | SELECT, INSERT, UPDATE |
  | `job` | SELECT, INSERT, UPDATE |
  | `job_attempt` | SELECT, INSERT (append-only) |
  | `source_watermark` | SELECT, INSERT, UPDATE |
  | `process_heartbeat` | SELECT, INSERT, UPDATE |

  **No DELETE or TRUNCATE**: the application never deletes (retention is an owner task, below). The advisory locks need no privilege.
- **Schedule enablement is admin-only.** Recommended: keep it with the schema owner, or create a small dedicated admin role (for example `pi_jobs_admin`, NOLOGIN, granted to the named administrators) and pass it as `PI_JOBS_ADMIN_ROLE` when applying the migration. That role gets USAGE, SELECT on all `pi_jobs` tables and UPDATE (`enabled`) on `schedule` only. Run `pi_jobs enable <KEY>` with that role's credentials supplied the same way as for the migration (never in `.env`). This separates "the configuration says it may run" (git-reviewed file, synced by the application) from "an administrator switched it on" (database), and the hash check stops an unsynced file edit from running.
- **Foreign keys:** `job.check_id → availability_check` and `source_watermark.last_job_id → job` are `ON DELETE SET NULL`; `job_attempt.job_id → job` is `ON DELETE CASCADE`. See [Retention](#retention).
- **Downgrade:** refused while `pi_jobs.job` holds rows, unless `PI_JOBS_ALLOW_DROP=yes`.

### Applying migration 0011 as the schema owner

Never put the `postgres` password in `.env`, in a command-line argument, in a script file or in shell history. The application's `.env` keeps `DB_USER=pi_app`; the owner identity is supplied for **one PowerShell process only**: environment variables override `.env`, and an empty `DB_PASSWORD` produces a connection URL without a password, so libpq takes it from `PGPASSWORD` or the password file.

```powershell
cd C:\PI\app
# 1. Review the SQL offline (no connection): renders 0010, 0012, 0013 and 0011 in chain order.
.\.venv\Scripts\alembic.exe upgrade 0009_mosdac:head --sql > "$env:TEMP\pi_0010_to_0011.sql"

# 2. Owner identity for this process only (optional admin role for 'pi_jobs enable')
$env:DB_USER = 'postgres'
$env:DB_PASSWORD = ''                       # empty: no password in the URL, libpq supplies it
$env:PI_JOBS_ADMIN_ROLE = ''                # e.g. 'pi_jobs_admin' if that role already exists

#    Password, option A: prompt (typed text is not echoed and not saved in history)
$secure = Read-Host -AsSecureString 'postgres password'
$env:PGPASSWORD = [System.Net.NetworkCredential]::new('', $secure).Password
Remove-Variable secure
#    Password, option B: %APPDATA%\postgresql\pgpass.conf of the administrator running this
#    (line format host:port:database:user:password, created with a text editor, file ACL limited
#    to that administrator). Then nothing is set in this step.

# 3. Rolled-back dry run (as for 0002-0009), then the real upgrade - each only when approved
#    (dry run: tests/support/alembic_tx.migrate_in_transaction on a connection that is rolled back)
.\.venv\Scripts\alembic.exe upgrade head            # 0010 -> 0012 -> 0013 -> 0011, one transaction

# 4. Clear the owner identity immediately
Remove-Item Env:PGPASSWORD, Env:DB_USER, Env:DB_PASSWORD, Env:PI_JOBS_ADMIN_ROLE -ErrorAction SilentlyContinue
```

Close the window afterwards. Check that PowerShell transcription is not writing this session to disk (`Get-ItemProperty HKLM:\SOFTWARE\Policies\Microsoft\Windows\PowerShell\Transcription -ErrorAction SilentlyContinue`); even so, the typed password itself is never part of a command.

## Reliability

| Requirement | Mechanism |
|---|---|
| No duplicate ingestion | 1. **Detection:** checkers compare the source with stored state.<br>2. **Watermarks:** the same version, once INGESTED, is never queued again.<br>3. **Queue:** one active job per `schedule\|item\|version`.<br>4. **Pipelines:** identical documents are skipped by SHA-256; natural-key versioning skips unchanged rows. Revisions become new versions and earlier rows are kept. |
| No overlapping jobs | A single scheduler (advisory lock `pi.jobs:scheduler`). Claims skip groups that have a RUNNING job, and the worker also holds the session advisory lock `pi.jobs:group:<lock_group>`. Different sources run in parallel up to `WORKER_CONCURRENCY` (2). |
| Retries | **TRANSIENT** (timeouts, connection errors, HTTP 408/425/429/5xx) → `RETRY_WAIT` after `backoff_base × 2^(attempt−1)` (2, 4, 8, 16 min, capped at 30 min), at most `max_attempts` (5). The HTTP client's own retries happen inside each attempt.<br>**NOT_PUBLISHED** (404, empty IEX data, missing listing) → never an error; detected again later.<br>**PERMANENT** (format changes, validation, invalid arguments) → FAILED, no retry; the watermark is QUARANTINED until someone runs `requeue`.<br>After retries are exhausted, the item waits `failure_cooldown` (6 h) before it can be queued again. |
| Atomic state | A job and its QUEUED watermark are inserted in one transaction; a job's final state, its attempt row and its watermark are written in one transaction. A watermark is only updated by a job of the **same version** (a late finish of an older version never marks a newer revision as ingested). `requeue` refuses a job whose item now has a different version unless `--force`, and never relabels the newer version. |
| Recovery | Jobs live in PostgreSQL, so a restart loses nothing. A worker that crashes or is killed stops renewing its lease; after `WORKER_LEASE_SECONDS` (10 min) the scheduler puts the job back to RETRY_WAIT (FAILED, with a FAILED watermark in the same transaction, if no attempts remain). A late finish by the old worker is ignored. Session advisory locks vanish with the crashed process. On start, the scheduler recovers leases and checks every enabled schedule immediately (**catch-up**); checkers look back up to `catchup_max_days` (7), never further. |
| Single scheduler | The scheduler re-checks on **every tick** (in `pg_locks`, on its own session) that it still holds `pi.jobs:scheduler`. If the session was lost (database restart, network drop, terminated backend), another scheduler may already hold the lock, so it stops at once with exit code 1 and the service wrapper restarts it; the restarted process waits for the lock. |
| Graceful stop | Ctrl+C, Ctrl+Break or SIGTERM (what service wrappers send), or the optional stop file. All waits are cut into 1-second slices, so a stop request is noticed within about a second (on Windows a console control event reaches Python only when a blocking wait returns; a single 30 s wait delayed the stop by up to 30 s). The scheduler stops after the current check. Workers stop claiming and wait up to `WORKER_SHUTDOWN_GRACE_SECONDS` (300, one shared deadline for all slots) for running jobs; anything cut off is recovered through its lease. |
| Monitoring | `availability_check`, `job` and `job_attempt` hold every timestamp, count, retry and error.<br>`python -m scripts.pi_jobs status` shows: each schedule's last check and outcome; active jobs; the last success; process heartbeats.<br>`health` exits non-zero when the scheduler or worker heartbeat is older than `--max-age`. |

## Configuration

`config/scheduler.toml` is reviewed in git. Per schedule it sets:
- `enabled`, `checker`, `datasets`, `lock_group`;
- `every` (outside the windows) and `windows` (local `start`–`end` with their own `every`; a window may cross midnight; `24:00` means end of day);
- `timezone` (`Asia/Kolkata`), `max_attempts`, `backoff_base`, `backoff_max`, `failure_cooldown`, `catchup_enabled`, `catchup_max_days` (at most 31), `priority`;
- checker `params`.

**A schedule runs only when all of these are true** (re-evaluated every tick):
1. `SCHEDULER_GLOBAL_ENABLE=true`;
2. `enabled = true` in the file;
3. the file's configuration of that schedule is **identical** to the copy synced into `pi_jobs.schedule` (SHA-256 of the canonical JSON). After any edit of `config/scheduler.toml`, the schedule is refused, with one warning in the log and in `pi_jobs status`, until `sync-config` is run;
4. enabled in `pi_jobs.schedule` by an administrator (`pi_jobs enable <KEY>`; `sync-config` never changes it, new rows start disabled);
5. Open-Meteo only: `OPEN_METEO_PLAN` and `OPEN_METEO_API_KEY` agree (free = no key; commercial = key), and on the free plan the worst-case daily calls stay within `OPEN_METEO_DAILY_CALL_LIMIT`. See [Open-Meteo plan and usage limits](#datasets-and-availability-strategies).

**Environment** (`.env`; none of these is a secret):

| Variable | Default |
|---|---|
| `SCHEDULER_GLOBAL_ENABLE` | `false` |
| `SCHEDULER_CONFIG_FILE` | `config/scheduler.toml` |
| `SCHEDULER_TIMEZONE` | `Asia/Kolkata` (fallback if the file sets none) |
| `SCHEDULER_TICK_SECONDS` | 30 |
| `SCHEDULER_RECOVERY_INTERVAL_SECONDS` | 300 |
| `SCHEDULER_JOBS_SCHEMA` | `pi_jobs` |
| `SCHEDULER_LOG_FILE_NAME` | `pi-scheduler.log` |
| `SCHEDULER_CHECK_LOG_FILE_NAME` | `pi-scheduler-check.log` (manual `pi_scheduler check`) |
| `WORKER_LOG_FILE_NAME` | `pi-worker.log` |
| `WORKER_ONCE_LOG_FILE_NAME` | `pi-worker-once.log` (manual `pi_worker once`) |
| `LOG_TO_CONSOLE` | `true`; set `false` for the services (see deployment) |
| `WORKER_CONCURRENCY` | 2 |
| `WORKER_POLL_SECONDS` | 10 |
| `WORKER_LEASE_SECONDS` | 600 |
| `WORKER_HEARTBEAT_SECONDS` | 30 |
| `WORKER_SHUTDOWN_GRACE_SECONDS` | 300 |
| `SCHEDULER_STOP_FILE`, `WORKER_STOP_FILE` | optional |

**Logs:** one rotating file per process in `LOG_DIR`, sized by `LOG_MAX_BYTES` (10 MB) with `LOG_BACKUP_COUNT` (5) backups. Python's `RotatingFileHandler` is not safe for two processes writing one file, so the manual `check` and `once` commands use their own files and never interleave with, or rotate, a running service's log. Pipeline logs of a job go to the worker's file. `pi_jobs` logs to stderr only.

## Commands

```powershell
cd C:\PI\app
.\.venv\Scripts\python.exe -m scripts.pi_jobs validate-config            # offline
.\.venv\Scripts\python.exe -m scripts.pi_jobs sync-config --dry-run      # needs 0011; rolled back
.\.venv\Scripts\python.exe -m scripts.pi_jobs sync-config
.\.venv\Scripts\python.exe -m scripts.pi_scheduler check --schedule IEX_RTM   # live probe, writes nothing
.\.venv\Scripts\python.exe -m scripts.pi_scheduler run                   # service process
.\.venv\Scripts\python.exe -m scripts.pi_worker run                      # service process (2 slots)
.\.venv\Scripts\python.exe -m scripts.pi_worker once                     # one job, then exit
.\.venv\Scripts\python.exe -m scripts.pi_jobs status
.\.venv\Scripts\python.exe -m scripts.pi_jobs health --max-age 180
.\.venv\Scripts\python.exe -m scripts.pi_jobs list --status FAILED
.\.venv\Scripts\python.exe -m scripts.pi_jobs requeue 123              # refused if a newer version exists
.\.venv\Scripts\python.exe -m scripts.pi_jobs requeue 123 --force      # re-run anyway (fetches current data)
.\.venv\Scripts\python.exe -m scripts.pi_jobs cancel 124
# ADMIN role only (pi_app gets "permission denied"):
.\.venv\Scripts\python.exe -m scripts.pi_jobs enable IEX_RTM
.\.venv\Scripts\python.exe -m scripts.pi_jobs disable IEX_RTM
```

**Exit codes** (`pi_scheduler`, `pi_worker`):

| Code | Meaning | Service wrapper |
|---|---|---|
| `0` | normal stop | — |
| `1` | runtime failure: scheduler lock already held or lost, unexpected error | restart (after a delay) |
| `2` | configuration error: migration missing, invalid `scheduler.toml`, invalid option | **do not restart** (it would fail the same way forever) |

`pi_jobs`: `0` ok, `1` refused / unhealthy / database or permission error, `2` configuration error.

## Windows Server 2019 deployment (not installed yet)

**Recommended:** two Windows services wrapping the console processes with **NSSM**, the Non-Sucking Service Manager (a single `nssm.exe`, nothing else installed).
- **What it provides:** automatic start at boot (delayed), restart after a crash, a graceful stop by Ctrl+C, and its own rotation for stdout/stderr capture.
- **Why a wrapper:** `sc.exe create` cannot run a Python script directly as a service (it needs a service-aware executable).
- **Alternative:** WinSW, an equivalent XML-configured wrapper, works the same way; see below.
- **Not recommended:** Task Scheduler, which can't send Ctrl+C. Use it only as a fallback, with the stop files.

**Prerequisites:**
- **NSSM binary:** NSSM 2.24 (2014) is the last stable release and the project is no longer actively maintained; on current Windows versions the 2.24-101 pre-release build is commonly used. Download it only from nssm.cc, record and verify its SHA-256 against a trusted copy, and place it where only administrators can write (e.g. `C:\PI\tools\nssm\nssm.exe`). If an unmaintained binary is not acceptable, use WinSW (below).
- **Service account:** a dedicated, non-administrator account; never LocalSystem.
  - **Preferred:** a group managed service account (gMSA, e.g. `DOMAIN\gmsa_pi$`) if the server is domain-joined: Windows manages its password, so no password is ever handled.
  - **Otherwise:** a local account (e.g. `.\svc_pi`) with a long random password kept in the company password vault.
  - **Rights:** "Log on as a service" (`secpol.msc` → Local Policies → User Rights Assignment). Setting the account in `services.msc` grants it automatically; the CIM method below does **not**, so grant it explicitly and check that no group policy removes it ("Deny log on as a service" must not include the account). It needs no interactive logon.
  - **File access:** read on `C:\PI\app` (code, `.venv`, `config`) and `.env`; modify on `LOG_DIR` (`C:\PI\app\logs`) and `RAW_DATA_DIR`; nothing else.
  - **Network:** outbound HTTPS to the source hosts and the PostgreSQL port.
- **`.env` permissions:** restrict to the service account and administrators, because it holds the `pi_app` password. (It never holds the `postgres` password.)
- **PostgreSQL service name** (for the dependency): `Get-Service *postgres*`.

```powershell
# Run in an elevated PowerShell. Replace <POSTGRES_SERVICE>. NO password appears in any command.
$nssm = "C:\PI\tools\nssm\nssm.exe"
$py   = "C:\PI\app\.venv\Scripts\python.exe"
$app  = "C:\PI\app"
foreach ($svc in @(@{n="pi-scheduler"; a="-m scripts.pi_scheduler run"; d="PI availability scheduler"},
                   @{n="pi-worker";    a="-m scripts.pi_worker run";    d="PI ingestion worker"})) {
  & $nssm install $svc.n $py $svc.a
  & $nssm set $svc.n AppDirectory $app
  & $nssm set $svc.n DisplayName $svc.d
  & $nssm set $svc.n Start SERVICE_DELAYED_AUTO_START
  & $nssm set $svc.n DependOnService <POSTGRES_SERVICE>
  # The application writes its own rotating log; no duplicate console copy. stdout/stderr capture
  # then only holds start-up failures and crash tracebacks.
  & $nssm set $svc.n AppEnvironmentExtra PYTHONUNBUFFERED=1 LOG_TO_CONSOLE=false
  & $nssm set $svc.n AppStdout "$app\logs\$($svc.n).service.log"
  & $nssm set $svc.n AppStderr "$app\logs\$($svc.n).service.log"
  & $nssm set $svc.n AppRotateFiles 1
  & $nssm set $svc.n AppRotateOnline 1
  & $nssm set $svc.n AppRotateBytes 10485760
  & $nssm set $svc.n AppExit Default Restart       # 0 / 1 / crash: restart ...
  & $nssm set $svc.n AppExit 2 Exit                # ... but NOT a configuration error (exit code 2)
  & $nssm set $svc.n AppRestartDelay 30000         # wait 30 s before restarting
  & $nssm set $svc.n AppThrottle 60000
  & $nssm set $svc.n AppStopMethodSkip 6           # stop = Ctrl+C only (skip WM_CLOSE / thread messages)
  # SCM recovery actions cover a crash of nssm.exe itself. Do NOT enable 'sc.exe failureflag':
  # it would also fire them after a deliberate exit 2 and recreate the restart loop.
  sc.exe failure $svc.n reset= 86400 actions= restart/60000/restart/60000/restart/300000
}
& $nssm set pi-scheduler AppStopMethodConsole 60000    # Ctrl+C, then up to 60 s (stops within ~1 s)
& $nssm set pi-worker    AppStopMethodConsole 400000   # >= 360 s: grace 300 s + margin for the last commit

# Log-on account, without a password on any command line:
#   gMSA:   sc.exe config pi-scheduler obj= 'DOMAIN\gmsa_pi$'   (same for pi-worker; no password exists)
#   local:  services.msc -> pi-scheduler -> Log On -> This account (typed into the dialog), or:
$cred = Get-Credential -UserName '.\svc_pi' -Message 'Log-on account for pi-scheduler and pi-worker'
foreach ($n in 'pi-scheduler', 'pi-worker') {
  $r = Get-CimInstance Win32_Service -Filter "Name='$n'" |
       Invoke-CimMethod -MethodName Change -Arguments @{ StartName = $cred.UserName;
                                                         StartPassword = $cred.GetNetworkCredential().Password }
  "$n log-on change: ReturnValue $($r.ReturnValue)"     # 0 = success
}
Remove-Variable cred
# Then grant "Log on as a service" to the account (secpol.msc) - the CIM method does not.

# Do NOT start yet: services stay stopped until activation is approved.
# Later:  Start-Service pi-worker, pi-scheduler   /   Stop-Service pi-scheduler, pi-worker
```

**Why not `nssm set ... ObjectName <account> <password>`:** the password would be on the command line (visible to other processes while it runs, in PSReadLine history and in any transcription or process-creation audit log). `Get-Credential` and the `services.msc` dialog take it as a prompt instead.

**System shutdown:** during an OS shutdown Windows gives services only a short, fixed time (`WaitToKillServiceTimeout`, a few seconds by default), whatever `AppStopMethodConsole` says. A job cut off then is recovered through its lease after restart; no data is lost because every load is transactional.

**After installing, verify before activation:**
1. `nssm get pi-worker AppExit 2` returns `Exit`, and `nssm get pi-worker AppStopMethodConsole` returns `400000`.
2. Start `pi-scheduler` once while migration 0011 is missing or with an invalid `SCHEDULER_CONFIG_FILE`: the service must end **stopped** with exit code 2 in `pi-scheduler.service.log`, without restarting.
3. `Stop-Service pi-scheduler` completes within a few seconds and the log shows `Stop requested (SIGINT)`.

**How the requirements are met:**

| Requirement | How |
|---|---|
| Automatic start after reboot | `SERVICE_DELAYED_AUTO_START`, after PostgreSQL |
| Restart after a crash | NSSM `AppExit Default Restart` (exit 0/1 and crashes), plus SCM recovery actions for an `nssm.exe` crash; **no restart** on exit code 2 (configuration error) |
| Graceful shutdown | Ctrl+C triggers the processes' signal handler, noticed within about 1 s; the worker gets 400 s (> its 300 s grace); a stuck process is killed only after the timeout, and its job is recovered through its lease |
| Separate logs and rotation | The application's own rotating `pi-scheduler.log` and `pi-worker.log` (manual commands use separate files). With `LOG_TO_CONSOLE=false` nothing is logged twice; NSSM-rotated `*.service.log` keeps only start-up errors and crash output |
| Health and status | `pi_jobs health` (exit code), runnable from monitoring or a Task Scheduler job every 5 minutes; `pi_jobs status`; `Get-Service pi-*` |
| Recovery after restart | Automatic: expired leases are requeued and a catch-up check runs on start |

**WinSW alternative:** create `pi-worker.xml` next to `WinSW.exe`, renamed `pi-worker.exe`:

```xml
<service>
  <id>pi-worker</id><name>PI ingestion worker</name>
  <executable>C:\PI\app\.venv\Scripts\python.exe</executable>
  <arguments>-m scripts.pi_worker run</arguments>
  <workingdirectory>C:\PI\app</workingdirectory>
  <startmode>Automatic</startmode><delayedAutoStart>true</delayedAutoStart>
  <env name="LOG_TO_CONSOLE" value="false"/>
  <stoptimeout>400 sec</stoptimeout>
  <onfailure action="restart" delay="30 sec"/><onfailure action="restart" delay="60 sec"/>
  <log mode="roll-by-size"><sizeThreshold>10240</sizeThreshold><keepFiles>5</keepFiles></log>
  <!-- service account (WinSW v3 syntax): <serviceaccount><username>DOMAIN\gmsa_pi$</username><allowservicelogon>true</allowservicelogon></serviceaccount>
       (gMSA, no password). For a local account, set it in services.msc instead of writing a password here. -->
</service>
```

Then `pi-worker.exe install`. Do the same for `pi-scheduler` with a `<stoptimeout>` of 60 seconds. WinSW sends Ctrl+C on stop. WinSW's `onfailure` applies to every non-zero exit; check its documentation for excluding exit code 2, or rely on `AppThrottle`-style back-off and the health check.

**Task Scheduler fallback** (no extra binary; no Ctrl+C, so set `SCHEDULER_STOP_FILE` and `WORKER_STOP_FILE` and create those files to stop):

```powershell
schtasks /Create /TN "PI\pi-scheduler" /SC ONSTART /DELAY 0002:00 /RU svc_pi /RP * /RL LIMITED /TR "C:\PI\app\.venv\Scripts\python.exe -m scripts.pi_scheduler run" /F
```

Set "Restart on failure" and "Start in: C:\PI\app" in the task's settings.

## Activation checklist

Every step needs its own approval.

1. **Migrations**, by the schema owner ([procedure](#applying-migration-0011-as-the-schema-owner)):
   - review the SQL first: `alembic upgrade 0009_mosdac:head --sql` (0010 → 0012 → 0013 → 0011);
   - then a rolled-back dry run (as for 0002–0009);
   - then `alembic upgrade head` (0011 is the head; this applies 0010, 0012 and 0013 as well, in one transaction).
2. **Integration tests** for the queue: `pytest tests/integration/test_scheduler_queue.py`, as `pi_app`. Every row is written inside one transaction that is always rolled back; one test opens and terminates its own extra session (advisory lock only, no rows). The retention test is skipped for `pi_app` (no DELETE) and runs only as the owner, also rolled back.
3. **Copy the configuration:** `pi_jobs sync-config --dry-run`, then `sync-config` (all rows start disabled in the database).
4. **Live probes:** `pi_scheduler check --schedule <KEY>` for each candidate. These are live probes that write nothing.
5. **Per approved schedule:** set `enabled = true` in the file, `sync-config`, then the administrator runs `pi_jobs enable <KEY>`; `pi_jobs status` must show `would_run: true` only for approved schedules.
6. **Services:** install them (stopped) and run the post-install checks, set `SCHEDULER_GLOBAL_ENABLE=true`, then start `pi-worker` and `pi-scheduler`.
7. **Watch the first cycle:** `pi_jobs status` and `pi_jobs list`; compare job results with `pi.ingestion_run`.

**Activation candidates** (2026-11-25: the company confirmed permission to use the source data for internal operations):

| Schedule(s) | Candidate | Note |
|---|---|---|
| `CEA_COAL_DAILY`, `CEA_DGR_DAILY`, `CEA_PSP_MONTHLY` | yes | |
| `IEX_DAM`, `IEX_GDAM`, `IEX_RTM` | yes | |
| `GRID_INDIA_PSP` (all nine sections) | yes | migration 0010 (ACE columns) is part of the same chain, before 0011 |
| `NPP_DEMAND`, `NPP_GENERATION` | yes | |
| `MOSDAC_SOLAR`, `MOSDAC_WIND` | yes | wind endpoint has had HTTP 500 outages; independent of solar |
| `OPEN_METEO_FORECAST`, `OPEN_METEO_HISTORICAL` | **only for non-commercial use** | free endpoint, no API key, fewer than 10,000 calls a day, CC BY 4.0 attribution. Disabled by default. Activate only after confirming the use is non-commercial; any commercial use needs a paid subscription and `OPEN_METEO_PLAN=commercial` with `OPEN_METEO_API_KEY` |

## Retention

Nothing is deleted automatically, and `pi_app` cannot delete. Growth with every candidate enabled: roughly 300–400 `availability_check` rows a day (each up to a few kB of `detail`), a few dozen `job` and `job_attempt` rows a day, and one `source_watermark` row per detected item.

**Requirements:**
- Keep `job`, `job_attempt` and `source_watermark` for at least as long as `pi.ingestion_run` history is kept: they explain why and when each run happened.
- Never delete a `source_watermark` row of an item that can still be detected (inside `catchup_max_days`, or a CEA PSP month still re-checked): without it the item would be queued and ingested again. Identical documents would still be skipped by the pipelines, but the job would run.
- Never delete active jobs (PENDING, RUNNING, RETRY_WAIT).
- `availability_check` can be trimmed much earlier (e.g. 90 days); it is monitoring data.

**Foreign-key implications:**
- Deleting `availability_check` rows sets `job.check_id` to NULL (`ON DELETE SET NULL`); jobs keep everything else.
- Deleting a finished `job` deletes its `job_attempt` rows (`CASCADE`) and sets `source_watermark.last_job_id` to NULL (`SET NULL`); the watermark itself, which prevents re-ingestion, is kept.

**Suggested owner-run clean-up** (example; run as the owner or admin role, after approval, never by the application):

```sql
BEGIN;
DELETE FROM pi_jobs.availability_check WHERE checked_at < now() - interval '90 days';
DELETE FROM pi_jobs.job
 WHERE status IN ('SUCCEEDED', 'PARTIAL', 'NOT_PUBLISHED', 'CANCELLED')
   AND finished_at < now() - interval '400 days';
COMMIT;
```

## Known limitations

- **Queue SQL not yet tested on PostgreSQL.** `PostgresJobRepository` is covered by `tests/integration/test_scheduler_queue.py` only once `pi_jobs` exists. The unit tests use an in-memory repository with the same semantics, plus a recording fake connection that checks which statements share a transaction.
- **No committed-row concurrency test.** Two real processes racing on committed rows (claim, enqueue) are not tested, because `pi_app` cannot delete the rows afterwards. A one-off owner-run test in a scratch schema would cover it.
- **Checks download the probe data.** CEA daily files, the IEX snapshot and NPP days are downloaded by the check and again by the job: one extra request per new item. HEAD requests were not used, because HEAD support is unverified.
- **Open-Meteo is cadence-based.** No issue time or verified model-run metadata exists, so its refresh follows the slots, not detection.
- **MOSDAC detects series, not values.** Same-series value revisions without a new series start are not detected (per kind).
- **IEX probes the market price only.** The check compares MKT prices; bid-area-only revisions are picked up whenever the job runs for that day.
- **No catch-up beyond the window.** Catch-up covers at most `catchup_max_days`; longer outages need explicit backfill commands.
- **Retention is manual.** See [Retention](#retention); no automatic clean-up exists.
