# Availability-based scheduler (`pi-scheduler` / `pi-worker`)

The scheduler keeps the database current by polling each source for newly published data and turning
it into ingestion jobs. A worker executes those jobs by calling the existing ingestion runners. The
pipelines themselves are unchanged.

> **Status (2026-11-26): deployed, not active.**
> - `IEX_DAM`, `IEX_GDAM`, `IEX_RTM`, `GRID_INDIA_PSP`, `CEA_COAL_DAILY`, `CEA_DGR_DAILY` and `CEA_PSP_MONTHLY` are `enabled = true` in `config/scheduler.toml` (approved 2026-11-26); every other schedule is disabled. They run only once the database switch (`pi_jobs enable`), `SCHEDULER_GLOBAL_ENABLE=true` and the started tasks are in place.
> - Migrations applied by the schema owner on 2026-11-26: `0009_mosdac → 0010_grid_state_ace → 0012_area_state_map → 0013_iex_market_snapshot → 0011_pi_jobs` (head). 0011 uses nothing from 0010, 0012 or 0013 (ordering only).
> - `pi_jobs.schedule` holds the 13 synced schedules, all disabled in the database. The queue integration tests passed on PostgreSQL 18.7 as `pi_app` (9 passed; the retention test needs the owner).
> - Two Windows Task Scheduler tasks, `\PI\pi-scheduler` and `\PI\pi-worker`, run as `LOCAL SERVICE` and are registered **Disabled**: see [deployment](#windows-server-2019-deployment-task-scheduler).
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
- **Schedule enablement is admin-only.** Recommended: keep it with the schema owner, or create a small dedicated admin role (for example `pi_jobs_admin`, NOLOGIN, granted to the named administrators) and pass it as `PI_JOBS_ADMIN_ROLE` when applying the migration. That role gets USAGE, SELECT on all `pi_jobs` tables and UPDATE (`enabled`) on `schedule` only. Run `pi_jobs enable <KEY>` with that role's credentials supplied the same way as for the migration (never in `.env`). This separates "the configuration says it may run" (git-reviewed file, synced by the application) from "an administrator switched it on" (database), and the hash check stops an unsynced file edit from running. **Current installation:** no admin role was created (`PI_JOBS_ADMIN_ROLE` was empty when 0011 was applied), so `pi_jobs enable/disable` runs with the schema owner's identity.
- **Foreign keys:** `job.check_id → availability_check` and `source_watermark.last_job_id → job` are `ON DELETE SET NULL`; `job_attempt.job_id → job` is `ON DELETE CASCADE`. See [Retention](#retention).
- **Downgrade:** refused while `pi_jobs.job` holds rows, unless `PI_JOBS_ALLOW_DROP=yes`.

### Applying migration 0011 as the schema owner

Applied on 2026-11-26 (rolled-back dry run first, then `alembic upgrade head`; the database is at `0011_pi_jobs`). The procedure stays the reference for later migrations and for every command that needs the owner identity, such as `pi_jobs enable`.

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
| Single scheduler | The scheduler re-checks on **every tick** (in `pg_locks`, on its own session) that it still holds `pi.jobs:scheduler`. If the session was lost (database restart, network drop, terminated backend), another scheduler may already hold the lock, so it stops at once with exit code 1 and the watchdog trigger starts it again. A scheduler that finds the lock held does not wait: it exits with code 1 and is retried at the next watchdog start. |
| Graceful stop | The stop file (`SCHEDULER_STOP_FILE`, `WORKER_STOP_FILE`; the deployed way, because Task Scheduler cannot send Ctrl+C), or Ctrl+C, Ctrl+Break or SIGTERM in a console. All waits are cut into 1-second slices, so a stop request is noticed within about a second. The scheduler stops after the check in progress, and a stop requested before or during start-up skips the lock, lease recovery and the catch-up check. Workers stop claiming and let running jobs finish: after `WORKER_SHUTDOWN_GRACE_SECONDS` (300, one shared deadline for all slots) they log a warning, but the process still waits for the job. Only a killed process cuts a job off; it is then recovered through its lease. |
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
| `LOG_TO_CONSOLE` | `true` (under Task Scheduler the console copy is simply discarded) |
| `WORKER_CONCURRENCY` | 2 |
| `WORKER_POLL_SECONDS` | 10 |
| `WORKER_LEASE_SECONDS` | 600 |
| `WORKER_HEARTBEAT_SECONDS` | 30 |
| `WORKER_SHUTDOWN_GRACE_SECONDS` | 300 |
| `SCHEDULER_STOP_FILE`, `WORKER_STOP_FILE` | optional; deployed: `C:\PI\run\pi-scheduler.stop`, `C:\PI\run\pi-worker.stop` |

**Logs:** one rotating file per process in `LOG_DIR` (deployed: `C:\PI\logs`), sized by `LOG_MAX_BYTES` (10 MB) with `LOG_BACKUP_COUNT` (5) backups. Python's `RotatingFileHandler` is not safe for two processes writing one file, so the manual `check` and `once` commands use their own files and never interleave with, or rotate, a running service's log. Pipeline logs of a job go to the worker's file. `pi_jobs` logs to stderr only.

## Commands

```powershell
cd C:\PI\app
.\.venv\Scripts\python.exe -m scripts.pi_jobs validate-config            # offline
.\.venv\Scripts\python.exe -m scripts.pi_jobs sync-config --dry-run      # needs 0011; rolled back
.\.venv\Scripts\python.exe -m scripts.pi_jobs sync-config
.\.venv\Scripts\python.exe -m scripts.pi_scheduler check --schedule IEX_RTM   # live probe, writes nothing
.\.venv\Scripts\python.exe -m scripts.pi_scheduler run                   # task process (not by hand while the tasks are enabled)
.\.venv\Scripts\python.exe -m scripts.pi_worker run                      # task process (2 slots)
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

| Code | Meaning | Under Task Scheduler |
|---|---|---|
| `0` | normal stop (also: not started because the stop file exists) | started again by the watchdog within 5 min while the task is enabled |
| `1` | runtime failure: scheduler lock already held or lost, unexpected error | started again by the watchdog within 5 min |
| `2` | configuration error: migration missing, invalid `scheduler.toml`, invalid option | also started again every 5 min and fails the same way until fixed; check the last result and the log |

The manual commands above use the development `.venv` and run as the logged-on administrator; the tasks use `C:\PI\venv-svc` as `LOCAL SERVICE`. Both read the same `C:\PI\app\.env`.

`pi_jobs`: `0` ok, `1` refused / unhealthy / database or permission error, `2` configuration error.

## Windows Server 2019 deployment (Task Scheduler)

> **State (2026-11-26):** both tasks are registered and **Disabled**. Each was started once for testing as `LOCAL SERVICE`, with every schedule disabled and the global switch off, and stopped again.

Two Windows **Task Scheduler** tasks run the console processes. Task Scheduler is built into Windows, needs no extra binary and no stored password, and runs the tasks without anyone logged on. What it cannot do, the application covers:

| Need | How it is met |
|---|---|
| Start at boot without a logon | Boot trigger, 2-minute delay, account `LOCAL SERVICE` |
| Restart after a crash or exit | A **watchdog** time trigger every 5 minutes. While the process runs, Task Scheduler ignores it ("do not start a new instance", event 322); after any exit the task is started again within 5 minutes |
| Only one scheduler | The task's single-instance policy, plus the PostgreSQL advisory lock: a second scheduler exits with code 1 |
| Graceful stop | Task Scheduler cannot send Ctrl+C. Stopping uses the **stop files** (below); ending a task kills the process |
| No restart loop on a configuration error | Not possible: exit code 2 is retried by the watchdog every 5 minutes. Visible as last result `2` and in the log |

### Tasks

| | `\PI\pi-scheduler` | `\PI\pi-worker` |
|---|---|---|
| Account | `NT AUTHORITY\LOCAL SERVICE` (S-1-5-19), least privilege, no password | same |
| Program | `C:\PI\venv-svc\Scripts\python.exe` | same |
| Arguments | `-m scripts.pi_scheduler run` | `-m scripts.pi_worker run` |
| Start in | `C:\PI\app` | same |
| Triggers | at startup + 2 min; every 5 min, indefinitely (watchdog) | same |
| If already running | do not start a new instance (`IgnoreNew`) | same |
| Time limit | none (`PT0S`; the Task Scheduler default of 72 h would kill the process) | same |
| Priority | 5 (normal; the default 7 is below normal) | same |
| Restart on failure | every 1 min, 3 times (covers only a failure to **launch**, not an exit code) | same |
| Conditions | none (no battery, idle or network conditions); start when available after a missed trigger | same |

The definitions are in `C:\PI\tools\tasks\task_pi-scheduler.xml` and `task_pi-worker.xml`; the versioned copies are in [`deploy/windows/`](../deploy/windows/README.md). Registered, disabled, with (elevated PowerShell):

```powershell
foreach ($t in 'pi-scheduler', 'pi-worker') {
  Register-ScheduledTask -TaskPath '\PI\' -TaskName $t -Xml (Get-Content "C:\PI\tools\tasks\task_$t.xml" -Raw) `
                         -User 'NT AUTHORITY\LOCALSERVICE' | Out-Null
  Disable-ScheduledTask -TaskPath '\PI\' -TaskName $t | Out-Null
}
```

`C:\PI\venv-svc\Scripts\python.exe` is CPython's venv launcher: it starts the real interpreter (`C:\PI\tools\python313\python.exe`) as a child in a job object that is killed with the launcher, and it returns the child's exit code. So a task shows two `python.exe` processes, ending the task ends both, and the task's last result is the application's exit code.

### Python environment for the tasks

- `C:\PI\tools\python313`: CPython 3.13.14 from the official CPython package on nuget.org (`python`; `python.exe` signed by the Python Software Foundation), outside any user profile.
- `C:\PI\venv-svc`: the tasks' virtual environment, built from that interpreter with exactly the package versions of the development `.venv` (`pip freeze` as constraints; `C:\PI\tools\pi-svc-constraints.txt`).
- **Why not `.venv`:** its base interpreter lives in the Administrator's user profile, which `LOCAL SERVICE` cannot read.
- **After a dependency change:** install the same versions into `venv-svc` (`-r requirements.txt -c <new freeze>`), then run `pip check` and the unit tests with `C:\PI\venv-svc\Scripts\python.exe`.

### Environment and files

- **Settings:** the code reads `C:\PI\app\.env` by its own location, so the tasks get exactly the settings of the manual commands. The tasks set no environment variables of their own. `.env` contains `SCHEDULER_STOP_FILE=C:\PI\run\pi-scheduler.stop` and `WORKER_STOP_FILE=C:\PI\run\pi-worker.stop`.
- **Database:** `pg_hba.conf` requires `scram-sha-256` on 127.0.0.1, so the Windows account does not matter; the `pi_app` password from `.env` is used.
- **Permissions** (applied 2026-11-26; `C:\PI` no longer inherits from `C:\`):

  | Path | SYSTEM, Administrators | `LOCAL SERVICE` | Other local users |
  |---|---|---|---|
  | `C:\PI` (app, `.venv`, `venv-svc`, tools) | full control | read + execute | read + execute |
  | `C:\PI\logs`, `C:\PI\raw_data`, `C:\PI\run` | full control | read + execute, **create files and folders**, full control on what it creates | read + execute |
  | `C:\PI\app\.env` | full control | read | **none** |

  Backup of the previous permissions of every item: `C:\PI\tools\acl-backup\pi_tree_before_hardening.acl`. Restore with `icacls C:\PI /inheritance:e`, then `icacls C:\ /restore C:\PI\tools\acl-backup\pi_tree_before_hardening.acl`.
- **Python caches:** `LOCAL SERVICE` cannot write `__pycache__` in the code folders; Python then compiles in memory (slightly slower start, no error).

### Logs and history

- `C:\PI\logs\pi-scheduler.log` and `C:\PI\logs\pi-worker.log`: rotating (10 MB × 5), created and owned by `LOCAL SERVICE`. Pipeline logs of a job go to the worker's file.
- Manual diagnostics write their own files: `pi-scheduler-check.log` (`pi_scheduler check`), `pi-worker-once.log` (`pi_worker once`).
- `LOG_TO_CONSOLE` stays `true`; under Task Scheduler there is no visible console, so that copy is discarded. Output written before logging is set up (for example an import error) is not captured: the task's last result shows the exit code.
- **Task Scheduler history** is enabled (`wevtutil sl Microsoft-Windows-TaskScheduler/Operational /e:true`). Useful events: 100 started, 102 finished, 201 action completed with return code, 322 start ignored (already running), 111 terminated, 142 disabled.

```powershell
foreach ($t in 'pi-scheduler', 'pi-worker') {
  $i = Get-ScheduledTaskInfo -TaskPath '\PI\' -TaskName $t
  '{0}: state={1} last run={2} last result={3}' -f $t, (Get-ScheduledTask -TaskPath '\PI\' -TaskName $t).State, $i.LastRunTime, $i.LastTaskResult
}
# last result: 0 ok, 1 runtime failure, 2 configuration error, 267009 running, 267011 never run, 267014 terminated,
#   2147946720 (0x800710E0) = the watchdog's start request was refused because the task is already running (normal)
Get-WinEvent -FilterHashtable @{ LogName = 'Microsoft-Windows-TaskScheduler/Operational'; StartTime = (Get-Date).AddDays(-1) } |
  Where-Object { $_.Message -match '\\PI\\pi-' } | Select-Object TimeCreated, Id, Message
```

### Starting, stopping and restarting

Start the worker before the scheduler; stop the scheduler before the worker.

```powershell
# start (only once activation is approved; the stop file must not exist)
Remove-Item C:\PI\run\pi-worker.stop -ErrorAction SilentlyContinue
Enable-ScheduledTask -TaskPath '\PI\' -TaskName 'pi-worker';    Start-ScheduledTask -TaskPath '\PI\' -TaskName 'pi-worker'
Enable-ScheduledTask -TaskPath '\PI\' -TaskName 'pi-scheduler'; Start-ScheduledTask -TaskPath '\PI\' -TaskName 'pi-scheduler'

# graceful stop of one task: FIRST disable it (so the watchdog does not start it again), then the stop file
Disable-ScheduledTask -TaskPath '\PI\' -TaskName 'pi-worker'
New-Item -ItemType File C:\PI\run\pi-worker.stop -Force
while (Get-CimInstance Win32_Process -Filter "Name='python.exe'" | Where-Object CommandLine -match 'scripts\.pi_worker run') { Start-Sleep 5 }
Remove-Item C:\PI\run\pi-worker.stop
```

- **Scheduler:** notices the stop file within about a second and stops after the check in progress. If the file exists when it starts, it exits at once (log: `pi-scheduler not started: stop requested before start-up`, exit code 0) without taking the lock, recovering leases or running the catch-up check.
- **Worker:** stops claiming, lets running jobs finish and then exits with code 0. After `WORKER_SHUTDOWN_GRACE_SECONDS` (300 s) it logs a warning, but the process still waits for the running job; it is never cut off by the grace period.
- **A hung job:** `Stop-ScheduledTask -TaskPath '\PI\' -TaskName 'pi-worker'` kills the process. The job's lease expires after `WORKER_LEASE_SECONDS` (10 min) and the scheduler puts it back to RETRY_WAIT. Every load is transactional, so nothing is half-written.
- **Do not run `pi_scheduler run` or `pi_worker run` by hand while the tasks are enabled.** A second scheduler exits (lock); a second worker would work but write to the same log file. Use `pi_scheduler check` and `pi_worker once` for diagnostics.
- **System shutdown:** processes are ended without a graceful stop; jobs are recovered through their leases after the reboot, and the boot trigger starts both tasks again.

### Recovery behaviour

| Event | What happens |
|---|---|
| Crash or exit code 1 (e.g. scheduler lock lost) | The watchdog starts the task again within 5 minutes; the scheduler recovers expired leases and runs a catch-up check |
| Exit code 2 (configuration error: migration missing, invalid `scheduler.toml`) | Retried every 5 minutes and fails the same way until fixed; last result `2`, cause in the log |
| PostgreSQL not up at boot | The process exits; the watchdog retries every 5 minutes |
| Worker killed during a job | Lease expires (10 min), job back to RETRY_WAIT, retried |
| Stop file present while the task is enabled | Each watchdog start exits at once with code 0 (no work). Disable the task before creating a stop file |

### Enabling and disabling schedules safely

The scheduler reads `scheduler.toml` and `.env` **once at start**, but reads the database `enabled` flag and the synced configuration hash **on every tick**.

**Enable one schedule:**
1. Live probe: `python -m scripts.pi_scheduler check --schedule <KEY>` (writes nothing).
2. Set `enabled = true` for that schedule in `config/scheduler.toml` (reviewed in git).
3. `python -m scripts.pi_jobs sync-config --dry-run`, then `sync-config` (the database row stays disabled).
4. As the schema owner (no admin role was created; `PI_JOBS_ADMIN_ROLE` was empty when 0011 was applied): `python -m scripts.pi_jobs enable <KEY>`, with the owner identity supplied for that one process exactly as in [Applying migration 0011](#applying-migration-0011-as-the-schema-owner). `pi_app` gets "permission denied" by design.
5. Once: `SCHEDULER_GLOBAL_ENABLE=true` in `.env`.
6. Restart the scheduler task (graceful stop, then start): it loaded the old file and `.env` at start, and until it is restarted the edited schedule is refused because its in-memory configuration no longer matches the synced hash.
7. `python -m scripts.pi_jobs status`: `would_run` must be `true` only for the approved schedules.

**Disable one schedule:**
- **Immediately:** `pi_jobs disable <KEY>` as the owner; effective at the next tick (≤ 30 s), no restart.
- **Permanently:** also set `enabled = false` in the file, `sync-config`, restart the scheduler task.
- Jobs already queued are **not** cancelled: `pi_jobs list --status PENDING`, then `pi_jobs cancel <id>` if needed.

**Stop all automatic ingestion:** stop the scheduler task (no new jobs), then the worker task (finishes running jobs). Or set `SCHEDULER_GLOBAL_ENABLE=false` and restart the scheduler; the worker still processes jobs already queued.

### Monitoring

**Health-check task `\PI\pi-health`** (enabled 2026-11-26): every 5 minutes (and 10 minutes after boot) Task Scheduler runs `C:\PI\tools\monitor\pi_health_check.ps1` as `LOCAL SERVICE` (Windows PowerShell, 3-minute limit). It only calls `pi_jobs health`, `status` and `list`, so it reads the database as `pi_app` and writes nothing there.

| Check | Level | Condition |
|---|---|---|
| `HEARTBEAT` | ERROR | the scheduler or worker heartbeat is older than 180 s |
| `FAILED` | ERROR | a job reached FAILED since the previous run (each job is reported once) |
| `CONFIG` | WARNING | a schedule enabled in the file is out of sync with `pi_jobs.schedule` |
| `STALE` | WARNING | a schedule that should run has had no availability check for more than 7 hours |
| `QUERY` | ERROR | `pi_jobs` itself failed (database down, configuration error) |

- **Results:** one JSON line per run in `C:\PI\logs\pi-health.log` (rotated at 5 MB); the state between runs in `C:\PI\run\pi-health.state.json`; the task's last result is `0` OK, `1` WARNING, `2` ERROR.
- **Events** in the Windows **Application** log, source `PI-Scheduler`: 1001 Error / 1002 Warning when the status changes (repeated at most once an hour while it persists), 1000 Information when it recovers, 1010 Error once per newly failed job.
- **Nobody is notified actively:** there is no e-mail or chat channel. Look at Event Viewer → Windows Logs → Application (source `PI-Scheduler`), or let a monitoring agent collect those events.
- **Trial run without writing anything:** `powershell -NoProfile -ExecutionPolicy Bypass -File C:\PI\tools\monitor\pi_health_check.ps1 -DryRun` (`-MaxAgeSeconds 1` forces a heartbeat ERROR to test the alert path).
- Source and task definition: [`deploy/windows/`](../deploy/windows/README.md).

**Commands:**
- `python -m scripts.pi_jobs health --max-age 180`: exit code 0 only if a scheduler and a worker heartbeat are fresher than 180 s.
- `python -m scripts.pi_jobs status`: per schedule the configuration state and `would_run`, the last checks, active jobs and process heartbeats.
- `python -m scripts.pi_jobs list --status FAILED`, then `requeue <id>` (version-aware) after fixing the cause.
- Each process start adds one `pi_jobs.process_heartbeat` row (keyed by host and PID); old rows are harmless and are removed only by the owner (see [Retention](#retention)).

## Activation checklist

Every step needs its own approval.

1. **Migrations** by the schema owner ([procedure](#applying-migration-0011-as-the-schema-owner)). **Done 2026-11-26:** rolled-back dry run (29/29 checks), then `alembic upgrade head`; verified as `pi_app` (20/20).
2. **Integration tests** for the queue: `pytest tests/integration/test_scheduler_queue.py`, as `pi_app`. Every row is written inside one transaction that is always rolled back; one test opens and terminates its own extra session (advisory lock only, no rows). The retention test is skipped for `pi_app` (no DELETE) and runs only as the owner, also rolled back. **Done 2026-11-26:** 9 passed, 1 skipped (retention).
3. **Copy the configuration:** `pi_jobs sync-config --dry-run`, then `sync-config` (all rows start disabled in the database). **Done 2026-11-26:** 13 rows, all disabled.
4. **Tasks:** registered disabled, test-started and stopped as `LOCAL SERVICE`; folder permissions hardened. **Done 2026-11-26.**
5. **Live probes:** `pi_scheduler check --schedule <KEY>` for each candidate. These are live probes that write nothing.
6. **Per approved schedule:** follow [Enabling and disabling schedules safely](#enabling-and-disabling-schedules-safely); `pi_jobs status` must show `would_run: true` only for approved schedules.
7. **Start:** enable and start `pi-worker`, then `pi-scheduler` ([procedure](#starting-stopping-and-restarting)).
8. **Watch the first cycle:** `pi_jobs status` and `pi_jobs list`; compare job results with `pi.ingestion_run`.

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

- **Queue SQL tested on PostgreSQL inside rolled-back transactions only.** `tests/integration/test_scheduler_queue.py` passed on PostgreSQL 18.7 as `pi_app` (the retention test needs the owner). The unit tests use an in-memory repository with the same semantics, plus a recording fake connection that checks which statements share a transaction. No job has yet run end to end through a real runner from the queue.
- **Task Scheduler cannot stop gracefully or hold back a configuration error.** Graceful stops need the stop files; ending a task kills the process (jobs are recovered through their leases). Exit code 2 is retried by the watchdog every 5 minutes.
- **`LOCAL SERVICE` is a shared identity.** Other Windows services running as `LOCAL SERVICE` can also read `.env` and write into `C:\PI\logs`, `C:\PI\raw_data` and `C:\PI\run`. A dedicated account would isolate this.
- **No push notifications.** `\PI\pi-health` writes alerts to the Application event log and `pi-health.log` only; nobody is notified unless someone, or a monitoring agent, reads them.
- **No committed-row concurrency test.** Two real processes racing on committed rows (claim, enqueue) are not tested, because `pi_app` cannot delete the rows afterwards. A one-off owner-run test in a scratch schema would cover it.
- **Checks download the probe data.** CEA daily files, the IEX snapshot and NPP days are downloaded by the check and again by the job: one extra request per new item. HEAD requests were not used, because HEAD support is unverified.
- **Open-Meteo is cadence-based.** No issue time or verified model-run metadata exists, so its refresh follows the slots, not detection.
- **MOSDAC detects series, not values.** Same-series value revisions without a new series start are not detected (per kind).
- **IEX probes the market price only.** The check compares MKT prices; bid-area-only revisions are picked up whenever the job runs for that day.
- **No catch-up beyond the window.** Catch-up covers at most `catchup_max_days`; longer outages need explicit backfill commands.
- **Retention is manual.** See [Retention](#retention); no automatic clean-up exists.
