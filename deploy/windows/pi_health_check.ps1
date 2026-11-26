<#
PI scheduler health check (deployed as C:\PI\tools\monitor\pi_health_check.ps1, run every 5 minutes by the
Task Scheduler task \PI\pi-health as NT AUTHORITY\LOCAL SERVICE). Read-only towards PostgreSQL: it only calls
`pi_jobs health`, `pi_jobs status` and `pi_jobs list` (application role pi_app, settings from C:\PI\app\.env).

Checks
  HEARTBEAT  ERROR    scheduler or worker heartbeat older than 180 s (pi_jobs health)
  FAILED     ERROR    a job reached FAILED since the previous run (each failed job is reported once)
  CONFIG     WARNING  a schedule enabled in the file is out of sync with pi_jobs.schedule (scheduler refuses it)
  STALE      WARNING  a schedule that should run has had no availability check for more than 7 hours
                      (the longest configured interval is 6 hours)
  QUERY      ERROR    the health / status command itself failed (database down, configuration error)

Output
  C:\PI\logs\pi-health.log       one JSON line per run (rotated at 5 MB, one backup)
  C:\PI\run\pi-health.state.json state between runs (overall status, reported failed job ids, last alert time)
  Application event log, source PI-Scheduler (registered once by an administrator):
     1000 Information  recovered (status back to OK)
     1001 Error        status ERROR (on change, then at most once an hour while it persists)
     1002 Warning      status WARNING (on change, then at most once an hour while it persists)
     1010 Error        one event per newly FAILED job
  Exit code: 0 OK, 1 WARNING, 2 ERROR (visible as the task's last result)

-DryRun: evaluate and print only; writes no log, state or event.
-MaxAgeSeconds: heartbeat age limit (default 180). A test run with 1 forces HEARTBEAT=ERROR to prove the alert path.
#>
param([switch]$DryRun, [int]$MaxAgeSeconds = 180)
$ErrorActionPreference = 'Stop'
$App = 'C:\PI\app'; $Py = 'C:\PI\venv-svc\Scripts\python.exe'
$LogFile = 'C:\PI\logs\pi-health.log'; $StateFile = 'C:\PI\run\pi-health.state.json'
$Source = 'PI-Scheduler'; $StaleHours = 7; $ReminderMinutes = 60
$now = Get-Date

function Invoke-PiJobs([string[]]$PiArgs) {
  Push-Location $App
  $previous = $ErrorActionPreference; $ErrorActionPreference = 'Continue'   # PS 5.1: stderr lines (logging) must not throw
  try {
    $out = & $Py -m scripts.pi_jobs @PiArgs 2>$null
    $code = $LASTEXITCODE
  } finally { $ErrorActionPreference = $previous; Pop-Location }
  $json = $null
  try { $json = ($out -join "`n") | ConvertFrom-Json } catch { }
  [pscustomobject]@{ Code = $code; Json = $json }
}

$findings = New-Object System.Collections.Generic.List[object]
function Add-Finding([string]$check, [string]$level, [string]$message) {
  $findings.Add([pscustomobject]@{ check = $check; level = $level; message = $message })
}

# --- previous state
$state = [pscustomobject]@{ status = 'OK'; reported_failed = @(); last_alert = $null }
if (Test-Path $StateFile) { try { $state = Get-Content $StateFile -Raw | ConvertFrom-Json } catch { } }
$reported = @($state.reported_failed | ForEach-Object { [long]$_ })

# --- HEARTBEAT
$health = Invoke-PiJobs @('health', '--max-age', "$MaxAgeSeconds")
if ($null -eq $health.Json) { Add-Finding 'QUERY' 'ERROR' "pi_jobs health failed (exit $($health.Code))" }
elseif ($health.Code -ne 0) { Add-Finding 'HEARTBEAT' 'ERROR' ("stale heartbeat; fresh: " + (@($health.Json.fresh) -join ', ')) }

# --- CONFIG and STALE (pi_jobs status)
$status = Invoke-PiJobs @('status')
$summary = @{}
if ($null -eq $status.Json) { Add-Finding 'QUERY' 'ERROR' "pi_jobs status failed (exit $($status.Code))" }
else {
  $s = $status.Json
  $summary.jobs = $s.jobs
  foreach ($c in @($s.configuration)) {
    if ($c.file_enabled -and $c.in_sync -eq $false) { Add-Finding 'CONFIG' 'WARNING' "$($c.schedule): configuration file differs from pi_jobs.schedule (run sync-config, restart the scheduler)" }
  }
  $running = @($s.configuration | Where-Object { $_.would_run } | ForEach-Object { $_.schedule })
  $summary.active_schedules = $running
  foreach ($row in @($s.schedules | Where-Object { $_.schedule_key -in $running })) {
    if (-not $row.last_check) { Add-Finding 'STALE' 'WARNING' "$($row.schedule_key): never checked"; continue }
    $age = ($now - [datetime]$row.last_check).TotalHours
    if ($age -gt $StaleHours) { Add-Finding 'STALE' 'WARNING' ("{0}: last check {1:N1} h ago" -f $row.schedule_key, $age) }
  }
}

# --- FAILED jobs not reported before
$failed = Invoke-PiJobs @('list', '--status', 'FAILED', '--limit', '200')
$newFailed = @()
if ($null -eq $failed.Json -and $failed.Code -ne 0) { Add-Finding 'QUERY' 'ERROR' "pi_jobs list failed (exit $($failed.Code))" }
else {
  $all = @($failed.Json | Where-Object { $_ -and $_.job_id })              # '[]' -> no element
  $newFailed = @($all | Where-Object { $_.job_id -and ([long]$_.job_id -notin $reported) })
  foreach ($j in $newFailed) {
    $err = "$($j.last_error)"; if ($err.Length -gt 300) { $err = $err.Substring(0, 300) + '...' }
    Add-Finding 'FAILED' 'ERROR' "job $($j.job_id) $($j.schedule_key) $($j.item_key): $err"
  }
  $reported = @($all | ForEach-Object { [long]$_.job_id })          # keep only ids still listed (bounded)
}

# --- overall
$overall = if ($findings | Where-Object level -eq 'ERROR') { 'ERROR' } elseif ($findings.Count) { 'WARNING' } else { 'OK' }
$record = [ordered]@{ time = $now.ToString('s'); status = $overall; findings = $findings.ToArray(); summary = $summary }
$line = $record | ConvertTo-Json -Compress -Depth 6

if ($DryRun) { $line; "DRY RUN: nothing written"; exit ([array]::IndexOf(@('OK', 'WARNING', 'ERROR'), $overall)) }

# --- log line (rotate at 5 MB)
try {
  if ((Test-Path $LogFile) -and (Get-Item $LogFile).Length -gt 5MB) { Move-Item $LogFile "$LogFile.1" -Force }
  Add-Content -Path $LogFile -Value $line -Encoding UTF8
} catch { }

# --- events: on change, hourly reminder while not OK, one per new failed job
$eventError = $null
function Write-PiEvent([int]$id, [string]$type, [string]$text) {
  try { [System.Diagnostics.EventLog]::WriteEntry($Source, $text, $type, $id) }
  catch { $script:eventError = $_.Exception.Message }
}
$detail = ($findings | ForEach-Object { "[$($_.level)] $($_.check): $($_.message)" }) -join "`r`n"
$lastAlert = if ($state.last_alert) { [datetime]$state.last_alert } else { [datetime]::MinValue }
$changed = $overall -ne $state.status
if ($overall -eq 'OK' -and $changed) { Write-PiEvent 1000 'Information' "PI scheduler health recovered (previous status $($state.status))." ; $lastAlert = $now }
elseif ($overall -ne 'OK' -and ($changed -or ($now - $lastAlert).TotalMinutes -ge $ReminderMinutes)) {
  $id = if ($overall -eq 'ERROR') { 1001 } else { 1002 }; $type = if ($overall -eq 'ERROR') { 'Error' } else { 'Warning' }
  Write-PiEvent $id $type "PI scheduler health: $overall`r`n$detail`r`nSee C:\PI\logs\pi-health.log and docs\scheduler.md."
  $lastAlert = $now
}
foreach ($j in $newFailed) { Write-PiEvent 1010 'Error' "PI job $($j.job_id) FAILED: $($j.schedule_key) $($j.item_key)`r`n$($j.last_error)" }
if ($eventError) { try { Add-Content -Path $LogFile -Value (@{ time = $now.ToString('s'); event_write_error = $eventError } | ConvertTo-Json -Compress) -Encoding UTF8 } catch { } }

# --- state
try {
  @{ status = $overall; reported_failed = $reported; last_alert = $(if ($lastAlert -eq [datetime]::MinValue) { $null } else { $lastAlert.ToString('s') }) } |
    ConvertTo-Json -Compress | Set-Content -Path $StateFile -Encoding UTF8
} catch { }
exit ([array]::IndexOf(@('OK', 'WARNING', 'ERROR'), $overall))
