# Windows Task Scheduler deployment files

The files used on the Windows Server 2019 host (installed 2026-11-26). Full description, operating
procedures and recovery: [`docs/scheduler.md`](../../docs/scheduler.md#windows-server-2019-deployment-task-scheduler).

| File | Installed as | Purpose |
|---|---|---|
| `task_pi-scheduler.xml` | task `\PI\pi-scheduler` (definition kept in `C:\PI\tools\tasks\`) | `pi_scheduler run`: boot + 2 min, 5-minute watchdog, `LOCAL SERVICE` |
| `task_pi-worker.xml` | task `\PI\pi-worker` | `pi_worker run`: same triggers and account |
| `task_pi-health.xml` | task `\PI\pi-health` | health check every 5 minutes (boot + 10 min), `LOCAL SERVICE`, 3-minute limit |
| `pi_health_check.ps1` | `C:\PI\tools\monitor\pi_health_check.ps1` | the health check run by `\PI\pi-health` |

All three XML files register the task **disabled** (`<Enabled>false</Enabled>`); enabling is a separate step.

```powershell
# elevated PowerShell, from C:\PI\app
foreach ($t in 'pi-scheduler', 'pi-worker', 'pi-health') {
  Copy-Item "deploy\windows\task_$t.xml" "C:\PI\tools\tasks\task_$t.xml" -Force
  Register-ScheduledTask -TaskPath '\PI\' -TaskName $t -Xml (Get-Content "C:\PI\tools\tasks\task_$t.xml" -Raw) `
                         -User 'NT AUTHORITY\LOCALSERVICE' | Out-Null
  Disable-ScheduledTask -TaskPath '\PI\' -TaskName $t | Out-Null
}
# health check: script and event source (once)
New-Item -ItemType Directory -Force C:\PI\tools\monitor | Out-Null
Copy-Item deploy\windows\pi_health_check.ps1 C:\PI\tools\monitor\ -Force
if (-not [System.Diagnostics.EventLog]::SourceExists('PI-Scheduler')) { New-EventLog -LogName Application -Source PI-Scheduler }
# a read-only trial run that writes nothing:
powershell -NoProfile -ExecutionPolicy Bypass -File C:\PI\tools\monitor\pi_health_check.ps1 -DryRun
```

After changing a file here, copy it to its installed location again (and re-register the task for an XML change).
