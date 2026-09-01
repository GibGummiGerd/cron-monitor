# cron-monitor

Wrapper für Cron-Jobs, der deren Ergebnis robust an healthchecks.io (oder
kompatible Instanzen) meldet - stdout/stderr-Logs, Retries mit Disk-Spool,
Overlap-Schutz, Timeouts. Reine Python-Stdlib, keine Abhängigkeiten.

## Features

- Success-/Fail-/Start-Pings an healthchecks.io mit Retry (3 Versuche) und
  Disk-Spool: geht ein Ping (z. B. bei Netzausfall) verloren, wird er beim
  nächsten Lauf nachgesendet - kein Signal geht verloren.
- Pings sind isoliert: Monitoring kann das Ergebnis des Jobs nie verändern.
- Per-Job-flock verhindert überlappende Läufe (`on_overlap = skip | fail |
  kill_previous`), optional Success-Ping bei Skip (`ping_on_skip`).
- Hard-Timeout killt die ganze Prozessgruppe, SIGTERM/SIGINT werden
  weitergeleitet.
- stdout+stderr pro Lauf in eine Logdatei (optional: `log_to_file = false`,
  wenn der Job selbst loggt) - der Log-Tail landet im Body des Fail-Pings.

## Installation

```bash
uv tool install git+https://github.com/<user>/cron-monitor.git
# oder lokal:
uv tool install /path/to/cron-monitor
```

## Konfiguration (TOML)

Siehe `cron_monitor.example.toml`. Gesucht wird die Config via `-c`, dann
`$CRON_MONITOR_CONFIG`, `/etc/cron_monitor.toml`, `./cron_monitor.toml`.

```toml
[defaults]
ping_base = "https://hc.example/ping"
spool_dir = "/var/spool/cron_monitor"
log_dir   = "/var/log/cron_monitor"
lock_dir  = "/run/lock/cron_monitor"
timeout   = 3600
on_overlap = "skip"
log_to_file = true

[jobs.my-job]
uuid = "<check-uuid von healthchecks.io>"
command = ["/usr/bin/python3", "/srv/project/collect.py"]
cwd = "/srv/project"
timeout = 1800
```

## Usage

Crontab (eine Zeile, kein `&&`/`||` nötig):

```
*/30 * * * * ~/.local/bin/cron-monitor -c /srv/project/cron_monitor.toml run my-job
```

Weitere Subcommands: `list` (Jobs anzeigen), `flush` (gespoolte Pings
nachsenden), `test <job>` (Start+Success-Testpings senden).

Exit-Codes: der Exit-Code des Jobs wird durchgereicht; 2 = Config-/Usage-Fehler,
3 = Job wegen Overlap übersprungen.

## Entwicklung

```bash
python -m unittest discover tests -v   # oder: pytest
```
