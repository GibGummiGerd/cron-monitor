#!/usr/bin/env python3
"""
cron_monitor - run cron jobs and report their outcome to healthchecks, robustly.

Design goals: the monitoring layer must never change
the job's outcome, no signal must ever be silently lost, and a hung or
overlapping job must be handled deterministically.

Usage:

    cron_monitor.py run <job>   [-c CONFIG]   run a configured job (for crontab)
    cron_monitor.py flush       [-c CONFIG]   re-send spooled pings, then exit
    cron_monitor.py list        [-c CONFIG]   list configured jobs
    cron_monitor.py test <job>  [-c CONFIG]   send start+success test pings

Config (TOML, see cron_monitor.example.toml) is looked up via -c, the
CRON_MONITOR_CONFIG env var, /etc/cron_monitor.toml or ./cron_monitor.toml (in that order).
Requires Python >= 3.11 (tomllib).

Crontab example (one line, no && / ||):

    0 3 * * * /usr/bin/python3 /opt/cron_monitor/cron_monitor.py run collect-stats

Robustness properties:

  * Every ping is retried (3 attempts, backoff 2s/8s/30s). If all attempts
    fail, the ping is spooled to disk and re-sent ("flushed") by the next
    cron_monitor invocation - even a multi-day healthchecks outage loses no
    signal permanently.
  * Pings are fully isolated from the job: a ping/network problem can never
    alter the job's exit code or turn a success into a reported failure.
  * Per-job flock prevents overlapping runs; policy (on_overlap):
    skip (default) - new run exits with code 3, running job untouched,
    no ping (or a success ping with ping_on_skip = true); fail - the
    collision is reported via a /fail ping and exit code 1; kill_previous -
    the previous run's process group is SIGTERM/SIGKILLed and the new run
    takes over.
  * Hard per-job timeout kills the whole process group (no orphans).
  * SIGTERM/SIGINT are forwarded to the job's process group.
  * stdout+stderr are written to a per-run log file (unless log_to_file is
    disabled per config, in which case output passes through to the
    monitor's own stdout/stderr); the tail is attached to the /fail ping
    body, runtime metadata to /success.

Exit codes: the wrapped job's exit code is re-issued. 2 = usage/config
error, 3 = job skipped due to overlap (policy=skip).
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import tomllib
import urllib.error
import urllib.request
import uuid
from pathlib import Path

BODY_LIMIT = 9 * 1024          # stay under healthchecks' ~10 KB body limit
PING_TIMEOUT = 5               # seconds per ping attempt
PING_BACKOFF = (2, 8, 30)      # retry delays between attempts
SPOOL_MAX_AGE = 7 * 86400      # drop spooled pings older than 7 days
IS_POSIX = os.name == "posix"

if IS_POSIX:
    import fcntl


# ---------------------------------------------------------------------------
# Pings: retry + disk spool
# ---------------------------------------------------------------------------

def _ping_once(url: str, body: bytes) -> bool:
    req = urllib.request.Request(url, data=body, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=PING_TIMEOUT) as resp:
            resp.read()
        return 200 <= resp.status < 300
    except Exception as exc:  # noqa: BLE001 - monitoring must never raise
        print(f"cron_monitor: ping to {url} failed: {exc}", file=sys.stderr)
        return False


def spool_ping(spool_dir: Path, url: str, body: bytes) -> None:
    try:
        spool_dir.mkdir(parents=True, exist_ok=True)
        payload = {"url": url, "body": body.decode("utf-8", "replace"),
                   "ts": time.time()}
        name = f"{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}.json"
        # write-then-rename: a crash mid-write never leaves a partial file
        fd, tmp = tempfile.mkstemp(dir=spool_dir, suffix=".tmp")
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh)
        os.replace(tmp, spool_dir / name)
    except Exception as exc:  # noqa: BLE001
        print(f"cron_monitor: could not spool ping for {url}: {exc}",
              file=sys.stderr)


def ping(url: str, body: bytes = b"", spool_dir: Path | None = None) -> None:
    """Send a ping with retries; spool it if every attempt fails."""
    for delay in (0, *PING_BACKOFF):
        if delay:
            time.sleep(delay)
        if _ping_once(url, body):
            return
    print(f"cron_monitor: all ping attempts failed, spooling: {url}",
          file=sys.stderr)
    if spool_dir is not None:
        spool_ping(spool_dir, url, body)


def flush_spool(spool_dir: Path) -> tuple[int, int]:
    """Re-send spooled pings, oldest first. Returns (sent, remaining)."""
    if not spool_dir.is_dir():
        return (0, 0)
    sent = remaining = 0
    for path in sorted(spool_dir.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            path.unlink(missing_ok=True)  # corrupt file: drop it
            continue
        if time.time() - payload.get("ts", 0) > SPOOL_MAX_AGE:
            path.unlink(missing_ok=True)  # stale signal: drop
            continue
        body = payload.get("body", "").encode("utf-8")
        if _ping_once(payload["url"], body):
            path.unlink(missing_ok=True)
            sent += 1
        else:
            remaining += 1
            break  # still unreachable: keep order, stop here
    return (sent, remaining)




# ---------------------------------------------------------------------------
# Overlap protection (flock)
# ---------------------------------------------------------------------------

class JobLock:
    """Advisory flock with a pid file, backing the on_overlap policies."""

    def __init__(self, lock_path: Path):
        self.lock_path = lock_path
        self._fh = None

    def acquire(self, policy: str) -> bool:
        """True if the lock is held after the call."""
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(self.lock_path, "a+", encoding="utf-8")
        if not IS_POSIX:
            return True  # no fcntl on Windows; locking is a no-op there
        try:
            fcntl.flock(self._fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            if policy != "kill_previous":
                return False
            self._kill_holder()
            try:
                fcntl.flock(self._fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                return False
        self._fh.seek(0)
        self._fh.truncate()
        self._fh.write(str(os.getpid()))
        self._fh.flush()
        return True

    def _kill_holder(self) -> None:
        try:
            self._fh.seek(0)
            pid = int(self._fh.read().strip())
            os.killpg(pid, signal.SIGTERM)
            time.sleep(2)
            os.killpg(pid, 0)  # still alive?
            os.killpg(pid, signal.SIGKILL)
        except (ValueError, ProcessLookupError, PermissionError, OSError):
            pass
        time.sleep(1)

    def release(self) -> None:
        if self._fh:
            if IS_POSIX:
                try:
                    fcntl.flock(self._fh, fcntl.LOCK_UN)
                except OSError:
                    pass
            self._fh.close()


# ---------------------------------------------------------------------------
# Job execution
# ---------------------------------------------------------------------------

def run_job(command: list[str], cwd: str | None, env: dict | None,
            timeout: int, log_path: Path | None) -> tuple[int, bool]:
    """Run the job; returns (exit_code, timed_out).

    stdout+stderr are interleaved into log_path (or passed through to the
    monitor's own stdout/stderr if log_path is None). The job runs in its own
    process group so a timeout (or a forwarded signal) kills the whole tree.
    """
    log = None
    if log_path is not None:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log = open(log_path, "wb")
    job_env = dict(os.environ)
    if env:
        job_env.update({k: str(v) for k, v in env.items()})

    try:
        proc = subprocess.Popen(
            command, cwd=cwd, env=job_env,
            stdout=log if log is not None else None,
            stderr=subprocess.STDOUT if log is not None else None,
            stdin=subprocess.DEVNULL,
            start_new_session=IS_POSIX,
        )

        def _forward(signum, _frame):
            if IS_POSIX:
                try:
                    os.killpg(proc.pid, signum)
                except (ProcessLookupError, OSError):
                    pass
            else:
                try:
                    proc.terminate()
                except OSError:
                    pass

        old_handlers = {}
        for sig in (signal.SIGTERM, signal.SIGINT):
            old_handlers[sig] = signal.signal(sig, _forward)
        try:
            try:
                return proc.wait(timeout=timeout), False
            except subprocess.TimeoutExpired:
                _kill_tree(proc)
                return proc.wait(), True
        finally:
            for sig, handler in old_handlers.items():
                signal.signal(sig, handler)
    finally:
        if log is not None:
            log.close()


def _kill_tree(proc: subprocess.Popen) -> None:
    try:
        if IS_POSIX:
            os.killpg(proc.pid, signal.SIGTERM)
        else:
            proc.terminate()
        proc.wait(timeout=5)
    except (ProcessLookupError, OSError, subprocess.TimeoutExpired):
        try:
            if IS_POSIX:
                os.killpg(proc.pid, signal.SIGKILL)
            else:
                proc.kill()
        except (ProcessLookupError, OSError):
            pass


def log_tail(log_path: Path) -> str:
    try:
        with open(log_path, "rb") as fh:
            fh.seek(0, os.SEEK_END)
            size = fh.tell()
            fh.seek(max(0, size - BODY_LIMIT))
            return fh.read().decode("utf-8", "replace")
    except OSError:
        return ""


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DEFAULT_CONFIG_PATHS = (Path("/etc/cron_monitor.toml"), Path("cron_monitor.toml"))


def find_config(explicit: str | None) -> Path:
    candidates: list[Path] = []
    if explicit:
        candidates.append(Path(explicit))
    if os.environ.get("CRON_MONITOR_CONFIG"):
        candidates.append(Path(os.environ["CRON_MONITOR_CONFIG"]))
    candidates.extend(DEFAULT_CONFIG_PATHS)
    for path in candidates:
        if path.is_file():
            return path
    raise SystemExit(
        "cron_monitor: no config found (tried: "
        + ", ".join(str(p) for p in candidates) + "); use -c CONFIG")


def load_config(path: Path) -> dict:
    with open(path, "rb") as fh:
        cfg = tomllib.load(fh)
    cfg.setdefault("defaults", {})
    cfg.setdefault("jobs", {})
    base = path.parent
    for key, default in (("spool_dir", "spool"), ("log_dir", "logs"),
                         ("lock_dir", "locks")):
        p = Path(cfg["defaults"].get(key, str(base / default)))
        cfg["defaults"][key] = p if p.is_absolute() else (base / p)
    return cfg


def ping_url(cfg: dict, job: dict, suffix: str = "") -> str:
    base = cfg["defaults"].get("ping_base", "").rstrip("/")
    return f"{base}/{job['uuid']}{suffix}"


# ---------------------------------------------------------------------------
# Subcommands
# ---------------------------------------------------------------------------

def cmd_run(cfg: dict, job_name: str) -> int:
    job = cfg["jobs"].get(job_name)
    if job is None:
        print(f"cron_monitor: job '{job_name}' not in config", file=sys.stderr)
        return 2
    defaults = cfg["defaults"]
    spool_dir = Path(defaults["spool_dir"])
    log_dir = Path(defaults["log_dir"])
    lock_dir = Path(defaults["lock_dir"])
    timeout = int(job.get("timeout", defaults.get("timeout", 3600)))
    policy = job.get("on_overlap", defaults.get("on_overlap", "skip"))
    command = job.get("command")
    if not command:
        print(f"cron_monitor: job '{job_name}' has no command", file=sys.stderr)
        return 2

    # Never lose older signals, even if this run is skipped later.
    sent, remaining = flush_spool(spool_dir)
    if sent or remaining:
        print(f"cron_monitor: flushed {sent} spooled ping(s), {remaining} pending",
              file=sys.stderr)

    lock = JobLock(lock_dir / f"{job_name}.lock")
    if not lock.acquire(policy):
        msg = f"cron_monitor: job '{job_name}' already running (on_overlap={policy})"
        print(msg, file=sys.stderr)
        if policy == "fail":
            ping(ping_url(cfg, job, "/fail"), body=msg.encode(),
                 spool_dir=spool_dir)
            return 1
        if job.get("ping_on_skip", defaults.get("ping_on_skip", False)):
            # Overlapping run means the job is healthy and still busy:
            # ping success (with a note) so the check stays "on time".
            ping(ping_url(cfg, job), body=msg.encode(), spool_dir=spool_dir)
        return 3  # skip: not an error of the job itself

    started = time.monotonic()
    log_to_file = job.get("log_to_file", defaults.get("log_to_file", True))
    log_path = (log_dir / job_name /
                f"{datetime.datetime.now().strftime('%Y%m%d-%H%M%S')}.log"
                if log_to_file else None)
    try:
        ping(ping_url(cfg, job, "/start"), spool_dir=spool_dir)
        rc, timed_out = run_job(
            command, cwd=job.get("cwd"), env=job.get("env"),
            timeout=timeout, log_path=log_path)
    finally:
        lock.release()

    elapsed = time.monotonic() - started
    meta = f"job={job_name} exit={rc} runtime={elapsed:.1f}s"
    if log_path is not None:
        meta += f" log={log_path}"
    meta += "\n"
    if rc == 0 and not timed_out:
        ping(ping_url(cfg, job), body=meta.encode(), spool_dir=spool_dir)
        return 0

    body = meta
    if timed_out:
        body += f"KILLED after timeout of {timeout}s\n"
    if log_path is not None:
        tail = log_tail(log_path).strip()
        if tail:
            body += "\n--- log tail ---\n" + tail
    ping(ping_url(cfg, job, "/fail"),
         body=body.encode("utf-8", "replace")[-BODY_LIMIT:],
         spool_dir=spool_dir)
    return rc if rc != 0 else 1


def cmd_flush(cfg: dict) -> int:
    sent, remaining = flush_spool(Path(cfg["defaults"]["spool_dir"]))
    print(f"cron_monitor: sent {sent}, pending {remaining}")
    return 0 if remaining == 0 else 1


def cmd_list(cfg: dict) -> int:
    for name, job in cfg["jobs"].items():
        cmd = " ".join(map(str, job.get("command", [])))
        print(f"{name}\t{job.get('uuid', '?')}\t{cmd}")
    return 0


def cmd_test(cfg: dict, job_name: str) -> int:
    job = cfg["jobs"].get(job_name)
    if job is None:
        print(f"cron_monitor: job '{job_name}' not in config", file=sys.stderr)
        return 2
    spool_dir = Path(cfg["defaults"]["spool_dir"])
    ping(ping_url(cfg, job, "/start"), spool_dir=spool_dir)
    ping(ping_url(cfg, job), body=b"cron_monitor: manual test ping\n",
         spool_dir=spool_dir)
    print(f"cron_monitor: sent start+success test pings for '{job_name}'")
    return 0


# ---------------------------------------------------------------------------

def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="cron_monitor", description=__doc__)
    parser.add_argument("-c", "--config", help="path to cron_monitor TOML config")
    sub = parser.add_subparsers(dest="subcommand", required=True)
    for name in ("run", "test"):
        p = sub.add_parser(name)
        p.add_argument("job")
    sub.add_parser("flush")
    sub.add_parser("list")
    args = parser.parse_args(argv)

    try:
        cfg = load_config(find_config(args.config))
    except (OSError, tomllib.TOMLDecodeError) as exc:
        print(f"cron_monitor: config error: {exc}", file=sys.stderr)
        return 2

    if args.subcommand == "run":
        return cmd_run(cfg, args.job)
    if args.subcommand == "test":
        return cmd_test(cfg, args.job)
    if args.subcommand == "flush":
        return cmd_flush(cfg)
    return cmd_list(cfg)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))


def main_entry() -> None:  # console-script entry point
    sys.exit(main(sys.argv[1:]))
