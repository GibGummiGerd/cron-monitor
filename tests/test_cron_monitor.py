#!/usr/bin/env python3
"""Unit tests for cron_monitor.py. Run: python -m unittest test_cron_monitor -v"""

import io
import json
import sys
import tempfile
import time
import unittest
import urllib.error
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
import cron_monitor  # noqa: E402


def make_config(tmp: Path, job: dict | None = None) -> Path:
    job = job or {}
    cmd = job.get("command", [sys.executable, "-c", "pass"])
    cmd_toml = "[" + ", ".join(json.dumps(c) for c in cmd) + "]"
    extra = "\n".join(f"{k} = {json.dumps(v)}" for k, v in job.items()
                      if k != "command")
    cfg = tmp / "cron_monitor.toml"
    cfg.write_text(f"""
[defaults]
ping_base = "https://hc.example/ping"
timeout = 5
[jobs.demo]
uuid = "11111111-2222-3333-4444-555555555555"
command = {cmd_toml}
{extra}
""", encoding="utf-8")
    return cfg


def fake_urlopen_factory(status=200, fail_times=0, always_fail=False):
    """Build a urlopen replacement recording (url, body) calls."""
    calls = []
    state = {"n": 0}

    class Resp:
        def __init__(self):
            self.status = status

        def read(self):
            return b""

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def fake(req, timeout=None):
        calls.append((req.full_url, req.data or b""))
        state["n"] += 1
        if always_fail or state["n"] <= fail_times:
            raise urllib.error.URLError("boom")
        return Resp()

    return fake, calls


class CronMonitorTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())

    def run_cli(self, cfg, *args, **urlopen_kwargs):
        fake, calls = fake_urlopen_factory(**urlopen_kwargs)
        with mock.patch("urllib.request.urlopen", fake), \
             mock.patch("time.sleep", lambda s: None):
            rc = cron_monitor.main(["-c", str(cfg), *args])
        return rc, calls

    def test_success_pings_and_exit_code(self):
        cfg = make_config(self.tmp)
        rc, calls = self.run_cli(cfg, "run", "demo")
        self.assertEqual(rc, 0)
        urls = [u for u, _ in calls]
        self.assertEqual(urls, [
            "https://hc.example/ping/11111111-2222-3333-4444-555555555555/start",
            "https://hc.example/ping/11111111-2222-3333-4444-555555555555",
        ])
        self.assertIn(b"runtime=", calls[1][1])

    def test_failure_sends_log_tail_and_exit_code(self):
        cfg = make_config(self.tmp, {"command": [
            sys.executable, "-c",
            "import sys; sys.stderr.write('boom-traceback'); sys.exit(7)"]})
        rc, calls = self.run_cli(cfg, "run", "demo")
        self.assertEqual(rc, 7)  # job exit code is re-issued
        self.assertTrue(calls[-1][0].endswith("/fail"))
        self.assertIn(b"boom-traceback", calls[-1][1])
        self.assertIn(b"exit=7", calls[-1][1])

    def test_unreachable_healthchecks_spools_and_flushes_next_run(self):
        cfg = make_config(self.tmp)
        # 1st run: healthchecks down -> pings spooled, job still succeeds
        rc, _ = self.run_cli(cfg, "run", "demo", always_fail=True)
        self.assertEqual(rc, 0)
        spool = self.tmp / "spool"
        self.assertEqual(len(list(spool.glob("*.json"))), 2)
        # 2nd run: healthchecks back -> spool flushed first, then new pings
        rc, calls = self.run_cli(cfg, "run", "demo")
        self.assertEqual(rc, 0)
        self.assertEqual(len(list(spool.glob("*.json"))), 0)
        urls = [u for u, _ in calls]
        base = "https://hc.example/ping/11111111-2222-3333-4444-555555555555"
        # spooled pings are flushed first (start, success), then this run's
        self.assertEqual(urls, [base + "/start", base,
                                base + "/start", base])

    def test_flush_subcommand(self):
        cfg = make_config(self.tmp)
        cron_monitor.spool_ping(self.tmp / "spool",
                           "https://hc.example/ping/x/fail", b"late")
        rc, calls = self.run_cli(cfg, "flush")
        self.assertEqual(rc, 0)
        self.assertEqual(calls[0][0], "https://hc.example/ping/x/fail")

    def test_timeout_kills_job(self):
        cfg = make_config(self.tmp, {
            "command": [sys.executable, "-c", "import time; time.sleep(60)"],
            "timeout": 1})
        start = time.monotonic()
        rc, calls = self.run_cli(cfg, "run", "demo")
        self.assertLess(time.monotonic() - start, 30)
        self.assertNotEqual(rc, 0)
        self.assertTrue(calls[-1][0].endswith("/fail"))
        self.assertIn(b"KILLED after timeout", calls[-1][1])

    def test_unknown_job_and_list(self):
        cfg = make_config(self.tmp)
        rc, _ = self.run_cli(cfg, "run", "nope")
        self.assertEqual(rc, 2)
        buf = io.StringIO()
        with mock.patch("sys.stdout", buf):
            rc, _ = self.run_cli(cfg, "list")
        self.assertEqual(rc, 0)
        self.assertIn("demo", buf.getvalue())

    def test_skip_pings_success_with_ping_on_skip(self):
        # A run that overlaps a busy previous run and is skipped must
        # still send a success ping when ping_on_skip is enabled.
        cfg = make_config(self.tmp, {
            "command": [sys.executable, "-c", "pass"],
            "ping_on_skip": True})

        # First run: grab the lock with a raw JobLock and hold it open.
        first_lock = cron_monitor.JobLock(self.tmp / "locks" / "demo.lock")
        self.assertTrue(first_lock.acquire("skip"))

        rc, calls = self.run_cli(cfg, "run", "demo")
        self.assertEqual(rc, 3)  # skipped
        self.assertEqual(calls, [
            ("https://hc.example/ping/11111111-2222-3333-4444-555555555555",
             b"cron_monitor: job 'demo' already running (on_overlap=skip)"),
        ])
        first_lock.release()

    def test_skip_does_not_ping_by_default(self):
        cfg = make_config(self.tmp)
        first_lock = cron_monitor.JobLock(self.tmp / "locks" / "demo.lock")
        self.assertTrue(first_lock.acquire("skip"))
        rc, calls = self.run_cli(cfg, "run", "demo")
        self.assertEqual(rc, 3)
        self.assertEqual(calls, [])
        first_lock.release()

    def test_body_limit_tail(self):
        big = "x" * (cron_monitor.BODY_LIMIT * 3)
        cfg = make_config(self.tmp, {"command": [
            sys.executable, "-c",
            f"import sys; sys.stderr.write({big!r}); sys.exit(1)"]})
        rc, calls = self.run_cli(cfg, "run", "demo")
        self.assertEqual(rc, 1)
        self.assertLessEqual(len(calls[-1][1]), cron_monitor.BODY_LIMIT)

    def test_log_to_file_false_writes_no_log(self):
        # log_to_file = false: no per-run log file is created, the success
        # ping metadata has no log= field, output passes through.
        cfg = make_config(self.tmp, {
            "command": [sys.executable, "-c", "print('hi'); exit(0)"],
            "log_to_file": False})
        rc, calls = self.run_cli(cfg, "run", "demo")
        self.assertEqual(rc, 0)
        success = [c for c in calls if not c[0].endswith("/start")][-1]
        self.assertNotIn(b"log=", success[1])
        self.assertFalse((self.tmp / "logs").exists())

    def test_log_to_file_false_fail_has_no_tail(self):
        # A failing run with log_to_file = false must not crash and must
        # send a /fail ping without a log tail section.
        cfg = make_config(self.tmp, {
            "command": [sys.executable, "-c",
                        "import sys; sys.stderr.write('boom'); sys.exit(3)"],
            "log_to_file": False})
        rc, calls = self.run_cli(cfg, "run", "demo")
        self.assertEqual(rc, 3)
        fail = [c for c in calls if c[0].endswith("/fail")][-1]
        self.assertIn(b"exit=3", fail[1])
        self.assertNotIn(b"--- log tail ---", fail[1])
        self.assertFalse((self.tmp / "logs").exists())


if __name__ == "__main__":
    unittest.main()
