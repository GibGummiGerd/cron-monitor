"""cron-monitor: robust cron job wrapper with healthchecks.io reporting."""

from cron_monitor.cli import *  # noqa: F401,F403
from cron_monitor.cli import main, main_entry  # noqa: F401

__version__ = "0.1.0"
__all__ = ["main", "main_entry"]
