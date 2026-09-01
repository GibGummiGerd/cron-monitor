# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/) and the project adheres to
[Semantic Versioning](https://semver.org/).

## [0.1.0] - 2026-09-01

### Added
- Initial release: `cron-monitor` CLI for running cron jobs and reporting
  their outcome to healthchecks.io.
- Per-job flock-based overlap protection with `on_overlap` policies
  (`skip`, `fail`, `kill_previous`) and optional `ping_on_skip`.
- Per-run log files, configurable timeouts, and offline ping spooling
  with automatic replay on the next run.
