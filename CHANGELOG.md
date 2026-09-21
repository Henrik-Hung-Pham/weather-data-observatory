# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Project governance: `SECURITY.md`, `CONTRIBUTING.md`, and a `CODEOWNERS` file.

### Fixed
- `plotly` was declared in `requirements.txt` but missing from
  `requirements.lock`, so the dashboard image installed everything except
  the library `dashboard/app.py` imports at module scope. Added, with a test
  that fails when a declared dependency is not pinned in the lock.

### Security
- The RDS master password is generated and rotated by AWS in Secrets
  Manager (`manage_master_user_password`) instead of being passed in as a
  Terraform variable, where it was written to the state file in plaintext.
- Bumped the locked `gitpython` to 3.1.60 (PYSEC-2026-3982/3983/3984) and
  `anyio` to 4.14.2 (CVE-2026-63374/63349/64847).

### Changed
- Terraform: remote S3 state with native locking, the serving layer moved
  into a caller-supplied VPC behind its own security group, deletion
  protection and final snapshots on by default, and lifecycle rules for the
  data lake and both ECR repositories.
- Removed the obsolete `version:` key from `docker-compose.yml` (ignored by
  Compose v2).

<!--
Other hardening work is tracked in separate PRs (quality-gate consolidation,
orchestrator test coverage, supply-chain scanning, non-root container, CI
tightening, batched serving-layer upsert, single per-run partition timestamp).
Move entries here as they merge.
-->

## [1.0.0] - 2026-02-02

### Added
- Medallion (Bronze → Silver → Gold) ETL pipeline with shift-left quality gates.
- OpenWeather ingestion with retry/backoff; S3/LocalStack data lake with
  Hive-style date partitioning; PostgreSQL serving layer.
- Great Expectations validation suites per layer.
- Streamlit monitoring dashboard (weather, quality trends, anomaly detection,
  pipeline status).
- Self-healing record quarantine, structured JSON logging, optional Slack
  alerting, and optional Dagster orchestration.
- Docker/Docker Compose, GitHub Actions CI/CD, and Terraform AWS infra.

[Unreleased]: https://github.com/Henrik-Hung-Pham/weather-data-observatory/compare/main...HEAD
[1.0.0]: https://github.com/Henrik-Hung-Pham/weather-data-observatory/releases/tag/v1.0.0
