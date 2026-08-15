# 🔭 Data Observatory

## Self-Healing Data Quality Platform

[![CI Pipeline](https://github.com/Henrik-Hung-Pham/weather-data-observatory/actions/workflows/ci.yml/badge.svg)](https://github.com/Henrik-Hung-Pham/weather-data-observatory/actions/workflows/ci.yml)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

An end-to-end **Data Quality Platform** demonstrating production-ready data pipelines with automated validation, quality gates, CI/CD integration, and real-time monitoring. Built with a **Quality-First** approach.

---

## 🏗️ Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Weather   │────▶│   Bronze    │────▶│   Silver    │────▶│    Gold     │
│     API     │     │   (Raw)     │     │  (Cleaned)  │     │ (Aggregated)│
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
                           │                   │                   │
                           ▼                   ▼                   ▼
                    ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
                    │   Quality   │     │   Quality   │     │   Quality   │
                    │    Gate     │     │    Gate     │     │    Gate     │
                    └─────────────┘     └─────────────┘     └─────────────┘
                                               │
                                               ▼
                                        ┌─────────────┐
                                        │  Dashboard  │
                                        │  (Streamlit)│
                                        └─────────────┘
```

### Medallion Architecture

| Layer | Purpose | Storage | Validation |
|-------|---------|---------|------------|
| 🥉 **Bronze** | Raw data exactly as received | S3/LocalStack | Schema existence, non-null keys |
| 🥈 **Silver** | Cleaned, normalized, standardized | S3/LocalStack | Data types, ranges, completeness |
| 🥇 **Gold** | Aggregated, business-ready | PostgreSQL | Uniqueness, referential integrity |

Data-lake objects are written with **Hive-style date partitioning**
(`…/year=2024/month=01/day=15/…`) so query engines (Athena/Glue/Spark) can
prune partitions. Set `PARTITION_STYLE=plain` for bare `YYYY/MM/DD/` instead.

---

## ✨ Key Features

### 🛡️ Quality Gates (Shift-Left Approach)
- **Automatic pipeline blocking** when data quality issues are detected
- **Schema drift detection** - pipeline stops if schema changes unexpectedly
- **Configurable severity** - `warn` mode logs issues, `block` mode stops the pipeline
- **Dependency-free quality rules** - schema-drift, null, range and uniqueness
  checks live in [`data_pipeline/quality/gates.py`](data_pipeline/quality/gates.py),
  wired per layer by a single `build_gate_for_layer` factory (no rule drift).
  A `freshness_rule` is also implemented there but is **not currently wired to
  any layer** — see the rules table below

### 🔁 Self-Healing
- **Record-level quarantine** - records that fail Silver cleaning/validation are
  routed to a `quarantine` (dead-letter) prefix instead of being dropped, so the
  run continues with the valid subset and rejected data stays auditable

### 📊 Monitoring Dashboard
- **Real-time weather visualization**
- **Data quality metrics** (% passing validation)
- **Quality pass-rate trend** (per-layer, last 14 days)
- **Anomaly detection** — temperature outliers via robust modified z-score (MAD)
- **Pipeline health status**
- **Historical trend analysis**

### 🔄 Modern Data Engineering
- **Medallion Architecture** (Bronze/Silver/Gold)
- **ELT pattern** with SQL and Python transformations
- **Containerized** with Docker and Docker Compose — multi-stage build that runs
  as a **non-root user** with a minimal `.dockerignore` build context
- **CI** with GitHub Actions — lint, type check, unit tests, integration tests
  against real Postgres/LocalStack services, a Docker build, and a quality-gate
  configuration check. The `deploy.yml` workflow builds and pushes images to
  GHCR and validates the Terraform; its staging/production steps are
  placeholders, not a real deployment

---

## 🚀 Quick Start

### Prerequisites

- Python 3.10 or newer (CI runs 3.11; `.python-version` pins 3.11 locally)
- Docker & Docker Compose
- OpenWeather API key ([get free key](https://openweathermap.org/api))

### Option 1: Run with Docker (Recommended)

```bash
# Clone the repository
git clone https://github.com/Henrik-Hung-Pham/weather-data-observatory.git
cd weather-data-observatory

# Create environment file
cp .env.example .env
# Edit .env and add your OPENWEATHER_API_KEY

# Start all services
docker-compose up --build

# Access the dashboard
open http://localhost:8501
```

### Option 2: Local Development

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows

# Install dependencies
pip install -r requirements.txt

# Set up environment
cp .env.example .env
# Edit .env and add your OPENWEATHER_API_KEY

# Run the pipeline manually
python -m data_pipeline.pipeline

# Run the dashboard
streamlit run dashboard/app.py
```

---

## 📁 Project Structure

```
data-observatory/
├── data_pipeline/           # Python ETL logic
│   ├── ingestion/           # Bronze layer - API connectors
│   ├── transformation/      # Silver/Gold layer transformations
│   ├── quality/             # Quality gates (custom, dependency-free)
│   ├── storage/             # S3 & PostgreSQL abstractions
│   ├── orchestration/       # Dagster assets/job/schedule (optional)
│   ├── schema.py            # Canonical schema (single source of truth)
│   └── pipeline.py          # Main orchestrator
├── tests/                   # Comprehensive test suite
│   ├── unit/                # Unit tests
│   ├── integration/         # Integration tests
│   └── conftest.py          # Pytest fixtures
├── dashboard/               # Streamlit monitoring UI
├── sql/                     # Database schema
├── infra/                   # Infrastructure as Code
│   ├── terraform/           # AWS deployment
│   └── localstack/          # Local development
├── .github/workflows/       # CI/CD pipelines
├── docker-compose.yml       # Local development stack
├── Dockerfile               # Multi-stage production build
└── README.md                # You are here!
```

---

## 🧪 Testing

```bash
# Run all tests
pytest

# Run unit tests only
pytest tests/unit/ -v

# Run with coverage
pytest --cov=data_pipeline --cov-report=html

# Run integration tests (requires Docker services)
docker-compose up -d postgres localstack
pytest tests/integration/ -v
```

---

## 🔍 Quality Gate Configuration

Quality gates can be configured in two modes:

### Block Mode (Default)
```env
QUALITY_GATE_MODE=block
```
Pipeline stops immediately when quality issues are detected.

### Warn Mode
```env
QUALITY_GATE_MODE=warn
```
Pipeline logs warnings but continues processing.

### Built-in Quality Rules

| Rule | Description | Severity | Wired to |
|------|-------------|----------|----------|
| `schema_drift_rule` | Detects missing or extra columns | 🔴 Critical (missing) / 🟡 Warning (extra) | Bronze, Silver |
| `null_check_rule` | Checks for null values in required columns | 🟡 Warning* | Bronze, Silver, Gold |
| `range_check_rule` | Validates values are within expected ranges | 🟡 Warning | Silver |
| `unique_check_rule` | Detects duplicate (compound) keys in the serving layer | 🔴 Critical | Gold |
| `freshness_rule` | Checks data is not stale | 🟡 Warning | **Not wired to any layer** |

*Null check becomes critical if >10% of records affected

Note that in the default `block` mode a 🟡 Warning blocks the pipeline too —
only `warn` mode lets warnings through. `freshness_rule` is implemented and
tested but `build_gate_for_layer` does not add it to any layer, so it does not
run; wiring it up is a one-line change in that factory.

All rules are plain Python (no Great Expectations dependency). The per-layer
gate is assembled once in `build_gate_for_layer()` and reused by both the
pipeline orchestrator and the `observatory validate` CLI command.

---

## 🪵 Logging

Logging is centralised in
[`data_pipeline/logging_config.py`](data_pipeline/logging_config.py) and
configured from settings:

```env
LOG_LEVEL=INFO     # DEBUG | INFO | WARNING | ERROR
LOG_FORMAT=json    # text (human-readable) | json (structured, one object/line)
```

In `json` mode every record is emitted as a single JSON line with
`timestamp`, `level`, `logger`, and `message`, plus any structured context
passed via `extra={...}` — ready to ship to CloudWatch / Loki / Datadog.

---

## 🔔 Alerting

When a run **fails** or is **blocked** by a quality gate, the pipeline can post
a Slack notification (best-effort — a delivery failure is logged, never fatal).
Disabled by default; enable with:

```env
ALERTS_ENABLED=true
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/XXX/YYY/ZZZ
```

See [`data_pipeline/alerting.py`](data_pipeline/alerting.py).

---

## 🔁 Self-Healing (Quarantine)

The Silver transformer validates each record (required fields + value ranges).
Rather than silently dropping rejects, it routes them to a `quarantine`
dead-letter prefix in the data lake — tagged with the rejection reason and a
timestamp — and the run **self-heals** by continuing with the valid subset.
Quarantining is best-effort: a storage failure is logged, never fatal.

```env
QUARANTINE_ENABLED=true   # set false to drop rejects instead
```

See `_quarantine` in
[`data_pipeline/transformation/silver.py`](data_pipeline/transformation/silver.py).

---

## 🧬 Data Lineage

Every run writes a **lineage manifest** to the `lineage/` prefix in the data
lake (`lineage_<run_id>.json`), tying a run to the exact artifacts it produced:

```json
{
  "run_id": "…",
  "cities": ["London", "Paris"],
  "started_at": "2026-…",
  "artifact_count": 3,
  "artifacts": [
    {"layer": "bronze", "key": "bronze/weather/year=…/b.json", "record_count": 5},
    {"layer": "silver", "key": "silver/weather/year=…/s.json", "record_count": 5},
    {"layer": "gold",   "key": "gold/weather/year=…/g.json",   "record_count": 5}
  ]
}
```

So you can trace which Bronze/Silver/Gold objects belong to a given run. See
[`data_pipeline/lineage.py`](data_pipeline/lineage.py).

---

## 🎯 Demonstrating Senior-Level Skills

This project showcases key capabilities that differentiate a **Senior Data Engineer**:

| Skill Area | How It's Demonstrated |
|------------|----------------------|
| **Quality Engineering** | Custom shift-left quality gates with severity-driven pipeline blocking |
| **Data Architecture** | Medallion architecture, ELT patterns, one canonical schema module guarded by a consistency test |
| **Production Readiness** | Docker, CI, unit + service-backed integration tests |
| **Cloud Infrastructure** | AWS S3 (simulated with LocalStack), Terraform for storage/registry/database provisioning |
| **Observability** | Streamlit dashboard, structured JSON logging, quality metrics persisted per run |
| **Software Engineering** | Type hints (mypy strict), clean code, documentation |

---

## 🛠️ Development

### Code Quality

```bash
# Linting
ruff check data_pipeline/

# Formatting
ruff format data_pipeline/

# Type checking
mypy data_pipeline/
```

### Adding New Quality Rules

```python
from data_pipeline.quality.gates import QualityGate, QualityIssue, QualitySeverity

def custom_rule(data: list[dict]) -> list[QualityIssue]:
    issues = []
    # Your validation logic here
    if some_condition:
        issues.append(QualityIssue(
            rule_name="custom_rule",
            severity=QualitySeverity.WARNING,
            message="Description of the issue",
        ))
    return issues

# Use in pipeline
gate = QualityGate("my_gate")
gate.add_rule(custom_rule)
```

### Evolving the Schema

The weather schema is defined **once** in
[`data_pipeline/schema.py`](data_pipeline/schema.py). The Bronze/Silver
frozensets and the `SilverTransformer` type map all import from it, so they
cannot drift.

One artifact can't import Python — `sql/schema.sql` — so it's guarded by
[`tests/unit/test_schema_consistency.py`](tests/unit/test_schema_consistency.py).
To add or change a column:

1. Edit `data_pipeline/schema.py` (and `WeatherData` for a new raw field).
2. Run `pytest tests/unit/test_schema_consistency.py` — failures point you at
   the SQL DDL that still needs updating.

---

## 🗓️ Orchestration (Dagster)

The pipeline can run under [Dagster](https://dagster.io/) for scheduling, a run
UI, retries, and run history — without duplicating any ETL logic (the Dagster
asset wraps `DataPipeline`). It's an optional extra:

```bash
pip install -e ".[orchestration]"

# launch the Dagster UI
dagster dev -m data_pipeline.orchestration.definitions
```

The schedule defaults to hourly; override with `DAGSTER_CRON` (standard cron).
A successful run materializes the `weather_observatory` asset with metadata; a
failed/blocked run raises `dagster.Failure` so it surfaces (and retries) in the
UI. See [`data_pipeline/orchestration/definitions.py`](data_pipeline/orchestration/definitions.py).

---

## 📈 Roadmap

- [x] Orchestration & scheduling — via [Dagster](data_pipeline/orchestration/definitions.py)
- [x] Implement data lineage tracking — see [`data_pipeline/lineage.py`](data_pipeline/lineage.py)
- [x] Add Slack alerting — see [`data_pipeline/alerting.py`](data_pipeline/alerting.py)
- [ ] Support additional data sources (financial APIs, etc.)
- [x] **Provision** AWS storage with Terraform — see [`infra/terraform/`](infra/terraform/)
- [ ] **Deploy** to AWS — the Terraform provisions the data lake, two ECR
      repositories and the RDS serving layer, but there is no compute
      (ECS/Lambda/Batch), no VPC/security groups, no IAM roles and no remote
      state backend. Nothing runs the pipeline in AWS yet
- [ ] Wire `freshness_rule` into a layer, or remove it
- [ ] Replace the `deploy-staging` / `deploy-production` echo stubs in
      [`deploy.yml`](.github/workflows/deploy.yml) with a real deployment

---

## 📜 License

MIT License - see [LICENSE](LICENSE) for details.

---

## 🙏 Acknowledgments

- [OpenWeather API](https://openweathermap.org/api) for weather data
- [Streamlit](https://streamlit.io/) for the monitoring dashboard
- [LocalStack](https://localstack.cloud/) for AWS simulation

---

<p align="center">
  Built with ❤️ and a <strong>Quality-First</strong> mindset
</p>
