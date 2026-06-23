# 🔭 Data Observatory

## Self-Healing Data Quality Platform

[![CI Pipeline](https://github.com/Henrik-Hung-Pham/weather-data-observatory/actions/workflows/ci.yml/badge.svg)](https://github.com/Henrik-Hung-Pham/weather-data-observatory/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
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
- **Great Expectations integration** for declarative data validation

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
- **Containerized** with Docker and Docker Compose
- **CI/CD** with GitHub Actions

---

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
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
│   ├── quality/             # Quality gates & Great Expectations
│   ├── storage/             # S3 & PostgreSQL abstractions
│   ├── orchestration/       # Dagster assets/job/schedule (optional)
│   ├── schema.py            # Canonical schema (single source of truth)
│   └── pipeline.py          # Main orchestrator
├── tests/                   # Comprehensive test suite
│   ├── unit/                # Unit tests
│   ├── integration/         # Integration tests
│   └── conftest.py          # Pytest fixtures
├── dashboard/               # Streamlit monitoring UI
├── great_expectations/      # Expectation suites
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

| Rule | Description | Severity |
|------|-------------|----------|
| `schema_drift_rule` | Detects missing or extra columns | 🔴 Critical |
| `null_check_rule` | Checks for null values in required columns | 🟡 Warning* |
| `range_check_rule` | Validates values are within expected ranges | 🟡 Warning |
| `freshness_rule` | Checks data is not stale | 🟡 Warning |

*Null check becomes critical if >10% of records affected

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

## 🎯 Demonstrating Senior-Level Skills

This project showcases key capabilities that differentiate a **Senior Data Engineer**:

| Skill Area | How It's Demonstrated |
|------------|----------------------|
| **Quality Engineering** | Quality gates with shift-left mindset, Great Expectations |
| **Data Architecture** | Medallion architecture, ELT patterns |
| **Production Readiness** | Docker, CI/CD, comprehensive testing |
| **Cloud Infrastructure** | AWS S3 (simulated with LocalStack), Terraform |
| **Observability** | Streamlit dashboard, metrics collection |
| **Software Engineering** | Type hints, clean code, documentation |

---

## 🔐 Security & Supply Chain

Dependencies are **pinned and hash-locked** for reproducible builds, and every
push/PR is scanned by the [`Security`](.github/workflows/security.yml) workflow:

| Check | Tool | Gate |
|-------|------|------|
| Secret scanning | gitleaks | 🔴 blocking |
| Dependency CVEs | pip-audit (against `requirements.lock`) | 🟡 advisory* |
| Python SAST | bandit | 🟡 advisory* |
| Filesystem / IaC / secrets | Trivy | 🟡 advisory* |

*Advisory scans report findings but don't fail CI yet; promote them to blocking
(remove `continue-on-error`) once they're clean on `main`.

[Dependabot](.github/dependabot.yml) opens weekly update PRs for pip packages,
GitHub Actions, and Docker base images.

### Regenerating the lock file

`requirements.lock` is compiled from `requirements.txt` with fully pinned,
hashed versions. After changing a dependency, regenerate it:

```bash
pip install pip-tools
pip-compile --generate-hashes --strip-extras \
  --output-file requirements.lock requirements.txt
```

Reproducible install:

```bash
pip install --require-hashes -r requirements.lock
```

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

# Security scans (locally)
pip install -e ".[security]"
pip-audit -r requirements.lock
bandit -r data_pipeline dashboard -ll
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
frozensets, the `SilverTransformer` type map, and the validator's default
Bronze expectations all import from it, so they cannot drift.

Two artifacts can't import Python — the Great Expectations JSON suites and
`sql/schema.sql` — so they're guarded by
[`tests/unit/test_schema_consistency.py`](tests/unit/test_schema_consistency.py).
To add or change a column:

1. Edit `data_pipeline/schema.py` (and `WeatherData` for a new raw field).
2. Run `pytest tests/unit/test_schema_consistency.py` — failures point you at
   the JSON suite or SQL DDL that still needs updating.

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
- [ ] Implement data lineage tracking
- [x] Add Slack alerting — see [`data_pipeline/alerting.py`](data_pipeline/alerting.py)
- [ ] Support additional data sources (financial APIs, etc.)
- [x] Deploy to AWS with Terraform — see [`infra/terraform/`](infra/terraform/)

---

## 📜 License

MIT License - see [LICENSE](LICENSE) for details.

---

## 🙏 Acknowledgments

- [OpenWeather API](https://openweathermap.org/api) for weather data
- [Great Expectations](https://greatexpectations.io/) for data validation
- [Streamlit](https://streamlit.io/) for the monitoring dashboard
- [LocalStack](https://localstack.cloud/) for AWS simulation

---

<p align="center">
  Built with ❤️ and a <strong>Quality-First</strong> mindset
</p>
