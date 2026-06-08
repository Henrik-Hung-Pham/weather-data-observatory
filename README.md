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

---

## ✨ Key Features

### 🛡️ Quality Gates (Shift-Left Approach)
- **Automatic pipeline blocking** when data quality issues are detected
- **Schema drift detection** - pipeline stops if schema changes unexpectedly
- **Configurable severity** - `warn` mode logs issues, `block` mode stops the pipeline
- **Great Expectations integration** for declarative data validation

### 📊 Monitoring Dashboard
- **Real-time weather visualization**
- **Data quality metrics** (% passing validation)
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

---

## 📈 Roadmap

- [ ] Add Apache Airflow for scheduling
- [ ] Implement data lineage tracking
- [ ] Add Slack/PagerDuty alerting
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
