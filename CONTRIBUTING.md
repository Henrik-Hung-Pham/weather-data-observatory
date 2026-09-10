# Contributing to Data Observatory

Thanks for your interest in improving the project! This guide covers local
setup, the quality bar CI enforces, and conventions for branches and PRs.

## Development setup

```bash
# Clone and create a virtualenv
python -m venv venv && source venv/bin/activate

# Install the package with dev (and optionally security/orchestration) extras
pip install -e ".[dev]"
# pip install -e ".[dev,security,orchestration]"   # everything

# Copy env defaults
cp .env.example .env   # add your OPENWEATHER_API_KEY
```

## Quality bar (run before pushing)

CI lints, type-checks, and tests the **whole repo**. Run the same checks
locally:

```bash
ruff check data_pipeline/ tests/ dashboard/      # lint
ruff format data_pipeline/ tests/ dashboard/     # format
mypy data_pipeline/ dashboard/                   # type-check
pytest tests/unit/ --cov=data_pipeline           # tests + coverage floor
```

- New code needs tests. The coverage floor
  (`tool.coverage.report.fail_under`) must not regress.
- Keep `ruff` and `mypy` clean — both are required CI checks.
- Integration tests need Docker services: `docker-compose up -d postgres localstack`.

## Changing the schema

The weather schema is defined **once** in
[`data_pipeline/schema.py`](data_pipeline/schema.py). Python call sites import
from it, and [`tests/unit/test_schema_consistency.py`](tests/unit/test_schema_consistency.py)
guards the artifacts that can't (the SQL DDL). Edit `schema.py`, then run that
test — failures tell you what else to update.

## Adding a quality rule

Add the rule to [`data_pipeline/quality/gates.py`](data_pipeline/quality/gates.py)
and wire it into the relevant layer in `build_gate_for_layer()`. Cover it with
a unit test in `tests/unit/test_quality_gates.py`.

## Branches & commits

- Branch from `main` using a descriptive prefix: `feat/…`, `fix/…`,
  `perf/…`, `ci/…`, `docs/…`, `test/…`.
- Write imperative, scoped commit subjects ("Add X", "Fix Y") with a body
  explaining the *why*.
- Keep PRs focused on a single concern; update docs and `CHANGELOG.md` in the
  same PR.

## Pull requests

- Fill in what changed and why, and how you validated it.
- Ensure all CI checks pass (lint, types, unit, integration, security).
- A maintainer (see [CODEOWNERS](.github/CODEOWNERS)) will review.
