"""Integration tests that exercise the real LocalStack S3 and PostgreSQL.

These used to mock ``boto3.client``, ``sqlalchemy.create_engine`` and
``requests.Session``, then bypass ``__init__`` with ``__new__`` and set
``storage = None``. Nothing opened a socket. Meanwhile the CI job stood up a
Postgres service, a LocalStack service, created the bucket and loaded
``sql/schema.sql`` -- all for tests that never touched any of it. The result
was `database.py` sitting at 13% coverage: the module with all the SQL and
all the side effects was the least tested in the project.

These talk to the real services.

Running them
------------
Locally, start the dependencies first::

    docker-compose up -d postgres localstack
    pytest tests/integration/ -v

Without those services the tests **skip**, so a plain ``pytest`` on a laptop
stays green. In CI that would recreate the original problem -- silent
non-execution -- so CI sets ``REQUIRE_INTEGRATION_SERVICES=1``, which turns
every skip into a hard failure.
"""

import os
import socket
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlparse

import pytest
from sqlalchemy import text

# In CI the services are provisioned, so an unreachable service is a failure,
# not a reason to quietly pass.
_REQUIRE_SERVICES = os.getenv("REQUIRE_INTEGRATION_SERVICES") == "1"

pytestmark = pytest.mark.integration


def _service_unavailable(service: str, exc: object) -> None:
    """Skip locally, fail loudly in CI."""
    message = f"{service} is not reachable: {exc}"
    if _REQUIRE_SERVICES:
        pytest.fail(f"{message} (REQUIRE_INTEGRATION_SERVICES=1 -- services must be up)")
    pytest.skip(f"{message} -- run `docker-compose up -d postgres localstack`")


def _require_port(service: str, host: str, port: int) -> None:
    """Fail fast when nothing is listening.

    Without this the boto3 and psycopg2 clients burn their retry budgets
    before giving up, turning a plain local ``pytest`` into a ~25s wait for
    seven skips.
    """
    try:
        with socket.create_connection((host, port), timeout=1.0):
            return
    except OSError as exc:
        _service_unavailable(service, exc)


# ---------------------------------------------------------------------------
# Fixtures bound to the real services
# ---------------------------------------------------------------------------
@pytest.fixture(scope="module")
def storage():
    """A DataLakeStorage pointed at LocalStack, with the bucket ensured."""
    from data_pipeline.config import get_settings
    from data_pipeline.storage import DataLakeStorage

    endpoint = urlparse(get_settings().aws_endpoint_url or "http://localhost:4566")
    _require_port("LocalStack S3", endpoint.hostname or "localhost", endpoint.port or 4566)

    try:
        lake = DataLakeStorage()
        lake.ensure_bucket_exists()
        lake.s3_client.head_bucket(Bucket=lake.bucket_name)
    except Exception as exc:  # noqa: BLE001 - any failure means "not reachable"
        _service_unavailable("LocalStack S3", exc)
    return lake


@pytest.fixture(scope="module")
def database():
    """A DatabaseManager pointed at PostgreSQL."""
    from data_pipeline.config import get_settings
    from data_pipeline.storage import DatabaseManager

    settings = get_settings()
    _require_port("PostgreSQL", settings.postgres_host, settings.postgres_port)

    try:
        db = DatabaseManager()
        if not db.health_check():
            raise RuntimeError("health_check() returned False")
    except Exception as exc:  # noqa: BLE001 - any failure means "not reachable"
        _service_unavailable("PostgreSQL", exc)
    return db


@pytest.fixture
def clean_tables(database):
    """Empty the serving tables around each test."""
    statement = text(
        "TRUNCATE data_quality_metrics, pipeline_runs, gold_weather RESTART IDENTITY CASCADE"
    )
    with database.get_session() as session:
        session.execute(statement)
    yield
    with database.get_session() as session:
        session.execute(statement)


def _weather_row(city: str = "London", **overrides: Any) -> dict[str, Any]:
    row = {
        "city": city,
        "country": "GB",
        "temperature_celsius": 12.0,
        "feels_like_celsius": 11.0,
        "humidity": 65,
        "pressure": 1013,
        "wind_speed": 5.5,
        "wind_direction": 180,
        "weather_condition": "Clear",
        "weather_description": "clear sky",
        "clouds_percentage": 10,
        "visibility": 10000,
        "timestamp": datetime(2024, 1, 30, 12, 0, tzinfo=timezone.utc),
        "sunrise": datetime(2024, 1, 30, 7, 0, tzinfo=timezone.utc),
        "sunset": datetime(2024, 1, 30, 17, 0, tzinfo=timezone.utc),
    }
    row.update(overrides)
    return row


# ---------------------------------------------------------------------------
# Data lake (LocalStack S3)
# ---------------------------------------------------------------------------
def test_datalake_round_trip_through_s3(storage):
    """Write, list and read an object against real S3."""
    timestamp = datetime(2024, 1, 15, 9, 30, tzinfo=timezone.utc)
    payload = [{"city": "London", "temperature_celsius": 12.0}]
    filename = f"integration_{uuid.uuid4().hex}"

    key = storage.write_json(payload, "bronze", filename, timestamp)

    try:
        # Hive partitioning is the documented default and query engines
        # depend on it, so assert the real key layout, not just the write.
        assert "year=2024/month=01/day=15/" in key

        assert storage.read_json(key) == payload
        assert key in storage.list_objects("bronze")
    finally:
        assert storage.delete_object(key) is True


def test_datalake_date_filtering_uses_the_partition_path(storage):
    """list_objects date filtering must parse real Hive partition keys."""
    old = datetime(2023, 3, 4, tzinfo=timezone.utc)
    recent = datetime(2024, 6, 7, tzinfo=timezone.utc)
    tag = uuid.uuid4().hex

    old_key = storage.write_json([{"n": 1}], "silver", f"old_{tag}", old)
    recent_key = storage.write_json([{"n": 2}], "silver", f"recent_{tag}", recent)

    try:
        from_2024 = storage.list_objects(
            "silver", start_date=datetime(2024, 1, 1, tzinfo=timezone.utc)
        )
        assert recent_key in from_2024
        assert old_key not in from_2024
    finally:
        storage.delete_object(old_key)
        storage.delete_object(recent_key)


# ---------------------------------------------------------------------------
# Serving layer (PostgreSQL)
# ---------------------------------------------------------------------------
def test_weather_insert_and_read_back(database, clean_tables):
    """insert_weather_data -> the read paths the dashboard uses."""
    rows = [_weather_row("London"), _weather_row("Paris", country="FR")]

    assert database.insert_weather_data(rows) == 2

    latest = database.get_latest_weather(limit=10)
    assert {r["city"] for r in latest} == {"London", "Paris"}

    london = database.get_weather_by_city("London")
    assert len(london) == 1
    assert float(london[0]["temperature_celsius"]) == 12.0

    metrics = database.get_quality_metrics()
    assert metrics["total_records"] == 2
    assert metrics["unique_cities"] == 2


def test_weather_insert_is_idempotent_on_city_and_time(database, clean_tables):
    """The ON CONFLICT upsert must not duplicate a re-ingested reading."""
    row = _weather_row("London")

    database.insert_weather_data([row])
    database.insert_weather_data([{**row, "temperature_celsius": 14.0}])

    stored = database.get_weather_by_city("London")
    assert len(stored) == 1, "unique (city, recorded_at) should upsert, not duplicate"
    assert float(stored[0]["temperature_celsius"]) == 14.0


def test_run_and_quality_metrics_persist_and_join(database, clean_tables):
    """A run row plus its gate rows, read back through the dashboard queries."""
    run_id = str(uuid.uuid4())
    started = datetime.now(timezone.utc) - timedelta(seconds=5)

    database.insert_pipeline_run(
        {
            "run_id": run_id,
            "status": "success",
            "started_at": started.isoformat(),
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "duration_seconds": 5,
            "cities_processed": 2,
            "records_ingested": 2,
            "records_transformed": 2,
            "records_loaded": 2,
            "quality_gate_passed": True,
            "quality_gate_reason": "",
            "error_message": "",
        }
    )

    for layer in ("bronze", "silver", "gold"):
        database.insert_quality_metrics(
            run_id=run_id,
            gate_result={
                "gate_name": f"{layer}_quality_gate",
                "layer": layer,
                "status": "passed",
                "issues": [],
                "metrics": {"total_records": 2, "rules_evaluated": 2},
            },
        )

    runs = database.get_recent_pipeline_runs(limit=5)
    # str() both sides: the driver may hand back uuid.UUID or str.
    assert [str(r["run_id"]) for r in runs] == [run_id]

    stats = database.get_pipeline_run_stats()
    assert stats["total_runs"] == 1
    assert stats["success_rate"] == 100.0

    gates = database.get_latest_gate_results()
    assert {g["layer"] for g in gates} == {"bronze", "silver", "gold"}
    # pass_rate is a generated column -- confirm Postgres computed it.
    assert all(float(g["pass_rate"]) == 100.0 for g in gates)

    trend = database.get_quality_trend(days=14)
    assert {t["layer"] for t in trend} == {"bronze", "silver", "gold"}

    # The rows are attributable to the run that produced them.
    with database.get_session() as session:
        joined = session.execute(
            text(
                "SELECT COUNT(*) FROM data_quality_metrics dqm "
                "JOIN pipeline_runs pr ON pr.run_id = dqm.run_id"
            )
        ).scalar_one()
    assert joined == 3


def test_quality_metrics_records_a_failing_gate(database, clean_tables):
    """A blocked gate is stored with its reason and a sub-100 pass rate."""
    run_id = str(uuid.uuid4())
    database.insert_pipeline_run(
        {
            "run_id": run_id,
            "status": "blocked",
            "started_at": datetime.now(timezone.utc).isoformat(),
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "duration_seconds": 1,
            "cities_processed": 1,
            "records_ingested": 10,
            "records_transformed": 0,
            "records_loaded": 0,
            "quality_gate_passed": False,
            "quality_gate_reason": "schema_drift",
            "error_message": "",
        }
    )
    database.insert_quality_metrics(
        run_id=run_id,
        gate_result={
            "gate_name": "bronze_quality_gate",
            "layer": "bronze",
            "status": "blocked",
            "issues": [
                {
                    "rule_name": "schema_drift",
                    "severity": "critical",
                    "message": "Missing expected columns",
                    "affected_records": 4,
                }
            ],
            "metrics": {"total_records": 10, "rules_evaluated": 2},
        },
    )

    (gate,) = database.get_latest_gate_results()
    assert gate["gate_passed"] is False
    assert "schema_drift" in gate["failure_reason"]
    assert gate["failed_records"] == 4
    assert float(gate["pass_rate"]) == 60.0

    stats = database.get_pipeline_run_stats()
    assert stats["success_rate"] == 0.0


# ---------------------------------------------------------------------------
# The whole pipeline, against both real services at once
# ---------------------------------------------------------------------------
class _StubWeather:
    def __init__(self, record: dict[str, Any]) -> None:
        self._record = record

    def to_dict(self) -> dict[str, Any]:
        return dict(self._record)


class _StubAPIClient:
    """Only the API is stubbed -- S3 and Postgres are real."""

    def __init__(self, records: list[dict[str, Any]]) -> None:
        self._records = records

    def fetch_multiple_cities(self, cities: list[str]) -> list[_StubWeather]:
        return [_StubWeather(r) for r in self._records]


def test_full_pipeline_lands_data_in_both_services(storage, database, clean_tables):
    """Bronze -> Silver -> Gold end to end, hitting real S3 and Postgres."""
    from data_pipeline.pipeline import DataPipeline

    bronze = [
        {
            "city": "London",
            "country": "GB",
            "temperature_kelvin": 285.15,
            "temperature_celsius": 12.0,
            "feels_like_celsius": 11.0,
            "humidity": 65,
            "pressure": 1013,
            "wind_speed": 5.5,
            "wind_direction": 180,
            "weather_condition": "Clear",
            "weather_description": "clear sky",
            "clouds_percentage": 10,
            "visibility": 10000,
            "timestamp": "2024-01-30T12:00:00+00:00",
            "sunrise": "2024-01-30T07:00:00+00:00",
            "sunset": "2024-01-30T17:00:00+00:00",
            "ingested_at": "2024-01-30T12:05:00+00:00",
        }
    ]

    pipeline = DataPipeline(
        api_client=_StubAPIClient(bronze),
        storage=storage,
        database=database,
    )

    result = pipeline.run(cities=["London"])

    assert result.status == "success", result.error_message or result.quality_gate_reason

    # The serving layer really has the row.
    stored = database.get_weather_by_city("London")
    assert len(stored) == 1
    assert float(stored[0]["temperature_celsius"]) == 12.0

    # The run was recorded.
    runs = database.get_recent_pipeline_runs(limit=5)
    assert str(runs[0]["run_id"]) == str(result.run_id)

    # And every medallion layer has an object in the lake.
    for layer in ("bronze", "silver", "gold"):
        assert storage.list_objects(layer), f"no objects written to {layer}"
