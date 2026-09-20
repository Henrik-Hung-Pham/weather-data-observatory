"""Tests for the Streamlit dashboard.

The dashboard was 643 lines with no tests at all -- it is also the part of
this project a reviewer is most likely to open, and the part most likely to
raise on a shape the pipeline no longer produces. These tests run the real
app through Streamlit's AppTest harness rather than asserting on internals.

The database is faked by swapping ``data_pipeline.storage.DatabaseManager``:
the app imports it at script-run time, so the fake is picked up without the
app knowing it is under test.
"""

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
import streamlit as st
from streamlit.testing.v1 import AppTest

from dashboard.app import format_temperature

APP = "dashboard/app.py"


@pytest.fixture(autouse=True)
def _clear_streamlit_caches():
    """get_database is @st.cache_resource; a cached fake would leak across tests."""
    st.cache_resource.clear()
    st.cache_data.clear()
    yield
    st.cache_resource.clear()
    st.cache_data.clear()


class _FakeDatabase:
    """Stands in for DatabaseManager with the shapes the dashboard expects."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.healthy = True

    def health_check(self) -> bool:
        return self.healthy

    def get_quality_metrics(self) -> dict[str, Any]:
        return {
            "total_records": 1234,
            "unique_cities": 5,
            "avg_temperature": 16.5,
            "valid_temperature_pct": 99.2,
        }

    def get_latest_weather(self, limit: int = 100) -> list[dict[str, Any]]:
        now = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)
        return [
            {
                "city": "London",
                "country": "GB",
                "temperature_celsius": 14.0 + i,
                "humidity": 70,
                "pressure": 1013,
                "wind_speed": 3.5,
                "weather_condition": "Clouds",
                "recorded_at": now - timedelta(hours=i),
            }
            for i in range(3)
        ]

    def get_latest_gate_results(self) -> list[dict[str, Any]]:
        return [
            {
                "layer": "bronze",
                "gate_passed": True,
                "expectations_evaluated": 10,
                "expectations_passed": 10,
                "failure_reason": None,
            },
            {
                "layer": "silver",
                "gate_passed": False,
                "expectations_evaluated": 10,
                "expectations_passed": 7,
                "failure_reason": "humidity out of range",
            },
        ]

    def get_quality_trend(self, days: int = 14) -> list[dict[str, Any]]:
        return [
            {"date": datetime(2026, 1, 1).date(), "layer": "bronze", "avg_pass_rate": 100.0},
            {"date": datetime(2026, 1, 2).date(), "layer": "bronze", "avg_pass_rate": 90.0},
        ]

    def get_recent_pipeline_runs(self, limit: int = 20) -> list[dict[str, Any]]:
        return [
            {
                "run_id": "11111111-1111-1111-1111-111111111111",
                "status": "success",
                "started_at": datetime(2026, 1, 1, 12, tzinfo=timezone.utc),
                "duration_seconds": 1.5,
                "records_loaded": 5,
            }
        ]

    def get_pipeline_run_stats(self) -> dict[str, Any]:
        return {
            "last_run_at": datetime(2026, 1, 1, 12, tzinfo=timezone.utc),
            "success_rate": 98.0,
            "avg_duration_seconds": 1.4,
        }


class _UnreachableDatabase(_FakeDatabase):
    def health_check(self) -> bool:
        return False


def _run_with(monkeypatch, database_cls, page: str | None = None) -> AppTest:
    import data_pipeline.storage as storage

    monkeypatch.setattr(storage, "DatabaseManager", database_cls)
    app = AppTest.from_file(APP, default_timeout=60)
    app.run()
    if page is not None:
        # The sidebar radio is the app's only navigation; selecting a page and
        # re-running is what a click does.
        app.sidebar.radio[0].set_value(page).run()
    return app


@pytest.mark.unit
@pytest.mark.parametrize(
    ("celsius", "expected"),
    [
        (-0.1, "🥶"),
        (-20.0, "🥶"),
        (0.0, "❄️"),  # boundary: 0 is cold, not freezing
        (14.9, "❄️"),
        (15.0, "🌤️"),  # boundary: mild starts at 15
        (24.9, "🌤️"),
        (25.0, "🔥"),  # boundary: hot starts at 25
    ],
)
def test_format_temperature_buckets(celsius: float, expected: str):
    formatted = format_temperature(celsius)
    assert formatted.startswith(expected)
    assert f"{celsius:.1f}°C" in formatted


@pytest.mark.unit
def test_dashboard_renders_overview_against_a_healthy_database(monkeypatch):
    app = _run_with(monkeypatch, _FakeDatabase)

    assert not app.exception
    assert app.title[0].value == "🔭 Data Observatory"

    # The overview's metric row, straight from get_quality_metrics().
    values = {metric.label: metric.value for metric in app.metric}
    assert values["Total Records"] == "1,234"
    assert values["Cities Tracked"] == "5"
    assert values["Avg Temperature"] == "16.5°C"
    assert values["Data Quality"] == "99.2%"


@pytest.mark.unit
def test_dashboard_degrades_instead_of_crashing_when_the_database_is_down(monkeypatch):
    """The failure mode a reviewer hits first: no docker-compose running."""
    app = _run_with(monkeypatch, _UnreachableDatabase)

    assert not app.exception
    assert any("Database connection unavailable" in error.value for error in app.error)
    # It must stop before the render functions, not fall through to them.
    assert not app.metric


@pytest.mark.unit
def test_dashboard_survives_an_empty_serving_layer(monkeypatch):
    """A fresh install has a healthy database and no rows in it."""

    class _EmptyDatabase(_FakeDatabase):
        def get_quality_metrics(self) -> dict[str, Any]:
            return {}

        def get_latest_weather(self, limit: int = 100) -> list[dict[str, Any]]:
            return []

    app = _run_with(monkeypatch, _EmptyDatabase)

    assert not app.exception
    # Defaults, not a KeyError or a division by zero.
    values = {metric.label: metric.value for metric in app.metric}
    assert values["Total Records"] == "0"
    assert values["Avg Temperature"] == "0.0°C"
    assert any("No weather data available" in info.value for info in app.info)


@pytest.mark.unit
def test_weather_explorer_page_summarises_the_readings(monkeypatch):
    app = _run_with(monkeypatch, _FakeDatabase, page="🌡️ Weather Data")

    assert not app.exception
    values = {metric.label: metric.value for metric in app.metric}
    # Three readings at 14, 15 and 16 degrees.
    assert values["Count"] == "3"
    assert values["Max Temp"] == "16.0°C"
    assert values["Min Temp"] == "14.0°C"


@pytest.mark.unit
def test_quality_page_renders_a_card_per_gate(monkeypatch):
    app = _run_with(monkeypatch, _FakeDatabase, page="✅ Data Quality")

    assert not app.exception
    markdown = " ".join(block.value for block in app.markdown)
    assert "Bronze Layer" in markdown
    assert "Silver Layer" in markdown
    # The failing gate must show why, not just that it failed.
    assert any("humidity out of range" in caption.value for caption in app.caption)


@pytest.mark.unit
def test_pipeline_status_page_reports_the_last_run(monkeypatch):
    app = _run_with(monkeypatch, _FakeDatabase, page="🚀 Pipeline Status")

    assert not app.exception
    values = {metric.label: metric.value for metric in app.metric}
    assert values["Pipeline Status"] == "🟢 Ready"
    assert values["Success Rate"] == "98.0%"
    assert "2026-01-01" in values["Last Run"]


@pytest.mark.unit
def test_pipeline_status_page_handles_a_database_with_no_runs_yet(monkeypatch):
    class _NoRuns(_FakeDatabase):
        def get_pipeline_run_stats(self) -> dict[str, Any]:
            return {}

        def get_recent_pipeline_runs(self, limit: int = 20) -> list[dict[str, Any]]:
            return []

    app = _run_with(monkeypatch, _NoRuns, page="🚀 Pipeline Status")

    assert not app.exception
    values = {metric.label: metric.value for metric in app.metric}
    assert values["Pipeline Status"] == "⚪ No runs yet"
    assert values["Success Rate"] == "—"


@pytest.mark.unit
@pytest.mark.parametrize("page", ["✅ Data Quality", "🚀 Pipeline Status"])
def test_pages_warn_rather_than_crash_when_the_schema_is_unmigrated(monkeypatch, page: str):
    """The quality and run tables are newer than the weather table.

    A database created before them raises ProgrammingError on these reads. The
    dashboard must say so and keep rendering, not die with a traceback.
    """

    class _UnmigratedDatabase(_FakeDatabase):
        def get_latest_gate_results(self) -> list[dict[str, Any]]:
            raise RuntimeError('relation "quality_gate_results" does not exist')

        def get_quality_trend(self, days: int = 14) -> list[dict[str, Any]]:
            raise RuntimeError('relation "quality_gate_results" does not exist')

        def get_pipeline_run_stats(self) -> dict[str, Any]:
            raise RuntimeError('relation "pipeline_runs" does not exist')

        def get_recent_pipeline_runs(self, limit: int = 20) -> list[dict[str, Any]]:
            raise RuntimeError('relation "pipeline_runs" does not exist')

    app = _run_with(monkeypatch, _UnmigratedDatabase, page=page)

    assert not app.exception
    assert any("does not exist" in warning.value for warning in app.warning)
