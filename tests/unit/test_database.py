"""Unit tests for DatabaseManager (no live Postgres required)."""

import math
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from data_pipeline.storage.database import DatabaseManager


@pytest.fixture
def db():
    """A DatabaseManager whose engine is mocked away (no real connection)."""
    with patch("data_pipeline.storage.database.create_engine"):
        return DatabaseManager("postgresql://test:test@localhost/test")


def _attach_mock_session(monkeypatch, db) -> MagicMock:
    """Replace db.get_session() with a context manager yielding a mock session."""
    session = MagicMock()

    @contextmanager
    def fake_get_session():
        yield session

    monkeypatch.setattr(db, "get_session", fake_get_session)
    return session


@pytest.mark.unit
def test_insert_weather_data_is_a_single_bulk_execute(monkeypatch, db, sample_bronze_data):
    """All rows go through one executemany call, not one execute per row."""
    session = _attach_mock_session(monkeypatch, db)

    inserted = db.insert_weather_data(sample_bronze_data)

    assert inserted == len(sample_bronze_data)
    # One execute() for the whole batch.
    assert session.execute.call_count == 1
    # Second positional arg is the full list of per-row param dicts.
    _, params = session.execute.call_args.args
    assert isinstance(params, list)
    assert len(params) == len(sample_bronze_data)
    assert {p["city"] for p in params} == {"London", "Paris"}
    # A single ingested_at stamp is shared across the batch.
    assert len({p["ingested_at"] for p in params}) == 1


@pytest.mark.unit
def test_insert_weather_data_empty_is_noop(monkeypatch, db):
    """An empty batch returns 0 and never opens a session."""
    session = _attach_mock_session(monkeypatch, db)

    assert db.insert_weather_data([]) == 0
    session.execute.assert_not_called()


@pytest.mark.unit
def test_insert_daily_aggregates_is_a_single_bulk_execute(monkeypatch, db):
    """The rollup batch goes through one executemany, like the weather batch."""
    session = _attach_mock_session(monkeypatch, db)
    rows = [
        {
            "city": "London",
            "country": "GB",
            "date": "2024-01-30",
            "temperature_celsius_mean": 12.0,
            "observation_count": 4,
        },
        {
            "city": "Paris",
            "country": "FR",
            "date": "2024-01-30",
            "temperature_celsius_mean": 9.0,
            "observation_count": 4,
        },
    ]

    assert db.insert_daily_aggregates(rows) == 2

    assert session.execute.call_count == 1
    _, params = session.execute.call_args.args
    assert {p["city"] for p in params} == {"London", "Paris"}
    # The flattened pandas name is mapped to the table's business name.
    assert {p["temp_avg"] for p in params} == {12.0, 9.0}


@pytest.mark.unit
def test_insert_daily_aggregates_empty_is_noop(monkeypatch, db):
    """An empty rollup returns 0 and never opens a session."""
    session = _attach_mock_session(monkeypatch, db)

    assert db.insert_daily_aggregates([]) == 0
    session.execute.assert_not_called()


@pytest.mark.unit
@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_numbers_become_null(value):
    """NaN and infinities are stored as NULL, not as numbers.

    pandas yields NaN for the standard deviation of a single-observation
    group -- a city's first reading of the day. Postgres NUMERIC *accepts*
    NaN, so without this the value would store silently and then poison every
    downstream AVG over the column.
    """
    assert DatabaseManager._finite_or_none(value) is None


@pytest.mark.unit
@pytest.mark.parametrize("value", [0.0, -12.5, 21.34, 100, None, "London"])
def test_ordinary_values_pass_through_unchanged(value):
    """Anything finite (or non-numeric) is handed to the driver untouched."""
    result = DatabaseManager._finite_or_none(value)

    if isinstance(value, float):
        assert isinstance(result, float) and math.isclose(result, value)
    else:
        assert result == value
