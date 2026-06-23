"""Unit tests for DatabaseManager (no live Postgres required)."""

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
