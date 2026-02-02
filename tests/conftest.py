"""Pytest configuration and fixtures for Data Observatory tests."""

import json
import os
from datetime import datetime, timezone
from typing import Any, Generator
from unittest.mock import MagicMock, patch

import pytest

# Set test environment before imports
os.environ["OPENWEATHER_API_KEY"] = "test_api_key"
os.environ["POSTGRES_HOST"] = "localhost"
os.environ["AWS_ENDPOINT_URL"] = "http://localhost:4566"


@pytest.fixture
def sample_api_response() -> dict[str, Any]:
    """Sample OpenWeather API response."""
    return {
        "coord": {"lon": -0.1257, "lat": 51.5085},
        "weather": [
            {"id": 800, "main": "Clear", "description": "clear sky", "icon": "01d"}
        ],
        "base": "stations",
        "main": {
            "temp": 285.15,  # Kelvin
            "feels_like": 284.15,
            "temp_min": 283.15,
            "temp_max": 287.15,
            "pressure": 1013,
            "humidity": 65,
        },
        "visibility": 10000,
        "wind": {"speed": 5.5, "deg": 180},
        "clouds": {"all": 10},
        "dt": 1706619600,
        "sys": {
            "country": "GB",
            "sunrise": 1706598000,
            "sunset": 1706630400,
        },
        "timezone": 0,
        "id": 2643743,
        "name": "London",
        "cod": 200,
    }


@pytest.fixture
def sample_bronze_data() -> list[dict[str, Any]]:
    """Sample Bronze layer data (raw from API)."""
    return [
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
        },
        {
            "city": "Paris",
            "country": "FR",
            "temperature_kelvin": 280.15,
            "temperature_celsius": 7.0,
            "feels_like_celsius": 5.0,
            "humidity": 80,
            "pressure": 1010,
            "wind_speed": 3.2,
            "wind_direction": 270,
            "weather_condition": "Clouds",
            "weather_description": "overcast clouds",
            "clouds_percentage": 90,
            "visibility": 8000,
            "timestamp": "2024-01-30T12:00:00+00:00",
            "sunrise": "2024-01-30T07:30:00+00:00",
            "sunset": "2024-01-30T17:30:00+00:00",
            "ingested_at": "2024-01-30T12:05:00+00:00",
        },
    ]


@pytest.fixture
def sample_silver_data() -> list[dict[str, Any]]:
    """Sample Silver layer data (cleaned)."""
    return [
        {
            "city": "London",
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
            "timestamp": "2024-01-30T12:00:00+00:00",
            "sunrise": "2024-01-30T07:00:00+00:00",
            "sunset": "2024-01-30T17:00:00+00:00",
            "_transformed_at": "2024-01-30T12:10:00+00:00",
            "_source_layer": "bronze",
        },
    ]


@pytest.fixture
def mock_api_session():
    """Mock requests session for API tests."""
    with patch("requests.Session") as mock_session:
        session_instance = MagicMock()
        mock_session.return_value = session_instance
        yield session_instance


@pytest.fixture
def mock_s3_client():
    """Mock S3 client for storage tests."""
    with patch("boto3.client") as mock_client:
        s3_instance = MagicMock()
        mock_client.return_value = s3_instance
        yield s3_instance


@pytest.fixture
def mock_database():
    """Mock database manager."""
    with patch("data_pipeline.storage.database.create_engine") as mock_engine:
        mock_engine.return_value = MagicMock()
        from data_pipeline.storage.database import DatabaseManager
        
        db = DatabaseManager("postgresql://test:test@localhost/test")
        yield db


@pytest.fixture
def temp_expectation_suite(tmp_path) -> Generator:
    """Create temporary expectation suite directory."""
    suite_dir = tmp_path / "expectations"
    suite_dir.mkdir()
    
    # Create a test suite
    test_suite = {
        "expectation_suite_name": "test_suite",
        "expectations": [
            {
                "expectation_type": "expect_column_to_exist",
                "kwargs": {"column": "city"},
                "meta": {},
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "city"},
                "meta": {},
            },
        ],
    }
    
    with open(suite_dir / "test_suite.json", "w") as f:
        json.dump(test_suite, f)
    
    yield suite_dir


# Markers for test categorization
def pytest_configure(config):
    """Configure custom pytest markers."""
    config.addinivalue_line(
        "markers", "unit: mark test as a unit test"
    )
    config.addinivalue_line(
        "markers", "integration: mark test as an integration test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )
