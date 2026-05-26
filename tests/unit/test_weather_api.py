"""Unit tests for Weather API client."""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone

from data_pipeline.ingestion.weather_api import (
    WeatherAPIClient,
    WeatherAPIError,
    WeatherData,
)


class TestWeatherAPIClient:
    """Tests for WeatherAPIClient."""

    @pytest.fixture
    def client(self):
        """Create a client with test API key."""
        return WeatherAPIClient(api_key="test_api_key")

    @pytest.fixture
    def mock_response(self, sample_api_response):
        """Create a mock successful response."""
        mock = MagicMock()
        mock.ok = True
        mock.status_code = 200
        mock.json.return_value = sample_api_response
        return mock

    @pytest.mark.unit
    def test_client_initialization(self, client):
        """Test client initializes with API key."""
        assert client.api_key == "test_api_key"
        assert client.session is not None

    @pytest.mark.unit
    def test_client_requires_api_key(self):
        """Test client raises error without API key.

        ``get_settings`` is ``@lru_cache``d, so the cached Settings (populated by
        conftest.py with ``test_api_key``) would mask an empty override of
        ``os.environ``. Patch the symbol the module actually uses to return a
        Settings whose ``openweather_api_key`` is empty.
        """
        fake_settings = MagicMock()
        fake_settings.openweather_api_key = ""
        with patch(
            "data_pipeline.ingestion.weather_api.get_settings",
            return_value=fake_settings,
        ):
            with pytest.raises(ValueError, match="API key is required"):
                WeatherAPIClient(api_key="")

    @pytest.mark.unit
    def test_fetch_weather_success(self, client, mock_response, sample_api_response):
        """Test successful weather fetch."""
        with patch.object(client.session, "get", return_value=mock_response):
            weather = client.fetch_weather("London")
            
            assert isinstance(weather, WeatherData)
            assert weather.city == "London"
            assert weather.country == "GB"
            assert weather.temperature_celsius == pytest.approx(12.0, rel=0.1)
            assert weather.humidity == 65
            assert weather.weather_condition == "Clear"

    @pytest.mark.unit
    def test_fetch_weather_invalid_api_key(self, client):
        """Test handling of invalid API key."""
        mock = MagicMock()
        mock.status_code = 401
        mock.ok = False
        
        with patch.object(client.session, "get", return_value=mock):
            with pytest.raises(WeatherAPIError, match="Invalid API key"):
                client.fetch_weather("London")

    @pytest.mark.unit
    def test_fetch_weather_city_not_found(self, client):
        """Test handling of city not found."""
        mock = MagicMock()
        mock.status_code = 404
        mock.ok = False
        
        with patch.object(client.session, "get", return_value=mock):
            with pytest.raises(WeatherAPIError, match="City not found"):
                client.fetch_weather("InvalidCity123")

    @pytest.mark.unit
    def test_fetch_weather_rate_limited(self, client):
        """Test handling of rate limiting."""
        mock = MagicMock()
        mock.status_code = 429
        mock.ok = False
        
        with patch.object(client.session, "get", return_value=mock):
            with pytest.raises(WeatherAPIError, match="Rate limit"):
                client.fetch_weather("London")

    @pytest.mark.unit
    def test_fetch_multiple_cities(self, client, mock_response):
        """Test fetching weather for multiple cities."""
        with patch.object(client.session, "get", return_value=mock_response):
            results = client.fetch_multiple_cities(["London", "Paris", "Tokyo"])
            
            assert len(results) == 3
            assert all(isinstance(r, WeatherData) for r in results)

    @pytest.mark.unit
    def test_fetch_multiple_cities_partial_failure(self, client, mock_response):
        """Test that partial failures don't stop processing."""
        error_mock = MagicMock()
        error_mock.status_code = 404
        error_mock.ok = False
        
        # First succeeds, second fails, third succeeds
        with patch.object(
            client.session,
            "get",
            side_effect=[mock_response, error_mock, mock_response],
        ):
            results = client.fetch_multiple_cities(["London", "Invalid", "Tokyo"])
            
            # Should have 2 results (skipping the failed one)
            assert len(results) == 2

    @pytest.mark.unit
    def test_weather_data_to_dict(self):
        """Test WeatherData serialization."""
        weather = WeatherData(
            city="London",
            country="GB",
            temperature_kelvin=285.15,
            temperature_celsius=12.0,
            feels_like_celsius=11.0,
            humidity=65,
            pressure=1013,
            wind_speed=5.5,
            wind_direction=180,
            weather_condition="Clear",
            weather_description="clear sky",
            clouds_percentage=10,
            visibility=10000,
            timestamp=datetime(2024, 1, 30, 12, 0, tzinfo=timezone.utc),
            sunrise=datetime(2024, 1, 30, 7, 0, tzinfo=timezone.utc),
            sunset=datetime(2024, 1, 30, 17, 0, tzinfo=timezone.utc),
            raw_response={},
        )
        
        result = weather.to_dict()
        
        assert result["city"] == "London"
        assert result["temperature_celsius"] == 12.0
        assert "ingested_at" in result
        assert isinstance(result["timestamp"], str)
