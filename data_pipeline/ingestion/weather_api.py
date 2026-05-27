"""OpenWeather API client for weather data ingestion.

This module handles fetching weather data from the OpenWeather API
with retry logic, error handling, and comprehensive logging.
"""

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from data_pipeline.config import get_settings

logger = logging.getLogger(__name__)


@dataclass
class WeatherData:
    """Structured weather data from API response."""

    city: str
    country: str
    temperature_kelvin: float
    temperature_celsius: float
    feels_like_celsius: float
    humidity: int
    pressure: int
    wind_speed: float
    wind_direction: int
    weather_condition: str
    weather_description: str
    clouds_percentage: int
    visibility: int
    timestamp: datetime
    sunrise: datetime
    sunset: datetime
    raw_response: dict[str, Any] = field(repr=False)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            "city": self.city,
            "country": self.country,
            "temperature_kelvin": self.temperature_kelvin,
            "temperature_celsius": self.temperature_celsius,
            "feels_like_celsius": self.feels_like_celsius,
            "humidity": self.humidity,
            "pressure": self.pressure,
            "wind_speed": self.wind_speed,
            "wind_direction": self.wind_direction,
            "weather_condition": self.weather_condition,
            "weather_description": self.weather_description,
            "clouds_percentage": self.clouds_percentage,
            "visibility": self.visibility,
            "timestamp": self.timestamp.isoformat(),
            "sunrise": self.sunrise.isoformat(),
            "sunset": self.sunset.isoformat(),
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }


class WeatherAPIError(Exception):
    """Custom exception for Weather API errors."""

    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


class WeatherAPIClient:
    """Client for OpenWeather API with retry logic and error handling.

    Implements exponential backoff for resilient data ingestion.
    """

    BASE_URL = "https://api.openweathermap.org/data/2.5/weather"
    DEFAULT_TIMEOUT = 30

    def __init__(self, api_key: str | None = None):
        """Initialize the Weather API client.

        Args:
            api_key: OpenWeather API key. If not provided, uses settings.
        """
        settings = get_settings()
        self.api_key = api_key if api_key is not None else settings.openweather_api_key

        if not self.api_key:
            raise ValueError(
                "OpenWeather API key is required. Set OPENWEATHER_API_KEY environment variable."
            )

        # Configure session with retry logic
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Create a requests session with retry configuration."""
        session = requests.Session()

        # Exponential backoff retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,  # 1s, 2s, 4s
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False,
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session

    def fetch_weather(self, city: str) -> WeatherData:
        """Fetch current weather data for a city.

        Args:
            city: City name (e.g., "London", "New York")

        Returns:
            WeatherData object with parsed weather information.

        Raises:
            WeatherAPIError: If API request fails.
        """
        logger.info(f"Fetching weather data for: {city}")

        params = {
            "q": city,
            "appid": self.api_key,
            "units": "standard",  # Kelvin for raw data
        }

        start_time = time.time()

        try:
            response = self.session.get(
                self.BASE_URL,
                params=params,
                timeout=self.DEFAULT_TIMEOUT,
            )
            elapsed = time.time() - start_time
            logger.debug(f"API response received in {elapsed:.2f}s")

            if response.status_code == 401:
                raise WeatherAPIError("Invalid API key", status_code=401)

            if response.status_code == 404:
                raise WeatherAPIError(f"City not found: {city}", status_code=404)

            if response.status_code == 429:
                raise WeatherAPIError("Rate limit exceeded", status_code=429)

            if not response.ok:
                raise WeatherAPIError(
                    f"API request failed: {response.text}",
                    status_code=response.status_code,
                )

            data = response.json()
            return self._parse_response(data, city)

        except requests.RequestException as e:
            logger.error(f"Network error fetching weather for {city}: {e}")
            raise WeatherAPIError(f"Network error: {e}") from e

    def _parse_response(self, data: dict[str, Any], city: str) -> WeatherData:
        """Parse API response into WeatherData object.

        Args:
            data: Raw API response JSON.
            city: Original city query.

        Returns:
            Parsed WeatherData object.
        """
        main = data.get("main", {})
        wind = data.get("wind", {})
        weather = data.get("weather", [{}])[0]
        clouds = data.get("clouds", {})
        sys = data.get("sys", {})

        temp_kelvin = main.get("temp", 0)
        temp_celsius = temp_kelvin - 273.15

        return WeatherData(
            city=data.get("name", city),
            country=sys.get("country", "Unknown"),
            temperature_kelvin=temp_kelvin,
            temperature_celsius=round(temp_celsius, 2),
            feels_like_celsius=round(main.get("feels_like", 0) - 273.15, 2),
            humidity=main.get("humidity", 0),
            pressure=main.get("pressure", 0),
            wind_speed=wind.get("speed", 0),
            wind_direction=wind.get("deg", 0),
            weather_condition=weather.get("main", "Unknown"),
            weather_description=weather.get("description", ""),
            clouds_percentage=clouds.get("all", 0),
            visibility=data.get("visibility", 0),
            timestamp=datetime.fromtimestamp(data.get("dt", 0), tz=timezone.utc),
            sunrise=datetime.fromtimestamp(sys.get("sunrise", 0), tz=timezone.utc),
            sunset=datetime.fromtimestamp(sys.get("sunset", 0), tz=timezone.utc),
            raw_response=data,
        )

    def fetch_multiple_cities(self, cities: list[str]) -> list[WeatherData]:
        """Fetch weather data for multiple cities.

        Args:
            cities: List of city names.

        Returns:
            List of WeatherData objects. Failed cities are logged but not included.
        """
        results: list[WeatherData] = []

        for city in cities:
            try:
                weather = self.fetch_weather(city)
                results.append(weather)
                logger.info(f"✓ Successfully fetched weather for {city}")
            except WeatherAPIError as e:
                logger.warning(f"✗ Failed to fetch weather for {city}: {e}")
                # Continue processing other cities
                continue

        logger.info(f"Fetched weather for {len(results)}/{len(cities)} cities")
        return results
