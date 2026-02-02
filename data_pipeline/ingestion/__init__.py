"""Ingestion module for Bronze layer data collection."""

from data_pipeline.ingestion.weather_api import WeatherAPIClient

__all__ = ["WeatherAPIClient"]
