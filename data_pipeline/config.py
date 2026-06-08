"""Configuration management for Data Observatory.

Uses Pydantic Settings for type-safe configuration with environment variable support.
"""

from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # API Configuration
    openweather_api_key: str = Field(
        default="",
        description="OpenWeather API key for weather data ingestion",
    )
    weather_cities: str = Field(
        default="London,New York,Tokyo,Sydney,Paris",
        description="Comma-separated list of cities to fetch weather for",
    )

    # Database Configuration
    postgres_host: str = Field(default="localhost")
    postgres_port: int = Field(default=5432)
    postgres_db: str = Field(default="observatory")
    postgres_user: str = Field(default="observatory")
    postgres_password: str = Field(default="observatory_secret")

    # AWS / LocalStack Configuration
    aws_endpoint_url: str | None = Field(
        default="http://localhost:4566",
        description="LocalStack endpoint for local development",
    )
    aws_access_key_id: str = Field(default="test")
    aws_secret_access_key: str = Field(default="test")
    aws_region: str = Field(default="us-east-1")
    s3_bucket_name: str = Field(default="data-observatory")

    # Data Lake Paths
    bronze_path: str = Field(default="bronze/weather")
    silver_path: str = Field(default="silver/weather")
    gold_path: str = Field(default="gold/weather")
    partition_style: Literal["hive", "plain"] = Field(
        default="hive",
        description="Date partition layout: 'hive' (year=YYYY/month=MM/day=DD, "
        "enables partition pruning in Athena/Glue/Spark) or 'plain' (YYYY/MM/DD)",
    )

    # Pipeline Configuration
    ingestion_interval_minutes: int = Field(default=60)
    quality_gate_mode: Literal["warn", "block"] = Field(
        default="block",
        description="Quality gate behavior: warn (log only) or block (stop pipeline)",
    )

    # Dashboard Configuration
    streamlit_port: int = Field(default=8501)

    @property
    def cities_list(self) -> list[str]:
        """Parse comma-separated cities into a list."""
        return [city.strip() for city in self.weather_cities.split(",")]

    @property
    def database_url(self) -> str:
        """Construct PostgreSQL connection URL."""
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
