"""PostgreSQL database manager for the serving layer.

Handles connections, schema management, and Gold layer data serving.
"""

import logging
from collections.abc import Generator
from contextlib import contextmanager
from datetime import datetime
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from data_pipeline.config import get_settings

logger = logging.getLogger(__name__)


class DatabaseError(Exception):
    """Custom exception for database operations."""

    pass


class DatabaseManager:
    """PostgreSQL database manager for Gold layer serving.

    Handles connection pooling, schema initialization, and data operations.
    """

    def __init__(self, database_url: str | None = None):
        """Initialize database manager.

        Args:
            database_url: PostgreSQL connection URL. Uses settings if not provided.
        """
        settings = get_settings()
        self.database_url = database_url or settings.database_url

        self.engine: Engine = create_engine(
            self.database_url,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            echo=False,
        )
        self.SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=self.engine,
        )

    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """Get a database session with automatic cleanup.

        Yields:
            SQLAlchemy session.

        Example:
            with db.get_session() as session:
                session.execute(...)
        """
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def initialize_schema(self, schema_path: str = "sql/schema.sql") -> bool:
        """Initialize database schema from SQL file.

        Args:
            schema_path: Path to schema SQL file.

        Returns:
            True if successful.

        Raises:
            DatabaseError: If schema initialization fails.
        """
        logger.info(f"Initializing database schema from {schema_path}")

        try:
            with open(schema_path) as f:
                schema_sql = f.read()

            with self.engine.begin() as conn:
                # Split by semicolons and execute each statement
                for statement in schema_sql.split(";"):
                    statement = statement.strip()
                    if statement:
                        conn.execute(text(statement))

            logger.info("Database schema initialized successfully")
            return True

        except FileNotFoundError:
            logger.warning(f"Schema file not found: {schema_path}")
            return False
        except SQLAlchemyError as e:
            error_msg = f"Schema initialization failed: {e}"
            logger.error(error_msg)
            raise DatabaseError(error_msg) from e

    def insert_weather_data(self, weather_records: list[dict[str, Any]]) -> int:
        """Insert weather data into Gold layer table.

        Args:
            weather_records: List of weather data dictionaries.

        Returns:
            Number of records inserted.

        Raises:
            DatabaseError: If insertion fails.
        """
        if not weather_records:
            return 0

        insert_sql = text("""
            INSERT INTO gold_weather (
                city, country, temperature_celsius, feels_like_celsius,
                humidity, pressure, wind_speed, wind_direction,
                weather_condition, weather_description, clouds_percentage,
                visibility, recorded_at, sunrise, sunset, ingested_at
            ) VALUES (
                :city, :country, :temperature_celsius, :feels_like_celsius,
                :humidity, :pressure, :wind_speed, :wind_direction,
                :weather_condition, :weather_description, :clouds_percentage,
                :visibility, :recorded_at, :sunrise, :sunset, :ingested_at
            )
            ON CONFLICT (city, recorded_at) DO UPDATE SET
                temperature_celsius = EXCLUDED.temperature_celsius,
                humidity = EXCLUDED.humidity,
                ingested_at = EXCLUDED.ingested_at
        """)

        try:
            with self.get_session() as session:
                for record in weather_records:
                    session.execute(
                        insert_sql,
                        {
                            "city": record["city"],
                            "country": record["country"],
                            "temperature_celsius": record["temperature_celsius"],
                            "feels_like_celsius": record["feels_like_celsius"],
                            "humidity": record["humidity"],
                            "pressure": record["pressure"],
                            "wind_speed": record["wind_speed"],
                            "wind_direction": record["wind_direction"],
                            "weather_condition": record["weather_condition"],
                            "weather_description": record["weather_description"],
                            "clouds_percentage": record["clouds_percentage"],
                            "visibility": record["visibility"],
                            "recorded_at": record.get("timestamp") or record.get("recorded_at"),
                            "sunrise": record["sunrise"],
                            "sunset": record["sunset"],
                            "ingested_at": datetime.utcnow(),
                        },
                    )

            logger.info(f"Inserted {len(weather_records)} weather records")
            return len(weather_records)

        except SQLAlchemyError as e:
            error_msg = f"Failed to insert weather data: {e}"
            logger.error(error_msg)
            raise DatabaseError(error_msg) from e

    def get_latest_weather(self, limit: int = 100) -> list[dict[str, Any]]:
        """Get latest weather records.

        Args:
            limit: Maximum number of records to return.

        Returns:
            List of weather records.
        """
        query = text("""
            SELECT * FROM gold_weather
            ORDER BY recorded_at DESC
            LIMIT :limit
        """)

        try:
            with self.get_session() as session:
                result = session.execute(query, {"limit": limit})
                return [dict(row._mapping) for row in result]

        except SQLAlchemyError as e:
            logger.error(f"Failed to fetch weather data: {e}")
            return []

    def get_weather_by_city(
        self,
        city: str,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> list[dict[str, Any]]:
        """Get weather records for a specific city.

        Args:
            city: City name.
            start_date: Start of date range.
            end_date: End of date range.

        Returns:
            List of weather records.
        """
        query = """
            SELECT * FROM gold_weather
            WHERE city = :city
        """
        params: dict[str, Any] = {"city": city}

        if start_date:
            query += " AND recorded_at >= :start_date"
            params["start_date"] = start_date

        if end_date:
            query += " AND recorded_at <= :end_date"
            params["end_date"] = end_date

        query += " ORDER BY recorded_at DESC"

        try:
            with self.get_session() as session:
                result = session.execute(text(query), params)
                return [dict(row._mapping) for row in result]

        except SQLAlchemyError as e:
            logger.error(f"Failed to fetch weather for {city}: {e}")
            return []

    def get_quality_metrics(self) -> dict[str, Any]:
        """Get data quality metrics from the database.

        Returns:
            Dictionary with quality metrics.
        """
        metrics_query = text("""
            SELECT
                COUNT(*) as total_records,
                COUNT(DISTINCT city) as unique_cities,
                MIN(recorded_at) as earliest_record,
                MAX(recorded_at) as latest_record,
                AVG(temperature_celsius) as avg_temperature,
                COUNT(*) FILTER (WHERE humidity IS NOT NULL) as records_with_humidity,
                COUNT(*) FILTER (WHERE temperature_celsius BETWEEN -50 AND 60) as valid_temp_records
            FROM gold_weather
        """)

        try:
            with self.get_session() as session:
                result = session.execute(metrics_query)
                row = result.fetchone()
                if row:
                    mapping = row._mapping
                    total = mapping["total_records"] or 0
                    valid_temp = mapping["valid_temp_records"] or 0
                    return {
                        "total_records": total,
                        "unique_cities": mapping["unique_cities"] or 0,
                        "earliest_record": mapping["earliest_record"],
                        "latest_record": mapping["latest_record"],
                        "avg_temperature": round(mapping["avg_temperature"] or 0, 2),
                        "data_completeness": round(
                            (mapping["records_with_humidity"] or 0) / max(total, 1) * 100, 2
                        ),
                        "valid_temperature_pct": round(valid_temp / max(total, 1) * 100, 2),
                    }
                return {}

        except SQLAlchemyError as e:
            logger.error(f"Failed to fetch quality metrics: {e}")
            return {}

    def health_check(self) -> bool:
        """Check database connectivity.

        Returns:
            True if database is accessible.
        """
        try:
            with self.get_session() as session:
                session.execute(text("SELECT 1"))
            return True
        except SQLAlchemyError:
            return False
