"""Silver layer transformation - data cleaning and normalization.

Transforms raw Bronze layer data into clean, standardized Silver layer data.
Handles null values, data type conversions, and schema enforcement.
"""

import logging
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from data_pipeline.storage import DataLakeStorage

logger = logging.getLogger(__name__)


class SilverTransformError(Exception):
    """Custom exception for Silver transformation errors."""

    pass


class SilverTransformer:
    """Transforms Bronze layer data into clean Silver layer data.

    Responsibilities:
    - Clean and normalize raw data
    - Handle missing values
    - Enforce schema consistency
    - Data type conversions
    """

    # Expected schema for Silver layer.
    # Must match the keys produced by _clean_record() below, including the
    # underscore-prefixed metadata fields that downstream consumers may use.
    SCHEMA = {
        "city": str,
        "country": str,
        "temperature_celsius": float,
        "feels_like_celsius": float,
        "humidity": int,
        "pressure": int,
        "wind_speed": float,
        "wind_direction": int,
        "weather_condition": str,
        "weather_description": str,
        "clouds_percentage": int,
        "visibility": int,
        "timestamp": str,  # ISO format
        "sunrise": str,
        "sunset": str,
        "_transformed_at": str,  # ISO format metadata
        "_source_layer": str,
    }

    # Valid ranges for data validation
    VALID_RANGES = {
        "temperature_celsius": (-100, 100),
        "humidity": (0, 100),
        "pressure": (800, 1200),
        "wind_speed": (0, 200),
        "clouds_percentage": (0, 100),
        "visibility": (0, 100000),
    }

    def __init__(self, storage: DataLakeStorage | None = None):
        """Initialize Silver transformer.

        Args:
            storage: DataLakeStorage instance for reading/writing data.
        """
        self.storage = storage or DataLakeStorage()

    def transform(self, bronze_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Transform Bronze data to Silver format.

        Args:
            bronze_data: List of raw weather data records from Bronze layer.

        Returns:
            List of cleaned and normalized records.

        Raises:
            SilverTransformError: If transformation fails critically.
        """
        if not bronze_data:
            logger.warning("No data to transform")
            return []

        logger.info(f"Transforming {len(bronze_data)} Bronze records to Silver")

        transformed: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []

        for record in bronze_data:
            try:
                clean_record = self._clean_record(record)
                if self._validate_record(clean_record):
                    transformed.append(clean_record)
                else:
                    errors.append(
                        {
                            "record": record,
                            "reason": "Validation failed",
                        }
                    )
            except Exception as e:
                logger.warning(f"Failed to transform record: {e}")
                errors.append(
                    {
                        "record": record,
                        "reason": str(e),
                    }
                )

        if errors:
            logger.warning(f"{len(errors)} records failed transformation")
            # Store failed records for audit
            self._log_errors(errors)

        logger.info(f"Successfully transformed {len(transformed)} records")
        return transformed

    def _clean_record(self, record: dict[str, Any]) -> dict[str, Any]:
        """Clean and normalize a single record.

        Args:
            record: Raw Bronze layer record.

        Returns:
            Cleaned record matching Silver schema.
        """
        # Extract and normalize fields
        cleaned = {
            "city": self._clean_string(record.get("city", "")),
            "country": self._clean_string(record.get("country", "Unknown")),
            "temperature_celsius": self._safe_float(record.get("temperature_celsius")),
            "feels_like_celsius": self._safe_float(record.get("feels_like_celsius")),
            "humidity": self._safe_int(record.get("humidity")),
            "pressure": self._safe_int(record.get("pressure")),
            "wind_speed": self._safe_float(record.get("wind_speed")),
            "wind_direction": self._safe_int(record.get("wind_direction")),
            "weather_condition": self._clean_string(record.get("weather_condition", "Unknown")),
            "weather_description": self._clean_string(record.get("weather_description", "")),
            "clouds_percentage": self._safe_int(record.get("clouds_percentage")),
            "visibility": self._safe_int(record.get("visibility")),
            "timestamp": self._normalize_timestamp(record.get("timestamp")),
            "sunrise": self._normalize_timestamp(record.get("sunrise")),
            "sunset": self._normalize_timestamp(record.get("sunset")),
        }

        # Add metadata
        cleaned["_transformed_at"] = datetime.now(timezone.utc).isoformat()
        cleaned["_source_layer"] = "bronze"

        return cleaned

    def _clean_string(self, value: Any) -> str:
        """Clean string values - trim whitespace, handle nulls."""
        if value is None:
            return ""
        return str(value).strip()

    def _safe_float(self, value: Any, default: float = 0.0) -> float:
        """Safely convert to float."""
        if value is None:
            return default
        try:
            return float(value)
        except (ValueError, TypeError):
            return default

    def _safe_int(self, value: Any, default: int = 0) -> int:
        """Safely convert to int."""
        if value is None:
            return default
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return default

    def _normalize_timestamp(self, value: Any) -> str:
        """Normalize timestamp to ISO format."""
        if value is None:
            return datetime.now(timezone.utc).isoformat()

        if isinstance(value, datetime):
            return value.isoformat()

        if isinstance(value, str):
            # Already ISO format, validate and return
            try:
                dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
                return dt.isoformat()
            except ValueError:
                pass

        # Try parsing as timestamp
        try:
            ts = float(value)
            return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
        except (ValueError, TypeError):
            return datetime.now(timezone.utc).isoformat()

    def _validate_record(self, record: dict[str, Any]) -> bool:
        """Validate a cleaned record against schema and ranges.

        Args:
            record: Cleaned record to validate.

        Returns:
            True if record is valid.
        """
        # Check required fields
        if not record.get("city"):
            logger.debug("Validation failed: missing city")
            return False

        # Check value ranges
        for field, (min_val, max_val) in self.VALID_RANGES.items():
            value = record.get(field)
            if value is not None and not (min_val <= value <= max_val):
                logger.debug(
                    f"Validation failed: {field}={value} outside range [{min_val}, {max_val}]"
                )
                return False

        return True

    def _log_errors(self, errors: list[dict[str, Any]]) -> None:
        """Log transformation errors for debugging."""
        for error in errors[:5]:  # Log first 5 errors
            logger.debug(f"Transform error: {error['reason']} for {error['record'].get('city')}")

    def transform_from_storage(
        self,
        bronze_keys: list[str],
        output_filename: str | None = None,
    ) -> str | None:
        """Transform Bronze data from storage and write to Silver layer.

        Args:
            bronze_keys: List of S3 keys for Bronze data files.
            output_filename: Optional output filename.

        Returns:
            S3 key of Silver layer output, or None if no data.
        """
        all_bronze_data: list[dict[str, Any]] = []

        for key in bronze_keys:
            try:
                data = self.storage.read_json(key)
                if isinstance(data, list):
                    all_bronze_data.extend(data)
                else:
                    all_bronze_data.append(data)
            except Exception as e:
                logger.warning(f"Failed to read {key}: {e}")

        if not all_bronze_data:
            logger.warning("No Bronze data found to transform")
            return None

        silver_data = self.transform(all_bronze_data)

        if not silver_data:
            logger.warning("No data after transformation")
            return None

        # Write to Silver layer
        filename = (
            output_filename or f"weather_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
        )
        return self.storage.write_json(silver_data, "silver", filename)

    def to_dataframe(self, records: list[dict[str, Any]]) -> pd.DataFrame:
        """Convert records to pandas DataFrame for further processing.

        Args:
            records: List of Silver layer records.

        Returns:
            pandas DataFrame.
        """
        df = pd.DataFrame(records)

        # Convert timestamp columns
        for col in ["timestamp", "sunrise", "sunset", "_transformed_at"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])

        return df
