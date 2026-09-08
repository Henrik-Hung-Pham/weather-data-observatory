"""Silver layer transformation - data cleaning and normalization.

Transforms raw Bronze layer data into clean, standardized Silver layer data.
Handles null values, data type conversions, and schema enforcement.

Null policy
-----------
Cleaning **never substitutes a value for missing input**. A field that is
absent or unparseable stays ``None``.

This used to work the other way: ``_safe_float`` returned ``0.0``,
``_safe_int`` returned ``0``, ``_clean_string`` returned ``""`` and
``_normalize_timestamp`` returned ``now()``. That fabricated readings -- a
missing temperature became a plausible 0.0 °C in the serving layer -- and,
because no nulls survived cleaning, it also guaranteed the Silver gate's
``null_check_rule`` could never fire. The cleaning step was destroying the
very signal the next step existed to detect.

Instead, a record missing any of :attr:`SilverTransformer.REQUIRED_FIELDS`
is quarantined with the specific field named, and the run self-heals with
the valid subset. Optional fields keep their ``None`` and land as SQL NULL.
"""

import logging
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from data_pipeline.schema import SILVER_FIELD_TYPES
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

    # Expected schema for Silver layer, sourced from the canonical schema
    # module (data_pipeline/schema.py). Must match the keys produced by
    # _clean_record() below, including the underscore-prefixed metadata fields.
    # The mapped type describes a *present* value; every field is nullable,
    # because cleaning preserves missing input rather than inventing a value.
    SCHEMA = SILVER_FIELD_TYPES

    # Fields that must carry a real value for a record to be usable. A null
    # here means the reading is unusable, so the record is quarantined rather
    # than defaulted -- these back the Silver gate's null_check_rule and the
    # NOT NULL columns in sql/schema.sql.
    REQUIRED_FIELDS = ("city", "country", "temperature_celsius", "timestamp")

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
                reason = self._rejection_reason(clean_record)
                if reason is None:
                    transformed.append(clean_record)
                else:
                    logger.debug(f"Validation failed: {reason}")
                    errors.append(
                        {
                            "record": record,
                            "reason": reason,
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
            # Self-heal: isolate bad records instead of silently dropping them,
            # so the run continues with the valid subset and rejects stay auditable.
            self._quarantine(errors)

        logger.info(f"Successfully transformed {len(transformed)} records")
        return transformed

    def _clean_record(self, record: dict[str, Any]) -> dict[str, Any]:
        """Clean and normalize a single record.

        Args:
            record: Raw Bronze layer record.

        Returns:
            Cleaned record matching Silver schema.
        """
        # Extract and normalize fields. Missing or unparseable input stays
        # None -- see the module note on why we never substitute a default.
        cleaned: dict[str, Any] = {
            "city": self._clean_string(record.get("city")),
            "country": self._clean_string(record.get("country")),
            "temperature_celsius": self._safe_float(record.get("temperature_celsius")),
            "feels_like_celsius": self._safe_float(record.get("feels_like_celsius")),
            "humidity": self._safe_int(record.get("humidity")),
            "pressure": self._safe_int(record.get("pressure")),
            "wind_speed": self._safe_float(record.get("wind_speed")),
            "wind_direction": self._safe_int(record.get("wind_direction")),
            "weather_condition": self._clean_string(record.get("weather_condition")),
            "weather_description": self._clean_string(record.get("weather_description")),
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

    def _clean_string(self, value: Any) -> str | None:
        """Trim a string value, preserving absence as None.

        A blank or whitespace-only value is absence, not an empty reading, so
        it normalizes to None rather than "".
        """
        if value is None:
            return None
        cleaned = str(value).strip()
        return cleaned or None

    def _safe_float(self, value: Any) -> float | None:
        """Convert to float, or None if absent/unparseable.

        Returns None rather than 0.0: a missing temperature is not 0 °C, and
        substituting one both fabricates a reading and hides it from the null
        check downstream.
        """
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _safe_int(self, value: Any) -> int | None:
        """Convert to int, or None if absent/unparseable."""
        if value is None:
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None

    def _normalize_timestamp(self, value: Any) -> str | None:
        """Normalize a timestamp to ISO format, or None if absent/unparseable.

        Returns None rather than ``now()``: stamping the current time onto a
        reading with no timestamp silently backdates unknown data to the run
        time and defeats the freshness check.
        """
        if value is None:
            return None

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
        except (ValueError, TypeError, OSError, OverflowError):
            return None

    def _rejection_reason(self, record: dict[str, Any]) -> str | None:
        """Explain why a cleaned record is unusable, or None if it is fine.

        Args:
            record: Cleaned record to validate.

        Returns:
            A human-readable reason for rejection, or None when the record is
            valid. The reason is carried into the quarantine payload so a
            reject can be triaged without re-running the pipeline.
        """
        # Required fields must carry a real value. Previously a null here was
        # silently defaulted (0.0 / "" / now()), which fabricated a reading
        # and blinded the Silver gate's null check.
        for field in self.REQUIRED_FIELDS:
            if record.get(field) is None:
                return f"Missing required field '{field}'"

        # Check value ranges
        for field, (min_val, max_val) in self.VALID_RANGES.items():
            value = record.get(field)
            if value is not None and not (min_val <= value <= max_val):
                return f"{field}={value} outside range [{min_val}, {max_val}]"

        return None

    def _validate_record(self, record: dict[str, Any]) -> bool:
        """Validate a cleaned record against required fields and ranges.

        Args:
            record: Cleaned record to validate.

        Returns:
            True if record is valid.
        """
        reason = self._rejection_reason(record)
        if reason is not None:
            logger.debug(f"Validation failed: {reason}")
            return False
        return True

    def _log_errors(self, errors: list[dict[str, Any]]) -> None:
        """Log transformation errors for debugging."""
        for error in errors[:5]:  # Log first 5 errors
            logger.debug(f"Transform error: {error['reason']} for {error['record'].get('city')}")

    def _quarantine(self, errors: list[dict[str, Any]]) -> str | None:
        """Route rejected records to a quarantine prefix (dead-letter).

        Best-effort: always logs, and additionally persists the rejects to the
        data lake under a ``quarantine`` prefix (with the rejection reason and
        a timestamp) when quarantining is enabled and storage is available.
        A storage failure is logged but never propagates, so isolating bad
        data cannot itself break the run.

        Args:
            errors: Records that failed cleaning/validation, each as
                ``{"record": ..., "reason": ...}``.

        Returns:
            The quarantine S3 key if records were persisted, else None.
        """
        self._log_errors(errors)

        if not errors:
            return None

        from data_pipeline.config import get_settings

        if not get_settings().quarantine_enabled or self.storage is None:
            return None

        timestamp = datetime.now(timezone.utc)
        payload = [
            {
                "record": e.get("record"),
                "reason": e.get("reason"),
                "quarantined_at": timestamp.isoformat(),
                "source_layer": "silver",
            }
            for e in errors
        ]
        filename = f"silver_rejects_{timestamp.strftime('%Y%m%d_%H%M%S')}"

        try:
            key = self.storage.write_json(payload, "quarantine", filename, timestamp)
            logger.warning(f"Quarantined {len(errors)} record(s) to {key}")
            return key
        except Exception as e:
            logger.warning(f"Failed to quarantine {len(errors)} record(s): {e}")
            return None

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
