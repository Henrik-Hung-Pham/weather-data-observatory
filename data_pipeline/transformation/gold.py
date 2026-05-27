"""Gold layer transformation - business aggregations.

Transforms Silver layer data into business-ready Gold layer data.
Creates aggregated views, statistics, and analytics-ready datasets.
"""

import logging
from datetime import datetime, timezone
from typing import Any, cast

import pandas as pd

from data_pipeline.storage import DatabaseManager, DataLakeStorage

logger = logging.getLogger(__name__)


class GoldTransformError(Exception):
    """Custom exception for Gold transformation errors."""

    pass


class GoldTransformer:
    """Transforms Silver layer data into Gold layer aggregations.

    Responsibilities:
    - Create business-level aggregations
    - Calculate statistics and trends
    - Prepare data for serving layer (PostgreSQL)
    - Generate analytics-ready datasets
    """

    def __init__(
        self,
        storage: DataLakeStorage | None = None,
        database: DatabaseManager | None = None,
    ):
        """Initialize Gold transformer.

        Args:
            storage: DataLakeStorage instance.
            database: DatabaseManager instance for serving layer.
        """
        self.storage = storage or DataLakeStorage()
        self.database = database

    def transform(self, silver_data: list[dict[str, Any]]) -> dict[str, Any]:
        """Transform Silver data to Gold format with aggregations.

        Args:
            silver_data: List of cleaned records from Silver layer.

        Returns:
            Dictionary containing:
            - records: Individual weather records (for serving layer)
            - daily_aggregates: Daily city-level aggregations
            - city_statistics: Overall city statistics
            - metadata: Transformation metadata
        """
        if not silver_data:
            logger.warning("No Silver data to transform")
            return {"records": [], "daily_aggregates": [], "city_statistics": []}

        logger.info(f"Transforming {len(silver_data)} Silver records to Gold")

        # Convert to DataFrame for easier aggregation
        df = pd.DataFrame(silver_data)

        # Ensure timestamp is datetime
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df["date"] = df["timestamp"].dt.date

        # Generate different aggregations
        records = self._prepare_records(df)
        daily_agg = self._daily_aggregates(df)
        city_stats = self._city_statistics(df)

        result = {
            "records": records,
            "daily_aggregates": daily_agg,
            "city_statistics": city_stats,
            "metadata": {
                "transformed_at": datetime.now(timezone.utc).isoformat(),
                "record_count": len(records),
                "cities_count": df["city"].nunique(),
                "date_range": {
                    "start": str(df["date"].min()),
                    "end": str(df["date"].max()),
                },
            },
        }

        logger.info(
            f"Gold transformation complete: {len(records)} records, "
            f"{len(daily_agg)} daily aggregates, {len(city_stats)} city stats"
        )

        return result

    def _prepare_records(self, df: pd.DataFrame) -> list[dict[str, Any]]:
        """Prepare individual records for Gold layer.

        Enriches records with additional computed fields.
        """
        records = df.copy()

        # Add computed fields
        records["temp_category"] = pd.cut(
            records["temperature_celsius"],
            bins=[-100, 0, 10, 20, 30, 100],
            labels=["freezing", "cold", "mild", "warm", "hot"],
        )

        records["wind_category"] = pd.cut(
            records["wind_speed"],
            bins=[-1, 5, 15, 30, 200],
            labels=["calm", "light", "moderate", "strong"],
        )

        # Convert back to records
        return cast(list[dict[str, Any]], records.to_dict(orient="records"))

    def _daily_aggregates(self, df: pd.DataFrame) -> list[dict[str, Any]]:
        """Create daily city-level aggregations."""
        daily = (
            df.groupby(["city", "country", "date"])
            .agg(
                {
                    "temperature_celsius": ["mean", "min", "max", "std"],
                    "humidity": ["mean", "min", "max"],
                    "pressure": "mean",
                    "wind_speed": ["mean", "max"],
                    "clouds_percentage": "mean",
                    "visibility": "mean",
                }
            )
            .round(2)
        )

        # Flatten column names
        daily.columns = ["_".join(col).strip() for col in daily.columns.values]
        daily = daily.reset_index()

        # Convert date to string for JSON serialization
        daily["date"] = daily["date"].astype(str)

        return cast(list[dict[str, Any]], daily.to_dict(orient="records"))

    def _city_statistics(self, df: pd.DataFrame) -> list[dict[str, Any]]:
        """Calculate overall statistics per city."""
        stats = (
            df.groupby(["city", "country"])
            .agg(
                {
                    "temperature_celsius": ["mean", "min", "max", "std", "count"],
                    "humidity": "mean",
                    "pressure": "mean",
                    "wind_speed": "mean",
                }
            )
            .round(2)
        )

        # Flatten column names
        stats.columns = ["_".join(col).strip() for col in stats.columns.values]
        stats = stats.reset_index()

        # Rename count column
        stats = stats.rename(columns={"temperature_celsius_count": "observation_count"})

        return cast(list[dict[str, Any]], stats.to_dict(orient="records"))

    def transform_and_store(
        self,
        silver_keys: list[str],
        output_filename: str | None = None,
        persist_to_database: bool = True,
    ) -> dict[str, Any]:
        """Transform Silver data from storage and write to Gold layer.

        Args:
            silver_keys: List of S3 keys for Silver data files.
            output_filename: Optional output filename.
            persist_to_database: Whether to persist to PostgreSQL serving layer.

        Returns:
            Dictionary with storage keys and database status.
        """
        # Read all Silver data
        all_silver_data: list[dict[str, Any]] = []

        for key in silver_keys:
            try:
                data = self.storage.read_json(key)
                if isinstance(data, list):
                    all_silver_data.extend(data)
                else:
                    all_silver_data.append(data)
            except Exception as e:
                logger.warning(f"Failed to read {key}: {e}")

        if not all_silver_data:
            logger.warning("No Silver data found to transform")
            return {"status": "no_data"}

        # Transform
        gold_data = self.transform(all_silver_data)

        # Generate filename
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        base_filename = output_filename or f"weather_{timestamp}"

        result: dict[str, Any] = {"status": "success", "keys": {}}

        # Write records to Gold layer
        records_key = self.storage.write_json(
            gold_data["records"],
            "gold",
            f"{base_filename}_records",
        )
        result["keys"]["records"] = records_key

        # Write daily aggregates
        if gold_data["daily_aggregates"]:
            daily_key = self.storage.write_json(
                gold_data["daily_aggregates"],
                "gold",
                f"{base_filename}_daily",
            )
            result["keys"]["daily_aggregates"] = daily_key

        # Write city statistics
        if gold_data["city_statistics"]:
            stats_key = self.storage.write_json(
                gold_data["city_statistics"],
                "gold",
                f"{base_filename}_stats",
            )
            result["keys"]["city_statistics"] = stats_key

        # Persist to PostgreSQL serving layer
        if persist_to_database and self.database:
            try:
                inserted = self.database.insert_weather_data(gold_data["records"])
                result["database"] = {
                    "status": "success",
                    "records_inserted": inserted,
                }
            except Exception as e:
                logger.error(f"Failed to persist to database: {e}")
                result["database"] = {
                    "status": "error",
                    "error": str(e),
                }

        result["metadata"] = gold_data["metadata"]
        return result

    def generate_analytics_view(
        self,
        silver_data: list[dict[str, Any]],
    ) -> pd.DataFrame:
        """Generate an analytics-ready DataFrame.

        Useful for dashboard visualizations and ad-hoc analysis.

        Args:
            silver_data: List of Silver layer records.

        Returns:
            pandas DataFrame with analytics features.
        """
        df = pd.DataFrame(silver_data)
        df["timestamp"] = pd.to_datetime(df["timestamp"])

        # Time-based features
        df["hour"] = df["timestamp"].dt.hour
        df["day_of_week"] = df["timestamp"].dt.day_name()
        df["is_daytime"] = df["hour"].between(6, 18)

        # Weather features
        df["is_comfortable"] = df["temperature_celsius"].between(18, 26) & df["humidity"].between(
            30, 60
        )

        df["heat_index"] = self._calculate_heat_index(
            df["temperature_celsius"],
            df["humidity"],
        )

        return df

    def _calculate_heat_index(
        self,
        temp: pd.Series,
        humidity: pd.Series,
    ) -> pd.Series:
        """Calculate heat index (feels-like temperature).

        Uses simplified Rothfusz regression equation.
        """
        # Convert to Fahrenheit for the formula
        temp_f = temp * 9 / 5 + 32

        # Simple heat index (valid for temps > 80°F and humidity > 40%)
        hi = 0.5 * (temp_f + 61.0 + ((temp_f - 68.0) * 1.2) + (humidity * 0.094))

        # Use actual temp when conditions don't warrant heat index
        result = pd.Series(index=temp.index, dtype=float)
        use_hi = (temp_f >= 80) & (humidity >= 40)
        result[use_hi] = hi[use_hi]
        result[~use_hi] = temp_f[~use_hi]

        # Convert back to Celsius
        return (result - 32) * 5 / 9
