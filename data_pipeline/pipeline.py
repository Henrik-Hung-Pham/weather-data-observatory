"""Main pipeline orchestrator for the Data Observatory.

Coordinates data ingestion, transformation, validation, and loading
across the medallion architecture (Bronze -> Silver -> Gold).
"""

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from data_pipeline.config import get_settings
from data_pipeline.ingestion import WeatherAPIClient
from data_pipeline.quality import QualityGate, QualityGateResult
from data_pipeline.quality.gates import (
    QualityGateBlocked,
    null_check_rule,
    range_check_rule,
    schema_drift_rule,
)
from data_pipeline.quality.validator import DataValidator
from data_pipeline.storage import DatabaseManager, DataLakeStorage
from data_pipeline.transformation import GoldTransformer, SilverTransformer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


@dataclass
class PipelineRunResult:
    """Result of a pipeline run."""

    run_id: UUID
    status: str  # running, success, failed, blocked
    started_at: datetime
    completed_at: datetime | None = None
    duration_seconds: float = 0

    # Processing stats
    cities_processed: int = 0
    records_ingested: int = 0
    records_transformed: int = 0
    records_loaded: int = 0

    # Quality gate results
    quality_results: list[QualityGateResult] = field(default_factory=list)
    quality_gate_passed: bool = True
    quality_gate_reason: str = ""

    # Error information
    error_message: str = ""
    error_traceback: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            "run_id": str(self.run_id),
            "status": self.status,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "duration_seconds": self.duration_seconds,
            "cities_processed": self.cities_processed,
            "records_ingested": self.records_ingested,
            "records_transformed": self.records_transformed,
            "records_loaded": self.records_loaded,
            "quality_gate_passed": self.quality_gate_passed,
            "quality_gate_reason": self.quality_gate_reason,
            "error_message": self.error_message,
        }


class DataPipeline:
    """Main data pipeline orchestrator.

    Coordinates the ETL process across medallion architecture layers
    with integrated data quality checks.
    """

    # Expected schema for quality gates.
    # Must match the keys produced by WeatherData.to_dict() in
    # data_pipeline/ingestion/weather_api.py. Keep in sync when fields change.
    BRONZE_SCHEMA = {
        "city",
        "country",
        "temperature_kelvin",
        "temperature_celsius",
        "feels_like_celsius",
        "humidity",
        "pressure",
        "wind_speed",
        "wind_direction",
        "weather_condition",
        "weather_description",
        "clouds_percentage",
        "visibility",
        "timestamp",
        "sunrise",
        "sunset",
        "ingested_at",
    }

    # Must match the keys produced by SilverTransformer._clean_record() in
    # data_pipeline/transformation/silver.py (including the underscore-prefixed
    # metadata fields _transformed_at and _source_layer).
    SILVER_SCHEMA = {
        "city",
        "country",
        "temperature_celsius",
        "feels_like_celsius",
        "humidity",
        "pressure",
        "wind_speed",
        "wind_direction",
        "weather_condition",
        "weather_description",
        "clouds_percentage",
        "visibility",
        "timestamp",
        "sunrise",
        "sunset",
        "_transformed_at",
        "_source_layer",
    }

    def __init__(
        self,
        api_client: WeatherAPIClient | None = None,
        storage: DataLakeStorage | None = None,
        database: DatabaseManager | None = None,
        validator: DataValidator | None = None,
    ):
        """Initialize the data pipeline.

        Args:
            api_client: Weather API client for data ingestion.
            storage: Data lake storage for Bronze/Silver/Gold layers.
            database: Database manager for serving layer.
            validator: Data validator using Great Expectations.
        """
        settings = get_settings()

        self.settings = settings
        self.api_client = api_client or WeatherAPIClient()
        self.storage = storage or DataLakeStorage()
        self.database = database or DatabaseManager()
        self.validator = validator or DataValidator()

        # Transformers
        self.silver_transformer = SilverTransformer(self.storage)
        self.gold_transformer = GoldTransformer(self.storage, self.database)

        # Current run tracking
        self.run_id = uuid4()
        self._current_result: PipelineRunResult | None = None

    def run(self, cities: list[str] | None = None) -> PipelineRunResult:
        """Execute the full data pipeline.

        Args:
            cities: List of cities to fetch weather for. Uses settings if not provided.

        Returns:
            PipelineRunResult with execution details.
        """
        start_time = time.time()
        self.run_id = uuid4()
        cities = cities or self.settings.cities_list

        result = PipelineRunResult(
            run_id=self.run_id,
            status="running",
            started_at=datetime.now(timezone.utc),
        )
        self._current_result = result

        logger.info(f"🚀 Starting pipeline run {self.run_id}")
        logger.info(f"   Cities: {', '.join(cities)}")

        try:
            # Phase 1: Ingest to Bronze
            bronze_data = self._ingest_to_bronze(cities)
            result.records_ingested = len(bronze_data)
            result.cities_processed = len({r.get("city", "") for r in bronze_data})

            if not bronze_data:
                raise ValueError("No data ingested from API")

            # Phase 2: Quality check Bronze
            bronze_gate_result = self._validate_bronze(bronze_data)
            result.quality_results.append(bronze_gate_result)

            if bronze_gate_result.blocked:
                raise QualityGateBlocked(bronze_gate_result)

            # Phase 3: Transform to Silver
            silver_data = self._transform_to_silver(bronze_data)
            result.records_transformed = len(silver_data)

            # Phase 4: Quality check Silver
            silver_gate_result = self._validate_silver(silver_data)
            result.quality_results.append(silver_gate_result)

            if silver_gate_result.blocked:
                raise QualityGateBlocked(silver_gate_result)

            # Phase 5: Transform to Gold and load to serving layer
            gold_result = self._transform_to_gold(silver_data)
            result.records_loaded = gold_result.get("metadata", {}).get("record_count", 0)

            # Phase 6: Quality check Gold
            gold_data = gold_result.get("records", [])
            gold_gate_result = self._validate_gold(gold_data)
            result.quality_results.append(gold_gate_result)

            if gold_gate_result.blocked:
                raise QualityGateBlocked(gold_gate_result)

            # Success!
            result.status = "success"
            result.quality_gate_passed = True
            logger.info(f"✅ Pipeline run {self.run_id} completed successfully")

        except QualityGateBlocked as e:
            result.status = "blocked"
            result.quality_gate_passed = False
            result.quality_gate_reason = str(e)
            logger.error(f"🛑 Pipeline blocked by quality gate: {e}")

        except Exception as e:
            result.status = "failed"
            result.error_message = str(e)
            logger.error(f"❌ Pipeline failed: {e}", exc_info=True)

        finally:
            result.completed_at = datetime.now(timezone.utc)
            result.duration_seconds = time.time() - start_time

        # Store run result
        self._persist_run_result(result)

        logger.info(
            f"📊 Pipeline Summary:\n"
            f"   Status: {result.status}\n"
            f"   Duration: {result.duration_seconds:.2f}s\n"
            f"   Records: {result.records_ingested} → {result.records_transformed} → {result.records_loaded}\n"
            f"   Quality Gate: {'PASSED' if result.quality_gate_passed else 'FAILED'}"
        )

        return result

    def _ingest_to_bronze(self, cities: list[str]) -> list[dict[str, Any]]:
        """Ingest weather data from API to Bronze layer.

        Args:
            cities: List of cities to fetch.

        Returns:
            List of raw weather records.
        """
        logger.info("📥 Phase 1: Ingesting data to Bronze layer")

        weather_data = self.api_client.fetch_multiple_cities(cities)

        # Convert to dictionaries
        bronze_data = [w.to_dict() for w in weather_data]

        # Store in Bronze layer
        timestamp = datetime.now(timezone.utc)
        filename = f"weather_batch_{timestamp.strftime('%Y%m%d_%H%M%S')}"
        self.storage.write_json(bronze_data, "bronze", filename, timestamp)

        logger.info(f"   Ingested {len(bronze_data)} records to Bronze layer")
        return bronze_data

    def _validate_bronze(self, data: list[dict[str, Any]]) -> QualityGateResult:
        """Validate Bronze layer data.

        Args:
            data: Bronze layer records.

        Returns:
            Quality gate result.
        """
        logger.info("🔍 Validating Bronze layer")

        # Great Expectations validation
        ge_results = self.validator.validate(data, "bronze_weather_suite")

        # Quality gate with custom rules
        gate = QualityGate("bronze_quality_gate", mode=self.settings.quality_gate_mode)
        gate.add_rule(schema_drift_rule(self.BRONZE_SCHEMA))
        gate.add_rule(null_check_rule(["city", "timestamp"]))

        return gate.evaluate(data, "bronze", ge_results)

    def _transform_to_silver(self, bronze_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Transform Bronze data to Silver layer.

        Args:
            bronze_data: Raw Bronze layer records.

        Returns:
            Cleaned Silver layer records.
        """
        logger.info("⚙️ Phase 3: Transforming to Silver layer")

        silver_data = self.silver_transformer.transform(bronze_data)

        # Store in Silver layer
        timestamp = datetime.now(timezone.utc)
        filename = f"weather_cleaned_{timestamp.strftime('%Y%m%d_%H%M%S')}"
        self.storage.write_json(silver_data, "silver", filename, timestamp)

        logger.info(f"   Transformed {len(silver_data)} records to Silver layer")
        return silver_data

    def _validate_silver(self, data: list[dict[str, Any]]) -> QualityGateResult:
        """Validate Silver layer data.

        Args:
            data: Silver layer records.

        Returns:
            Quality gate result.
        """
        logger.info("🔍 Validating Silver layer")

        # Great Expectations validation
        ge_results = self.validator.validate(data, "silver_weather_suite")

        # Quality gate with custom rules
        gate = QualityGate("silver_quality_gate", mode=self.settings.quality_gate_mode)
        gate.add_rule(schema_drift_rule(self.SILVER_SCHEMA))
        gate.add_rule(null_check_rule(["city", "temperature_celsius", "country"]))
        gate.add_rule(range_check_rule("temperature_celsius", -100, 100))
        gate.add_rule(range_check_rule("humidity", 0, 100))

        return gate.evaluate(data, "silver", ge_results)

    def _transform_to_gold(self, silver_data: list[dict[str, Any]]) -> dict[str, Any]:
        """Transform Silver data to Gold layer and load to serving layer.

        Args:
            silver_data: Cleaned Silver layer records.

        Returns:
            Gold transformation result with metadata.
        """
        logger.info("⚙️ Phase 5: Transforming to Gold layer")

        gold_result = self.gold_transformer.transform(silver_data)

        # Store records in Gold layer
        timestamp = datetime.now(timezone.utc)
        filename = f"weather_gold_{timestamp.strftime('%Y%m%d_%H%M%S')}"
        self.storage.write_json(gold_result["records"], "gold", filename, timestamp)

        # Persist to PostgreSQL serving layer
        try:
            inserted = self.database.insert_weather_data(gold_result["records"])
            logger.info(f"   Loaded {inserted} records to serving layer (PostgreSQL)")
        except Exception as e:
            logger.warning(f"   Failed to load to PostgreSQL: {e}")

        logger.info(f"   Created {len(gold_result['records'])} Gold layer records")
        return gold_result

    def _validate_gold(self, data: list[dict[str, Any]]) -> QualityGateResult:
        """Validate Gold layer data.

        Args:
            data: Gold layer records.

        Returns:
            Quality gate result.
        """
        logger.info("🔍 Validating Gold layer")

        # Great Expectations validation
        ge_results = self.validator.validate(data, "gold_weather_suite")

        # Quality gate
        gate = QualityGate("gold_quality_gate", mode=self.settings.quality_gate_mode)
        gate.add_rule(null_check_rule(["city", "temperature_celsius", "timestamp"]))

        return gate.evaluate(data, "gold", ge_results)

    def _persist_run_result(self, result: PipelineRunResult) -> None:
        """Persist pipeline run result to storage and database.

        Args:
            result: Pipeline run result to persist.
        """
        # Store to S3
        try:
            self.storage.write_json(
                result.to_dict(),
                "gold",
                f"pipeline_run_{result.run_id}",
            )
        except Exception as e:
            logger.warning(f"Failed to persist run result to S3: {e}")

        # Store quality metrics to database
        for qr in result.quality_results:
            try:
                self._store_quality_metrics(qr)
            except Exception as e:
                logger.warning(f"Failed to persist quality metrics: {e}")

    def _store_quality_metrics(self, quality_result: QualityGateResult) -> None:
        """Store quality metrics to database.

        Args:
            quality_result: Quality gate result to store.
        """
        # This would insert into data_quality_metrics table
        # Implementation depends on exact schema needs
        pass


def main() -> None:
    """Main entry point for pipeline execution."""
    import sys

    try:
        pipeline = DataPipeline()
        result = pipeline.run()

        if result.status == "success":
            sys.exit(0)
        elif result.status == "blocked":
            sys.exit(2)  # Quality gate blocked
        else:
            sys.exit(1)  # Failed

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
