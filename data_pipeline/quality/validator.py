"""Great Expectations integration for data validation.

Provides a wrapper around Great Expectations for validating
data at each layer of the medallion architecture.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from great_expectations.core import ExpectationSuite
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.dataset import PandasDataset
from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler

logger = logging.getLogger(__name__)


class DataValidator:
    """Data validator using Great Expectations.

    Validates data against predefined expectation suites for each
    layer of the medallion architecture.
    """

    EXPECTATION_SUITES_DIR = Path("great_expectations/expectations")

    def __init__(self, suites_dir: Path | str | None = None):
        """Initialize data validator.

        Args:
            suites_dir: Directory containing expectation suite JSON files.
        """
        self.suites_dir = Path(suites_dir) if suites_dir else self.EXPECTATION_SUITES_DIR
        self._suites_cache: dict[str, ExpectationSuite] = {}

    def validate(
        self,
        data: list[dict[str, Any]] | pd.DataFrame,
        suite_name: str,
    ) -> dict[str, Any]:
        """Validate data against an expectation suite.

        Args:
            data: Data to validate (list of dicts or DataFrame).
            suite_name: Name of the expectation suite to use.

        Returns:
            Validation result dictionary with success status and details.
        """
        logger.info(f"Validating data with suite: {suite_name}")

        # Convert to DataFrame if necessary
        df = pd.DataFrame(data) if isinstance(data, list) else data

        if df.empty:
            logger.warning("Empty dataset provided for validation")
            return {
                "success": True,
                "results": [],
                "statistics": {
                    "evaluated_expectations": 0,
                    "successful_expectations": 0,
                    "unsuccessful_expectations": 0,
                },
            }

        # Load or create expectation suite
        suite = self._get_or_create_suite(suite_name)

        # Create Great Expectations dataset
        ge_df = PandasDataset(df)

        # Run validation
        results = ge_df.validate(expectation_suite=suite, result_format="COMPLETE")

        # Convert to dict for serialization
        return self._format_results(results)

    def _get_or_create_suite(self, suite_name: str) -> ExpectationSuite:
        """Get cached suite or load/create it.

        Args:
            suite_name: Name of the expectation suite.

        Returns:
            ExpectationSuite object.
        """
        if suite_name in self._suites_cache:
            return self._suites_cache[suite_name]

        suite_path = self.suites_dir / f"{suite_name}.json"

        if suite_path.exists():
            suite = self._load_suite(suite_path)
        else:
            suite = self._create_default_suite(suite_name)

        self._suites_cache[suite_name] = suite
        return suite

    def _load_suite(self, path: Path) -> ExpectationSuite:
        """Load expectation suite from JSON file.

        Args:
            path: Path to suite JSON file.

        Returns:
            Loaded ExpectationSuite.
        """
        logger.debug(f"Loading expectation suite from {path}")

        with open(path) as f:
            suite_dict = json.load(f)

        suite = ExpectationSuite(
            expectation_suite_name=suite_dict.get("expectation_suite_name", path.stem)
        )

        for exp_config in suite_dict.get("expectations", []):
            suite.add_expectation(
                ExpectationConfiguration(
                    expectation_type=exp_config["expectation_type"],
                    kwargs=exp_config.get("kwargs", {}),
                    meta=exp_config.get("meta", {}),
                )
            )

        return suite

    def _create_default_suite(self, suite_name: str) -> ExpectationSuite:
        """Create a default expectation suite based on layer.

        Args:
            suite_name: Name indicating the layer (bronze, silver, gold).

        Returns:
            Default ExpectationSuite for the layer.
        """
        suite = ExpectationSuite(expectation_suite_name=suite_name)

        if "bronze" in suite_name.lower():
            expectations = self._get_bronze_expectations()
        elif "silver" in suite_name.lower():
            expectations = self._get_silver_expectations()
        elif "gold" in suite_name.lower():
            expectations = self._get_gold_expectations()
        else:
            expectations = self._get_basic_expectations()

        for exp in expectations:
            suite.add_expectation(exp)

        return suite

    def _get_bronze_expectations(self) -> list[ExpectationConfiguration]:
        """Get expectations for Bronze layer (raw data)."""
        return [
            ExpectationConfiguration(
                expectation_type="expect_table_columns_to_match_set",
                kwargs={
                    "column_set": [
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
                    ],
                    "exact_match": False,
                },
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_to_exist",
                kwargs={"column": "city"},
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_to_exist",
                kwargs={"column": "timestamp"},
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "city"},
            ),
        ]

    def _get_silver_expectations(self) -> list[ExpectationConfiguration]:
        """Get expectations for Silver layer (cleaned data)."""
        return [
            # Schema expectations
            ExpectationConfiguration(
                expectation_type="expect_column_to_exist",
                kwargs={"column": "city"},
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_to_exist",
                kwargs={"column": "temperature_celsius"},
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_to_exist",
                kwargs={"column": "humidity"},
            ),
            # Null checks
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "city"},
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "temperature_celsius"},
            ),
            # Type expectations
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_of_type",
                kwargs={"column": "temperature_celsius", "type_": "float64"},
            ),
            # Range expectations
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": "temperature_celsius",
                    "min_value": -100,
                    "max_value": 100,
                },
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": "humidity",
                    "min_value": 0,
                    "max_value": 100,
                },
            ),
        ]

    def _get_gold_expectations(self) -> list[ExpectationConfiguration]:
        """Get expectations for Gold layer (aggregated data)."""
        return [
            # Required columns
            ExpectationConfiguration(
                expectation_type="expect_column_to_exist",
                kwargs={"column": "city"},
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_to_exist",
                kwargs={"column": "temperature_celsius"},
            ),
            # No nulls in key columns
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "city"},
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "temperature_celsius"},
            ),
            # Uniqueness (for certain aggregations)
            ExpectationConfiguration(
                expectation_type="expect_compound_columns_to_be_unique",
                kwargs={"column_list": ["city", "timestamp"]},
            ),
        ]

    def _get_basic_expectations(self) -> list[ExpectationConfiguration]:
        """Get basic expectations for any data."""
        return [
            ExpectationConfiguration(
                expectation_type="expect_table_row_count_to_be_between",
                kwargs={"min_value": 1},
            ),
        ]

    def _format_results(self, results: Any) -> dict[str, Any]:
        """Format Great Expectations results for storage/reporting.

        Args:
            results: Raw GE validation results.

        Returns:
            Formatted results dictionary.
        """
        return {
            "success": results.success,
            "results": [
                {
                    "success": r.success,
                    "expectation_config": {
                        "expectation_type": r.expectation_config.expectation_type,
                        "kwargs": dict(r.expectation_config.kwargs),
                    },
                    "result": dict(r.result) if hasattr(r, "result") else {},
                }
                for r in results.results
            ],
            "statistics": {
                "evaluated_expectations": results.statistics.get("evaluated_expectations", 0),
                "successful_expectations": results.statistics.get("successful_expectations", 0),
                "unsuccessful_expectations": results.statistics.get("unsuccessful_expectations", 0),
                "success_percent": results.statistics.get("success_percent", 0),
            },
            "meta": {
                "validation_time": datetime.now(timezone.utc).isoformat(),
            },
        }

    def save_suite(self, suite_name: str, suite: ExpectationSuite) -> Path:
        """Save expectation suite to JSON file.

        Args:
            suite_name: Name of the suite.
            suite: ExpectationSuite to save.

        Returns:
            Path to saved file.
        """
        self.suites_dir.mkdir(parents=True, exist_ok=True)
        path = self.suites_dir / f"{suite_name}.json"

        suite_dict = {
            "expectation_suite_name": suite.expectation_suite_name,
            "expectations": [
                {
                    "expectation_type": exp.expectation_type,
                    "kwargs": dict(exp.kwargs),
                    "meta": exp.meta,
                }
                for exp in suite.expectations
            ],
            "meta": {
                "created_at": datetime.now(timezone.utc).isoformat(),
            },
        }

        with open(path, "w") as f:
            json.dump(suite_dict, f, indent=2)

        logger.info(f"Saved expectation suite to {path}")
        return path

    def generate_suite_from_data(
        self,
        data: list[dict[str, Any]] | pd.DataFrame,
        suite_name: str,
    ) -> ExpectationSuite:
        """Generate expectations from sample data using profiling.

        Args:
            data: Sample data to profile.
            suite_name: Name for the generated suite.

        Returns:
            Generated ExpectationSuite.
        """
        df = pd.DataFrame(data) if isinstance(data, list) else data

        ge_df = PandasDataset(df)

        # Use built-in profiler
        suite, _ = ge_df.profile(profiler=BasicDatasetProfiler)
        suite.expectation_suite_name = suite_name

        self._suites_cache[suite_name] = suite
        return suite
