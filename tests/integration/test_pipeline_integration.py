"""Integration tests for the full pipeline."""

from unittest.mock import MagicMock, patch

import pytest


@pytest.mark.integration
class TestPipelineIntegration:
    """Integration tests for the data pipeline."""

    @pytest.fixture
    def mock_components(self, sample_api_response, mock_s3_client):
        """Set up all mocked components."""
        # Mock API response
        api_response = MagicMock()
        api_response.ok = True
        api_response.status_code = 200
        api_response.json.return_value = sample_api_response

        # Mock session
        mock_session = MagicMock()
        mock_session.get.return_value = api_response

        with (
            patch("requests.Session", return_value=mock_session),
            patch("boto3.client", return_value=mock_s3_client),
            patch("sqlalchemy.create_engine"),
        ):
            yield

    @pytest.mark.integration
    def test_pipeline_bronze_to_silver(
        self,
        mock_components,
        sample_bronze_data,
    ):
        """Test Bronze to Silver transformation flow."""
        from data_pipeline.transformation.silver import SilverTransformer

        transformer = SilverTransformer.__new__(SilverTransformer)
        transformer.storage = None

        silver_data = transformer.transform(sample_bronze_data)

        assert len(silver_data) == len(sample_bronze_data)
        assert all("_transformed_at" in record for record in silver_data)
        assert all("_source_layer" in record for record in silver_data)

    @pytest.mark.integration
    def test_pipeline_silver_to_gold(
        self,
        mock_components,
        sample_silver_data,
    ):
        """Test Silver to Gold transformation flow."""
        from data_pipeline.transformation.gold import GoldTransformer

        transformer = GoldTransformer.__new__(GoldTransformer)
        transformer.storage = None
        transformer.database = None

        gold_result = transformer.transform(sample_silver_data)

        assert "records" in gold_result
        assert "daily_aggregates" in gold_result
        assert "metadata" in gold_result

    @pytest.mark.integration
    def test_full_etl_flow(
        self,
        mock_components,
        sample_bronze_data,
    ):
        """Test complete ETL flow from Bronze to Gold."""
        from data_pipeline.transformation.gold import GoldTransformer
        from data_pipeline.transformation.silver import SilverTransformer

        # Bronze -> Silver
        silver_transformer = SilverTransformer.__new__(SilverTransformer)
        silver_transformer.storage = None
        silver_data = silver_transformer.transform(sample_bronze_data)

        # Silver -> Gold
        gold_transformer = GoldTransformer.__new__(GoldTransformer)
        gold_transformer.storage = None
        gold_transformer.database = None
        gold_result = gold_transformer.transform(silver_data)

        # Verify data integrity
        assert gold_result["metadata"]["record_count"] == len(sample_bronze_data)
        assert gold_result["metadata"]["cities_count"] == len(
            {r["city"] for r in sample_bronze_data}
        )

    @pytest.mark.integration
    def test_quality_gates_integration(
        self,
        mock_components,
        sample_bronze_data,
    ):
        """Test quality gates work with real data flow."""
        from data_pipeline.quality.gates import QualityGate, schema_drift_rule

        expected_schema = {"city", "country", "temperature_celsius", "humidity"}

        gate = QualityGate("integration_test_gate", mode="warn")
        gate.add_rule(schema_drift_rule(expected_schema))

        result = gate.evaluate(sample_bronze_data, "bronze")

        # Should pass since expected schema is subset of actual
        assert result.passed is True

    @pytest.mark.integration
    def test_data_validation_integration(
        self,
        mock_components,
        sample_silver_data,
        temp_expectation_suite,
    ):
        """Test Great Expectations validation integration."""
        from data_pipeline.quality.validator import DataValidator

        validator = DataValidator(suites_dir=temp_expectation_suite)
        result = validator.validate(sample_silver_data, "test_suite")

        assert "success" in result
        assert "results" in result
        assert "statistics" in result
