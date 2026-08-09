"""Unit tests for data transformations."""

import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from data_pipeline.transformation.silver import SilverTransformer
from data_pipeline.transformation.gold import GoldTransformer


def _valid_bronze_record(**overrides):
    """A bronze record that passes Silver cleaning + range validation."""
    record = {
        "city": "London",
        "country": "GB",
        "temperature_celsius": 12.0,
        "feels_like_celsius": 11.0,
        "humidity": 65,
        "pressure": 1013,
        "wind_speed": 5.0,
        "wind_direction": 180,
        "weather_condition": "Clear",
        "weather_description": "clear sky",
        "clouds_percentage": 10,
        "visibility": 10000,
        "timestamp": "2024-01-30T12:00:00+00:00",
        "sunrise": "2024-01-30T07:00:00+00:00",
        "sunset": "2024-01-30T17:00:00+00:00",
    }
    record.update(overrides)
    return record


class TestSilverTransformer:
    """Tests for Silver layer transformer."""

    @pytest.fixture
    def transformer(self, mock_s3_client):
        """Create transformer with mocked storage."""
        from data_pipeline.storage.datalake import DataLakeStorage
        from unittest.mock import patch
        
        with patch.object(DataLakeStorage, '__init__', lambda self, *args, **kwargs: None):
            with patch.object(DataLakeStorage, '_create_client', return_value=mock_s3_client):
                t = SilverTransformer.__new__(SilverTransformer)
                t.storage = None
                return t

    @pytest.mark.unit
    def test_transform_success(self, sample_bronze_data):
        """Test successful transformation."""
        transformer = SilverTransformer.__new__(SilverTransformer)
        transformer.storage = None
        
        result = transformer.transform(sample_bronze_data)
        
        assert len(result) == 2
        assert result[0]["city"] == "London"
        assert result[0]["_source_layer"] == "bronze"
        assert "_transformed_at" in result[0]

    @pytest.mark.unit
    def test_transform_empty_data(self):
        """Test transformation with empty data."""
        transformer = SilverTransformer.__new__(SilverTransformer)
        transformer.storage = None
        
        result = transformer.transform([])
        
        assert result == []

    @pytest.mark.unit
    def test_clean_string(self):
        """Test string cleaning. Absence stays absent (None), never ""."""
        transformer = SilverTransformer.__new__(SilverTransformer)
        transformer.storage = None

        assert transformer._clean_string("  London  ") == "London"
        assert transformer._clean_string(123) == "123"
        assert transformer._clean_string(None) is None
        # Whitespace-only is absence, not an empty reading.
        assert transformer._clean_string("   ") is None

    @pytest.mark.unit
    def test_safe_float(self):
        """Test safe float conversion. A missing value is None, not 0.0."""
        transformer = SilverTransformer.__new__(SilverTransformer)
        transformer.storage = None

        assert transformer._safe_float(12.5) == 12.5
        assert transformer._safe_float("12.5") == 12.5
        # 0.0 would be a fabricated reading indistinguishable from a real one.
        assert transformer._safe_float(None) is None
        assert transformer._safe_float("invalid") is None
        # A genuine zero is preserved and must not be confused with absence.
        assert transformer._safe_float(0) == 0.0

    @pytest.mark.unit
    def test_safe_int(self):
        """Test safe int conversion. A missing value is None, not 0."""
        transformer = SilverTransformer.__new__(SilverTransformer)
        transformer.storage = None

        assert transformer._safe_int(12) == 12
        assert transformer._safe_int(12.9) == 12
        assert transformer._safe_int("12") == 12
        assert transformer._safe_int(None) is None
        assert transformer._safe_int("invalid") is None
        assert transformer._safe_int(0) == 0

    @pytest.mark.unit
    def test_validate_record_valid(self):
        """Test validation of a valid record."""
        transformer = SilverTransformer.__new__(SilverTransformer)
        transformer.storage = None

        valid_record = {
            "city": "London",
            "country": "GB",
            "timestamp": "2024-01-30T12:00:00+00:00",
            "temperature_celsius": 12.0,
            "humidity": 65,
            "pressure": 1013,
            "wind_speed": 5.5,
            "clouds_percentage": 10,
            "visibility": 10000,
        }

        assert transformer._validate_record(valid_record) is True
        assert transformer._rejection_reason(valid_record) is None

    @pytest.mark.unit
    def test_validate_record_missing_city(self):
        """Test validation fails without city."""
        transformer = SilverTransformer.__new__(SilverTransformer)
        transformer.storage = None

        invalid_record = {
            "city": None,
            "temperature_celsius": 12.0,
        }

        assert transformer._validate_record(invalid_record) is False

    @pytest.mark.unit
    @pytest.mark.parametrize("field", ["city", "country", "temperature_celsius", "timestamp"])
    def test_validate_record_rejects_null_required_field(self, field):
        """Every required field is rejected when null, and named in the reason."""
        transformer = SilverTransformer.__new__(SilverTransformer)
        transformer.storage = None

        record: dict[str, object] = {
            "city": "London",
            "country": "GB",
            "timestamp": "2024-01-30T12:00:00+00:00",
            "temperature_celsius": 12.0,
        }
        record[field] = None

        reason = transformer._rejection_reason(record)
        assert reason is not None
        assert field in reason

    @pytest.mark.unit
    def test_validate_record_out_of_range(self):
        """Test validation fails with out-of-range values."""
        transformer = SilverTransformer.__new__(SilverTransformer)
        transformer.storage = None
        
        invalid_record = {
            "city": "London",
            "temperature_celsius": 200.0,  # Out of range
            "humidity": 65,
        }
        
        assert transformer._validate_record(invalid_record) is False

    @pytest.mark.unit
    def test_normalize_timestamp(self):
        """Test timestamp normalization."""
        transformer = SilverTransformer.__new__(SilverTransformer)
        transformer.storage = None

        # ISO string
        result = transformer._normalize_timestamp("2024-01-30T12:00:00+00:00")
        assert "2024-01-30" in result

        # Datetime object
        dt = datetime(2024, 1, 30, 12, 0, tzinfo=timezone.utc)
        result = transformer._normalize_timestamp(dt)
        assert "2024-01-30" in result

        # Unix timestamp
        result = transformer._normalize_timestamp(1706619600)
        assert "2024" in result

    @pytest.mark.unit
    def test_normalize_timestamp_absent_is_none(self):
        """A missing timestamp must not be stamped with the run time.

        Defaulting to now() backdates unknown data to whenever the pipeline
        happened to run, which silently defeats the freshness check.
        """
        transformer = SilverTransformer.__new__(SilverTransformer)
        transformer.storage = None

        assert transformer._normalize_timestamp(None) is None
        assert transformer._normalize_timestamp("not-a-timestamp") is None

    @pytest.mark.unit
    def test_null_temperature_is_quarantined_not_zeroed(self):
        """A null temperature must be quarantined, never persisted as 0.0."""
        transformer = SilverTransformer.__new__(SilverTransformer)
        transformer.storage = None

        bronze = [
            {
                "city": "London",
                "country": "GB",
                "temperature_celsius": None,
                "timestamp": "2024-01-30T12:00:00+00:00",
            }
        ]

        result = transformer.transform(bronze)

        assert result == []

    @pytest.mark.unit
    def test_optional_nulls_survive_as_none(self):
        """Optional fields keep None so they land as SQL NULL, not as zeros.

        A humidity of 0% and an unknown humidity are different facts; zeroing
        the second makes them indistinguishable downstream.
        """
        transformer = SilverTransformer.__new__(SilverTransformer)
        transformer.storage = None

        bronze = [
            {
                "city": "London",
                "country": "GB",
                "temperature_celsius": 12.0,
                "timestamp": "2024-01-30T12:00:00+00:00",
                "humidity": None,
                "visibility": None,
                "wind_speed": 0,  # a genuine zero, must be preserved
            }
        ]

        (record,) = transformer.transform(bronze)

        assert record["humidity"] is None
        assert record["visibility"] is None
        assert record["wind_speed"] == 0.0


class TestGoldTransformer:
    """Tests for Gold layer transformer."""

    @pytest.mark.unit
    def test_transform_success(self, sample_silver_data):
        """Test successful Gold transformation."""
        transformer = GoldTransformer.__new__(GoldTransformer)
        transformer.storage = None
        transformer.database = None
        
        result = transformer.transform(sample_silver_data)
        
        assert "records" in result
        assert "daily_aggregates" in result
        assert "city_statistics" in result
        assert "metadata" in result
        assert len(result["records"]) == 1

    @pytest.mark.unit
    def test_transform_empty_data(self):
        """Test transformation with empty data."""
        transformer = GoldTransformer.__new__(GoldTransformer)
        transformer.storage = None
        transformer.database = None
        
        result = transformer.transform([])
        
        assert result["records"] == []

    @pytest.mark.unit
    def test_metadata_generation(self, sample_silver_data):
        """Test metadata is correctly generated."""
        transformer = GoldTransformer.__new__(GoldTransformer)
        transformer.storage = None
        transformer.database = None
        
        result = transformer.transform(sample_silver_data)
        
        metadata = result["metadata"]
        assert "transformed_at" in metadata
        assert metadata["record_count"] == 1
        assert metadata["cities_count"] == 1

    @pytest.mark.unit
    def test_temperature_categorization(self, sample_silver_data):
        """Test temperature categories are assigned."""
        transformer = GoldTransformer.__new__(GoldTransformer)
        transformer.storage = None
        transformer.database = None
        
        result = transformer.transform(sample_silver_data)
        records = result["records"]
        
        # London at 12°C should be "mild"
        assert records[0]["temp_category"] == "mild"


class TestSilverQuarantine:
    """Self-healing: rejected records are quarantined, not dropped."""

    @pytest.mark.unit
    def test_invalid_records_are_quarantined(self):
        storage = MagicMock()
        storage.write_json.return_value = "quarantine/weather/2024/01/30/silver_rejects_x.json"
        transformer = SilverTransformer.__new__(SilverTransformer)
        transformer.storage = storage

        data = [
            _valid_bronze_record(),
            _valid_bronze_record(temperature_celsius=200.0),  # out of range -> rejected
        ]
        result = transformer.transform(data)

        # Pipeline self-heals: continues with the valid subset...
        assert len(result) == 1
        # ...and the bad record is written to the quarantine prefix.
        storage.write_json.assert_called_once()
        call = storage.write_json.call_args
        payload, layer = call.args[0], call.args[1]
        assert layer == "quarantine"
        assert len(payload) == 1
        assert payload[0]["reason"]
        assert payload[0]["source_layer"] == "silver"

    @pytest.mark.unit
    def test_no_quarantine_when_all_valid(self):
        storage = MagicMock()
        transformer = SilverTransformer.__new__(SilverTransformer)
        transformer.storage = storage

        result = transformer.transform([_valid_bronze_record(), _valid_bronze_record()])

        assert len(result) == 2
        storage.write_json.assert_not_called()

    @pytest.mark.unit
    def test_quarantine_disabled_skips_write(self):
        storage = MagicMock()
        transformer = SilverTransformer.__new__(SilverTransformer)
        transformer.storage = storage

        with patch(
            "data_pipeline.config.get_settings",
            return_value=MagicMock(quarantine_enabled=False),
        ):
            result = transformer.transform(
                [_valid_bronze_record(), _valid_bronze_record(temperature_celsius=200.0)]
            )

        assert len(result) == 1
        storage.write_json.assert_not_called()

    @pytest.mark.unit
    def test_quarantine_storage_failure_is_swallowed(self):
        storage = MagicMock()
        storage.write_json.side_effect = RuntimeError("s3 down")
        transformer = SilverTransformer.__new__(SilverTransformer)
        transformer.storage = storage

        # Must not raise even though quarantine write fails.
        result = transformer.transform(
            [_valid_bronze_record(), _valid_bronze_record(temperature_celsius=200.0)]
        )
        assert len(result) == 1

    @pytest.mark.unit
    def test_quarantine_no_storage_is_safe(self):
        transformer = SilverTransformer.__new__(SilverTransformer)
        transformer.storage = None

        result = transformer.transform(
            [_valid_bronze_record(), _valid_bronze_record(temperature_celsius=200.0)]
        )
        assert len(result) == 1
