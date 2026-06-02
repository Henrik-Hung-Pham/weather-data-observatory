"""Unit tests for Data Lake storage."""

import json
from datetime import date, datetime, timezone
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from data_pipeline.storage.datalake import DataLakeError, DataLakeStorage


def _make_storage(mock_s3_client):
    """Construct a DataLakeStorage instance without calling ``__init__``.

    Mirrors the bypass idiom used in ``tests/unit/test_transformations.py`` to
    avoid touching ``Settings``/``boto3`` during unit tests.
    """
    storage = DataLakeStorage.__new__(DataLakeStorage)
    storage.bucket_name = "test-bucket"
    storage.endpoint_url = "http://localhost:4566"
    storage.bronze_path = "bronze/weather"
    storage.silver_path = "silver/weather"
    storage.gold_path = "gold/weather"
    storage.s3_client = mock_s3_client
    return storage


class TestPartitionPath:
    """Tests for ``_get_partition_path``."""

    @pytest.mark.unit
    def test_bronze_partition_path(self, mock_s3_client):
        storage = _make_storage(mock_s3_client)
        ts = datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc)

        path = storage._get_partition_path("bronze", ts)

        assert path == "bronze/weather/2024/01/15/"

    @pytest.mark.unit
    def test_silver_partition_path(self, mock_s3_client):
        storage = _make_storage(mock_s3_client)
        ts = datetime(2024, 7, 4, tzinfo=timezone.utc)

        path = storage._get_partition_path("silver", ts)

        assert path == "silver/weather/2024/07/04/"

    @pytest.mark.unit
    def test_gold_partition_path(self, mock_s3_client):
        storage = _make_storage(mock_s3_client)
        ts = datetime(2023, 12, 31, tzinfo=timezone.utc)

        path = storage._get_partition_path("gold", ts)

        assert path == "gold/weather/2023/12/31/"

    @pytest.mark.unit
    def test_unknown_layer_uses_layer_name(self, mock_s3_client):
        storage = _make_storage(mock_s3_client)
        ts = datetime(2024, 2, 9, tzinfo=timezone.utc)

        path = storage._get_partition_path("platinum", ts)

        assert path == "platinum/2024/02/09/"

    @pytest.mark.unit
    def test_no_timestamp_uses_now(self, mock_s3_client):
        """When no timestamp is provided, the current UTC date is used."""
        storage = _make_storage(mock_s3_client)

        path = storage._get_partition_path("bronze")
        now = datetime.now(timezone.utc)
        expected_prefix = (
            f"bronze/weather/{now.year}/{now.month:02d}/{now.day:02d}/"
        )

        assert path == expected_prefix


class TestExtractDateFromKey:
    """Tests for ``_extract_date_from_key``."""

    @pytest.mark.unit
    def test_extracts_date_from_partitioned_key(self, mock_s3_client):
        storage = _make_storage(mock_s3_client)

        result = storage._extract_date_from_key(
            "bronze/weather/2024/01/15/file.json"
        )

        assert result == date(2024, 1, 15)

    @pytest.mark.unit
    def test_extracts_date_from_silver_key(self, mock_s3_client):
        storage = _make_storage(mock_s3_client)

        result = storage._extract_date_from_key(
            "silver/weather/2023/12/31/payload.json"
        )

        assert result == date(2023, 12, 31)

    @pytest.mark.unit
    def test_returns_none_without_date_partition(self, mock_s3_client):
        storage = _make_storage(mock_s3_client)

        assert storage._extract_date_from_key("bronze/weather/file.json") is None

    @pytest.mark.unit
    def test_returns_none_on_invalid_date(self, mock_s3_client):
        storage = _make_storage(mock_s3_client)

        # 2024/13/40 is not a valid date
        assert (
            storage._extract_date_from_key("bronze/weather/2024/13/40/file.json")
            is None
        )


class TestWriteJson:
    """Tests for ``write_json``."""

    @pytest.mark.unit
    def test_write_json_happy_path(self, mock_s3_client, sample_bronze_data):
        storage = _make_storage(mock_s3_client)
        ts = datetime(2024, 1, 30, tzinfo=timezone.utc)

        key = storage.write_json(sample_bronze_data, "bronze", "weather_batch", ts)

        assert key == "bronze/weather/2024/01/30/weather_batch.json"

        mock_s3_client.put_object.assert_called_once()
        kwargs = mock_s3_client.put_object.call_args.kwargs
        assert kwargs["Bucket"] == "test-bucket"
        assert kwargs["Key"] == key
        assert kwargs["ContentType"] == "application/json"
        # Body is bytes containing valid JSON of the data we passed in
        decoded = json.loads(kwargs["Body"].decode("utf-8"))
        assert decoded == sample_bronze_data

    @pytest.mark.unit
    def test_write_json_raises_datalake_error_on_client_error(
        self, mock_s3_client, sample_bronze_data
    ):
        storage = _make_storage(mock_s3_client)
        mock_s3_client.put_object.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "nope"}},
            "PutObject",
        )

        with pytest.raises(DataLakeError, match="Failed to write to S3"):
            storage.write_json(sample_bronze_data, "bronze", "weather")


class TestReadJson:
    """Tests for ``read_json``."""

    @pytest.mark.unit
    def test_read_json_happy_path(self, mock_s3_client, sample_bronze_data):
        storage = _make_storage(mock_s3_client)
        body_bytes = json.dumps(sample_bronze_data).encode("utf-8")

        body_stream = MagicMock()
        body_stream.read.return_value = body_bytes
        mock_s3_client.get_object.return_value = {"Body": body_stream}

        result = storage.read_json("bronze/weather/2024/01/30/weather_batch.json")

        assert result == sample_bronze_data
        mock_s3_client.get_object.assert_called_once_with(
            Bucket="test-bucket",
            Key="bronze/weather/2024/01/30/weather_batch.json",
        )

    @pytest.mark.unit
    def test_read_json_raises_for_missing_key(self, mock_s3_client):
        storage = _make_storage(mock_s3_client)
        mock_s3_client.get_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "missing"}},
            "GetObject",
        )

        with pytest.raises(DataLakeError, match="Object not found"):
            storage.read_json("bronze/weather/missing.json")


class TestEnsureBucketExists:
    """Tests for ``ensure_bucket_exists``."""

    @pytest.mark.unit
    def test_returns_true_when_bucket_exists(self, mock_s3_client):
        storage = _make_storage(mock_s3_client)
        mock_s3_client.head_bucket.return_value = {}

        assert storage.ensure_bucket_exists() is True

        mock_s3_client.head_bucket.assert_called_once_with(Bucket="test-bucket")
        mock_s3_client.create_bucket.assert_not_called()

    @pytest.mark.unit
    def test_creates_bucket_when_missing(self, mock_s3_client):
        storage = _make_storage(mock_s3_client)
        mock_s3_client.head_bucket.side_effect = ClientError(
            {"Error": {"Code": "404", "Message": "Not Found"}},
            "HeadBucket",
        )
        mock_s3_client.create_bucket.return_value = {}

        assert storage.ensure_bucket_exists() is True

        mock_s3_client.create_bucket.assert_called_once_with(Bucket="test-bucket")

    @pytest.mark.unit
    def test_returns_false_on_create_failure(self, mock_s3_client):
        storage = _make_storage(mock_s3_client)
        mock_s3_client.head_bucket.side_effect = ClientError(
            {"Error": {"Code": "NoSuchBucket", "Message": "Not Found"}},
            "HeadBucket",
        )
        mock_s3_client.create_bucket.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "nope"}},
            "CreateBucket",
        )

        assert storage.ensure_bucket_exists() is False

    @pytest.mark.unit
    def test_returns_false_on_other_head_error(self, mock_s3_client):
        storage = _make_storage(mock_s3_client)
        mock_s3_client.head_bucket.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "denied"}},
            "HeadBucket",
        )

        assert storage.ensure_bucket_exists() is False
        mock_s3_client.create_bucket.assert_not_called()
