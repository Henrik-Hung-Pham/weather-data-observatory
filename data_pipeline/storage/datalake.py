"""Data Lake storage abstraction for S3-compatible storage.

Supports both AWS S3 and LocalStack for local development.
Implements medallion architecture (Bronze/Silver/Gold) partitioning.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from data_pipeline.config import get_settings

logger = logging.getLogger(__name__)


class DataLakeError(Exception):
    """Custom exception for Data Lake operations."""

    pass


class DataLakeStorage:
    """S3-compatible data lake storage with medallion architecture support.

    Handles data storage in Bronze/Silver/Gold layers with date-based partitioning.
    """

    def __init__(
        self,
        bucket_name: str | None = None,
        endpoint_url: str | None = None,
    ):
        """Initialize Data Lake storage.

        Args:
            bucket_name: S3 bucket name. Uses settings if not provided.
            endpoint_url: Custom endpoint URL (e.g., LocalStack).
        """
        settings = get_settings()
        self.bucket_name = bucket_name or settings.s3_bucket_name
        self.endpoint_url = endpoint_url or settings.aws_endpoint_url

        # Path configurations from settings
        self.bronze_path = settings.bronze_path
        self.silver_path = settings.silver_path
        self.gold_path = settings.gold_path

        # Initialize S3 client
        self.s3_client = self._create_client()

    def _create_client(self) -> Any:
        """Create S3 client with appropriate configuration."""
        settings = get_settings()

        config = Config(
            signature_version="s3v4",
            retries={"max_attempts": 3, "mode": "adaptive"},
        )

        return boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
            region_name=settings.aws_region,
            config=config,
        )

    def _get_partition_path(self, layer: str, timestamp: datetime | None = None) -> str:
        """Generate date-partitioned path for a layer.

        Args:
            layer: Data layer (bronze, silver, gold).
            timestamp: Timestamp for partitioning. Uses current time if not provided.

        Returns:
            Partitioned path string (e.g., 'bronze/weather/2024/01/15/').
        """
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)

        base_path = {
            "bronze": self.bronze_path,
            "silver": self.silver_path,
            "gold": self.gold_path,
        }.get(layer, layer)

        return f"{base_path}/{timestamp.year}/{timestamp.month:02d}/{timestamp.day:02d}/"

    def write_json(
        self,
        data: dict[str, Any] | list[dict[str, Any]],
        layer: str,
        filename: str,
        timestamp: datetime | None = None,
    ) -> str:
        """Write JSON data to the data lake.

        Args:
            data: Data to write (dict or list of dicts).
            layer: Target layer (bronze, silver, gold).
            filename: File name without extension.
            timestamp: Timestamp for partitioning.

        Returns:
            S3 key of the written object.

        Raises:
            DataLakeError: If write operation fails.
        """
        partition_path = self._get_partition_path(layer, timestamp)
        key = f"{partition_path}{filename}.json"

        logger.info(f"Writing to {layer} layer: s3://{self.bucket_name}/{key}")

        try:
            body = json.dumps(data, indent=2, default=str)
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=body.encode("utf-8"),
                ContentType="application/json",
            )
            logger.debug(f"Successfully wrote {len(body)} bytes to {key}")
            return key

        except ClientError as e:
            error_msg = f"Failed to write to S3: {e}"
            logger.error(error_msg)
            raise DataLakeError(error_msg) from e

    def read_json(self, key: str) -> dict[str, Any] | list[dict[str, Any]]:
        """Read JSON data from the data lake.

        Args:
            key: S3 key of the object.

        Returns:
            Parsed JSON data.

        Raises:
            DataLakeError: If read operation fails.
        """
        logger.debug(f"Reading from S3: s3://{self.bucket_name}/{key}")

        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=key,
            )
            body = response["Body"].read().decode("utf-8")
            data: dict[str, Any] | list[dict[str, Any]] = json.loads(body)
            return data

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "NoSuchKey":
                raise DataLakeError(f"Object not found: {key}") from e
            raise DataLakeError(f"Failed to read from S3: {e}") from e

    def list_objects(
        self,
        layer: str,
        prefix: str = "",
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> list[str]:
        """List objects in a data layer with optional date filtering.

        Args:
            layer: Data layer (bronze, silver, gold).
            prefix: Additional path prefix.
            start_date: Start date for filtering.
            end_date: End date for filtering.

        Returns:
            List of S3 keys.
        """
        base_path = {
            "bronze": self.bronze_path,
            "silver": self.silver_path,
            "gold": self.gold_path,
        }.get(layer, layer)

        full_prefix = f"{base_path}/{prefix}" if prefix else base_path

        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            keys: list[str] = []

            for page in paginator.paginate(
                Bucket=self.bucket_name,
                Prefix=full_prefix,
            ):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    # Apply date filtering if specified
                    if start_date or end_date:
                        # Extract date from partitioned path
                        obj_date = self._extract_date_from_key(key)
                        if obj_date:
                            if start_date and obj_date < start_date.date():
                                continue
                            if end_date and obj_date > end_date.date():
                                continue
                    keys.append(key)

            return keys

        except ClientError as e:
            logger.error(f"Failed to list objects: {e}")
            return []

    def _extract_date_from_key(self, key: str) -> Any:
        """Extract date from a partitioned S3 key.

        Args:
            key: S3 key with date partition (e.g., 'bronze/weather/2024/01/15/file.json').

        Returns:
            datetime.date object or None if parsing fails.
        """
        try:
            parts = key.split("/")
            # Find year/month/day pattern
            for i in range(len(parts) - 2):
                if parts[i].isdigit() and len(parts[i]) == 4:  # Year
                    year = int(parts[i])
                    month = int(parts[i + 1])
                    day = int(parts[i + 2])
                    from datetime import date

                    return date(year, month, day)
        except (ValueError, IndexError):
            pass
        return None

    def delete_object(self, key: str) -> bool:
        """Delete an object from the data lake.

        Args:
            key: S3 key of the object to delete.

        Returns:
            True if deletion was successful.
        """
        try:
            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=key,
            )
            logger.info(f"Deleted object: {key}")
            return True

        except ClientError as e:
            logger.error(f"Failed to delete object {key}: {e}")
            return False

    def ensure_bucket_exists(self) -> bool:
        """Ensure the S3 bucket exists, create if it doesn't.

        Returns:
            True if bucket exists or was created successfully.
        """
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.debug(f"Bucket {self.bucket_name} exists")
            return True
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code in ("404", "NoSuchBucket"):
                try:
                    self.s3_client.create_bucket(Bucket=self.bucket_name)
                    logger.info(f"Created bucket: {self.bucket_name}")
                    return True
                except ClientError as create_error:
                    logger.error(f"Failed to create bucket: {create_error}")
                    return False
            logger.error(f"Bucket check failed: {e}")
            return False
