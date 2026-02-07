"""
Minio S3-Compatible Storage Client

Handles uploading/downloading raw and processed data
to Minio with date-based partitioning.

Partition scheme: raw/gov_transactions/YYYY/MM/DD/
"""

import io
import json
import logging
import os
from datetime import datetime
from typing import Optional, List

logger = logging.getLogger(__name__)

# Default Minio configuration (overridable via env vars)
DEFAULT_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
DEFAULT_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
DEFAULT_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
DEFAULT_BUCKET = "real-estate-data"
DEFAULT_SECURE = False


class MinioStorageClient:
    """S3-compatible storage client for raw and processed data."""

    def __init__(
        self,
        endpoint: str = DEFAULT_ENDPOINT,
        access_key: str = DEFAULT_ACCESS_KEY,
        secret_key: str = DEFAULT_SECRET_KEY,
        bucket: str = DEFAULT_BUCKET,
        secure: bool = DEFAULT_SECURE,
    ):
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.bucket = bucket
        self.secure = secure
        self._client = None

    def _get_client(self):
        """Lazy-initialize Minio client."""
        if self._client is None:
            from minio import Minio
            self._client = Minio(
                self.endpoint,
                access_key=self.access_key,
                secret_key=self.secret_key,
                secure=self.secure,
            )
            # Ensure bucket exists
            if not self._client.bucket_exists(self.bucket):
                self._client.make_bucket(self.bucket)
                logger.info(f"Created bucket: {self.bucket}")
        return self._client

    def _date_partition(self, dt: Optional[datetime] = None) -> str:
        """Generate date partition path (YYYY/MM/DD)."""
        dt = dt or datetime.now()
        return f"{dt.year}/{dt.month:02d}/{dt.day:02d}"

    def upload_json(
        self,
        data: any,
        path: str,
        partition_by_date: bool = True,
    ) -> str:
        """
        Upload JSON data to Minio.

        Args:
            data: Data to serialize as JSON.
            path: Object path (e.g. "raw/gov_transactions/data.json").
            partition_by_date: Whether to add date partition to path.

        Returns:
            Full object path where data was stored.
        """
        client = self._get_client()

        if partition_by_date:
            date_part = self._date_partition()
            # Insert date partition before filename
            parts = path.rsplit('/', 1)
            if len(parts) == 2:
                path = f"{parts[0]}/{date_part}/{parts[1]}"
            else:
                path = f"{date_part}/{path}"

        json_bytes = json.dumps(data, ensure_ascii=False, default=str).encode('utf-8')
        data_stream = io.BytesIO(json_bytes)

        client.put_object(
            self.bucket,
            path,
            data_stream,
            length=len(json_bytes),
            content_type='application/json',
        )

        logger.info(f"Uploaded to {self.bucket}/{path} ({len(json_bytes)} bytes)")
        return f"{self.bucket}/{path}"

    def download_json(self, path: str) -> any:
        """
        Download and parse JSON data from Minio.

        Args:
            path: Object path in the bucket.

        Returns:
            Parsed JSON data.
        """
        client = self._get_client()
        try:
            response = client.get_object(self.bucket, path)
            data = json.loads(response.read().decode('utf-8'))
            response.close()
            response.release_conn()
            return data
        except Exception as e:
            logger.error(f"Failed to download {path}: {e}")
            raise

    def list_objects(
        self, prefix: str = "", recursive: bool = True
    ) -> List[str]:
        """
        List objects in the bucket with a given prefix.

        Args:
            prefix: Path prefix to filter.
            recursive: Whether to list recursively.

        Returns:
            List of object names.
        """
        client = self._get_client()
        objects = client.list_objects(
            self.bucket, prefix=prefix, recursive=recursive
        )
        return [obj.object_name for obj in objects]

    def upload_csv(
        self,
        csv_content: str,
        path: str,
        partition_by_date: bool = True,
    ) -> str:
        """
        Upload CSV string to Minio.

        Args:
            csv_content: CSV string content.
            path: Object path.
            partition_by_date: Whether to add date partition.

        Returns:
            Full object path.
        """
        client = self._get_client()

        if partition_by_date:
            date_part = self._date_partition()
            parts = path.rsplit('/', 1)
            if len(parts) == 2:
                path = f"{parts[0]}/{date_part}/{parts[1]}"
            else:
                path = f"{date_part}/{path}"

        csv_bytes = csv_content.encode('utf-8')
        data_stream = io.BytesIO(csv_bytes)

        client.put_object(
            self.bucket,
            path,
            data_stream,
            length=len(csv_bytes),
            content_type='text/csv',
        )

        logger.info(f"Uploaded CSV to {self.bucket}/{path} ({len(csv_bytes)} bytes)")
        return f"{self.bucket}/{path}"


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("MinioStorageClient ready. Use with docker-compose Minio.")
    print(f"Default endpoint: {DEFAULT_ENDPOINT}")
    print(f"Default bucket: {DEFAULT_BUCKET}")
