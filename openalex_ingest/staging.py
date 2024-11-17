import os
from pathlib import Path
from typing import Any, Protocol

import boto3
import pandas as pd
from dotenv import load_dotenv

from openalex_ingest.common import LOGGER

load_dotenv()


class DataDumper(Protocol):
    """Protocol for data dumpers."""

    def dump(self, data: list[dict], file_name: str, skip_exists: bool) -> None: ...

    def list_files(self) -> list[str]: ...


class LocalDumper:
    """Data dumper to local storage."""

    def __init__(self, path: Path | None = None):
        self.path = path or Path("local_data")

    def dump(self, data: list[dict], file_name: str, skip_exists: bool = True) -> None:
        """Dump data to local storage."""

        if skip_exists and (self.path / file_name).exists():
            LOGGER.info(f"File {self.path / file_name} already exists. Skipping...")
            return

        pd.DataFrame(data).to_parquet(self.path / f"{file_name}")
        LOGGER.info(f"Data dumped to {self.path / file_name}")

    def list_files(self, extensions: str = ".parquet") -> list[str]:
        """List files in the local storage."""
        return [str(file) for file in self.path.glob(f"*{extensions}")]


class S3Dumper:
    """Data dumper to S3."""

    def __init__(self, prefix: str, bucket_name: str | None = None):
        self.bucket_name = bucket_name or os.getenv("S3_BUCKET", "openalex-ingest")
        self.prefix = prefix
        self.s3 = self._load_s3()

    @staticmethod
    def _load_s3():
        """Load S3 resource."""
        if S3_ENDPOINT_URL := os.getenv("S3_ENDPOINT_URL"):
            return boto3.resource(
                "s3",
                endpoint_url=S3_ENDPOINT_URL,
                aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
                aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
            )
        else:
            return boto3.resource("s3")

    @staticmethod
    def _pd_storage_options() -> dict[str, Any] | None:
        """Return storage options for S3."""
        if not os.getenv("S3_ENDPOINT_URL"):
            return None
        return {
            "key": os.getenv("S3_ACCESS_KEY"),
            "secret": os.getenv("S3_SECRET_KEY"),
            "client_kwargs": {"endpoint_url": os.getenv("S3_ENDPOINT_URL")},
        }

    def dump(self, data: list[dict], file_name: str, skip_exists: bool = True) -> None:
        """Dump data to S3."""

        if skip_exists and file_name in self.list_files():
            LOGGER.info(f"File {self.prefix}/{file_name} already exists. Skipping...")
            return

        pd.DataFrame(data).to_parquet(
            f"s3://{self.bucket_name}/{self.prefix}/{file_name}",
            storage_options=self._pd_storage_options(),
        )
        LOGGER.info(f"Data dumped to {self.prefix}/{file_name}")

    def list_files(self, extensions: str = ".parquet") -> list[str]:
        """List files in the S3 bucket."""
        bucket = self.s3.Bucket(self.bucket_name)  # type: ignore
        return [
            obj.key
            for obj in bucket.objects.filter(Prefix=self.prefix)
            if obj.key.endswith(extensions)
        ]
