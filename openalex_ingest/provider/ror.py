import argparse
import io
import json
from typing import Any
from zipfile import ZipFile

import requests

from openalex_ingest.common import LOGGER
from openalex_ingest.staging import LocalDumper, S3Dumper


def get_most_recent_ror_dump_metadata() -> dict[str, Any]:
    """Obtain the most recent ROR data dump metadata from Zenodo.

    docs: https://ror.readme.io/docs/data-dump#download-ror-data-dumps-programmatically-with-the-zenodo-api
    """

    url = "https://zenodo.org/api/communities/ror-data/records?q=&sort=newest"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()["hits"]["hits"][0]["files"][-1]


def download_and_unzip_ror_data(
    url: str,
) -> tuple[list[dict[str, Any]], str]:
    """Download and unzip the ROR data dump from Zenodo."""

    response = requests.get(url)
    response.raise_for_status()

    with ZipFile(io.BytesIO(response.content)) as zip_file:
        for file_name in zip_file.namelist():
            if not file_name.endswith("ror-data_schema_v2.json"):
                continue
            with zip_file.open(file_name) as file:
                ror_data = json.load(file)
                return ror_data, file_name.split(".json")[0]
    raise RuntimeError("Failed to find the ROR data file in the zip archive!")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download and process the most recent ROR data dump."
    )
    parser.add_argument(
        "--dumper",
        choices=["local", "s3"],
        default="local",
        help="Select the dumper to use: 'local' for LocalDumper or 's3' for S3Dumper.",
    )
    args = parser.parse_args()

    most_recent_file_obj = get_most_recent_ror_dump_metadata()

    if most_recent_file_obj is None:
        LOGGER.info("Failed to get ROR data. Exiting without doing any updates...")
        return

    # Get the URL of the most recent file
    try:
        file_url = most_recent_file_obj["links"]["self"]
    except KeyError:
        LOGGER.error(
            "Failed to get URL out of the most recent file! Exiting without doing any updates..."
        )
        raise

    # Download and unzip the ROR data
    try:
        LOGGER.info(f"Downloading and unzipping ROR data from {file_url}")
        ror_data, file_name = download_and_unzip_ror_data(file_url)
    except Exception as e:
        LOGGER.error(
            f"Failed to download and unzip ROR data! Exiting without doing any updates... {e}"
        )
        raise

    if args.dumper == "s3":
        # Dump to S3
        dumper = S3Dumper("ror-data")
        dumper.dump(ror_data, f"{file_name}.parquet")

    if args.dumper == "local":
        dumper = LocalDumper()
        dumper.dump(ror_data, f"{file_name}.parquet")


if __name__ == "__main__":
    main()
