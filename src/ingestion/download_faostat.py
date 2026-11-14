
import hashlib
import json
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw"

DATASET_URL = (
    "https://bulks-faostat.fao.org/production/"
    "Production_Crops_Livestock_E_All_Data_(Normalized).zip"
)

ZIP_PATH = RAW_DIR / "faostat_crop_production.zip"
TEMP_PATH = RAW_DIR / "faostat_crop_production.zip.part"
METADATA_PATH = RAW_DIR / "faostat_download_metadata.json"


def calculate_sha256(file_path):
    sha256 = hashlib.sha256()

    with open(file_path, "rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            sha256.update(chunk)

    return sha256.hexdigest()


def download_dataset():
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    print("Starting FAOSTAT QCL dataset download...")
    print("Destination:", ZIP_PATH)

    try:
        with requests.get(
            DATASET_URL,
            stream=True,
            headers={"User-Agent": "NigeriaAgriClimatePipeline/1.0"},
            timeout=(15, 120),
        ) as response:

            response.raise_for_status()

            expected_size = response.headers.get("Content-Length")
            downloaded_size = 0

            with open(TEMP_PATH, "wb") as file:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        file.write(chunk)
                        downloaded_size += len(chunk)

            if expected_size and downloaded_size != int(expected_size):
                raise ValueError(
                    f"Download size mismatch. Expected {expected_size} bytes, "
                    f"received {downloaded_size} bytes."
                )

        print(f"Downloaded: {downloaded_size:,} bytes")

        # Verify that the downloaded file is a valid ZIP archive.
        with zipfile.ZipFile(TEMP_PATH, "r") as archive:
            corrupted_file = archive.testzip()

            if corrupted_file:
                raise ValueError(f"Corrupted ZIP member: {corrupted_file}")

            members = archive.infolist()

            csv_files = [
                member.filename
                for member in members
                if member.filename.lower().endswith(".csv")
            ]

            if not csv_files:
                raise ValueError("No CSV file found inside the ZIP archive.")

        # Move the verified download into its final location.
        TEMP_PATH.replace(ZIP_PATH)

        metadata = {
            "dataset_code": "QCL",
            "dataset_name": "Production: Crops and livestock products",
            "source": DATASET_URL,
            "catalogue_last_updated": "2025-12-31",
            "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
            "file_size_bytes": ZIP_PATH.stat().st_size,
            "sha256": calculate_sha256(ZIP_PATH),
            "csv_files": csv_files,
            "archive_members": [
                {
                    "filename": member.filename,
                    "size_bytes": member.file_size,
                }
                for member in members
            ],
        }

        METADATA_PATH.write_text(
            json.dumps(metadata, indent=4),
            encoding="utf-8",
        )

        print("\nDOWNLOAD SUCCESSFUL")
        print("ZIP file:", ZIP_PATH)
        print("SHA-256:", metadata["sha256"])
        print("\nCSV files found:")

        for csv_file in csv_files:
            print("-", csv_file)

        print("\nMetadata saved:", METADATA_PATH)

    except (requests.RequestException, OSError, ValueError, zipfile.BadZipFile) as error:
        print("\nDOWNLOAD OR VALIDATION FAILED:", error)
        raise

    finally:
        if TEMP_PATH.exists():
            TEMP_PATH.unlink()


if __name__ == "__main__":
    download_dataset()
