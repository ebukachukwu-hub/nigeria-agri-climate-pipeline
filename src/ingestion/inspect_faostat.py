
import csv
import io
import json
import zipfile
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
ZIP_PATH = PROJECT_ROOT / "data" / "raw" / "faostat_crop_production.zip"
REPORT_DIR = PROJECT_ROOT / "reports"
REPORT_PATH = REPORT_DIR / "faostat_schema_inspection.json"

SAMPLE_SIZE = 5


def inspect_csv(archive, csv_filename):
    """Inspect a CSV file directly inside the ZIP archive."""

    print("\n" + "=" * 70)
    print("CSV FILE:", csv_filename)
    print("=" * 70)

    with archive.open(csv_filename, "r") as binary_file:
        text_file = io.TextIOWrapper(
            binary_file,
            encoding="utf-8-sig",
            newline=""
        )

        reader = csv.DictReader(text_file)

        columns = reader.fieldnames

        if not columns:
            raise ValueError(
                f"No column headers found in {csv_filename}"
            )

        sample_records = []

        for _, record in zip(range(SAMPLE_SIZE), reader):
            sample_records.append(record)

    print("\nTOTAL COLUMNS:", len(columns))

    print("\nCOLUMN NAMES:")
    for index, column in enumerate(columns, start=1):
        print(f"{index}. {column}")

    print("\nSAMPLE RECORDS:")

    for index, record in enumerate(sample_records, start=1):
        print(f"\nRecord {index}:")
        print(json.dumps(record, indent=4, ensure_ascii=False))

    return {
        "csv_filename": csv_filename,
        "column_count": len(columns),
        "columns": columns,
        "sample_size": len(sample_records),
        "sample_records": sample_records
    }


def main():
    print("Starting FAOSTAT raw-data inspection...")

    if not ZIP_PATH.exists():
        raise FileNotFoundError(
            f"FAOSTAT ZIP archive not found: {ZIP_PATH}"
        )

    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    inspection_results = {
        "dataset": "FAOSTAT QCL",
        "archive": ZIP_PATH.name,
        "csv_files": []
    }

    with zipfile.ZipFile(ZIP_PATH, "r") as archive:

        csv_files = [
            name for name in archive.namelist()
            if name.lower().endswith(".csv")
        ]

        if not csv_files:
            raise ValueError(
                "No CSV files were found inside the FAOSTAT archive."
            )

        print("\nCSV FILES FOUND:", len(csv_files))

        for filename in csv_files:
            print(" -", filename)

        for filename in csv_files:
            result = inspect_csv(archive, filename)
            inspection_results["csv_files"].append(result)

    with open(REPORT_PATH, "w", encoding="utf-8") as file:
        json.dump(
            inspection_results,
            file,
            indent=4,
            ensure_ascii=False
        )

    print("\n" + "=" * 70)
    print("INSPECTION COMPLETED SUCCESSFULLY")
    print("=" * 70)
    print("Inspection report saved to:", REPORT_PATH)


if __name__ == "__main__":
    main()
