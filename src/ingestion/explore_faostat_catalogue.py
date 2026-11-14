
import json
from pathlib import Path

import requests


PROJECT_ROOT = Path(__file__).resolve().parents[2]

CATALOGUE_URL = (
    "https://bulks-faostat.fao.org/production/datasets_E.json"
)


def explore_catalogue():

    print("Connecting to FAOSTAT bulk catalogue...")

    try:
        response = requests.get(
            CATALOGUE_URL,
            headers={"User-Agent": "NigeriaAgriClimatePipeline/1.0"},
            timeout=(10, 60)
        )

        print("HTTP status:", response.status_code)

        response.raise_for_status()

        # Preserve the original catalogue response
        raw_directory = PROJECT_ROOT / "data" / "raw"
        raw_directory.mkdir(parents=True, exist_ok=True)

        output_file = raw_directory / "faostat_catalogue.json"
        output_file.write_bytes(response.content)

        catalogue = response.json()

        datasets = catalogue["Datasets"]["Dataset"]

        print("Total available datasets:", len(datasets))

        # Locate crop and livestock production
        crop_datasets = [
            dataset
            for dataset in datasets
            if dataset.get("DatasetCode") == "QCL"
        ]

        if not crop_datasets:
            print("QCL dataset not found.")
            return

        for dataset in crop_datasets:

            print("\nCROP PRODUCTION DATASET")

            print(
                json.dumps(
                    dataset,
                    indent=4,
                    ensure_ascii=False
                )
            )

        print("\nCatalogue saved to:", output_file)

    except requests.exceptions.RequestException as error:
        print("Request error:", error)

    except (KeyError, TypeError, ValueError) as error:
        print("Catalogue inspection error:", error)


if __name__ == "__main__":
    explore_catalogue()
