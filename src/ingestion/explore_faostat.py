
import json
import os
from pathlib import Path

import requests
from dotenv import load_dotenv


# Resolve the project root from this file's location
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Load optional environment variables
load_dotenv(PROJECT_ROOT / ".env")

BASE_URL = os.getenv(
    "FAOSTAT_BASE_URL",
    "https://faostatservices.fao.org/api/v1"
)

ACCESS_TOKEN = os.getenv("FAOSTAT_ACCESS_TOKEN")


def create_session():
    """Create an HTTP session for FAOSTAT requests."""

    session = requests.Session()

    session.headers.update({
        "Accept": "application/json",
        "User-Agent": "NigeriaAgriClimatePipeline/1.0"
    })

    if ACCESS_TOKEN:
        session.headers.update({
            "Authorization": f"Bearer {ACCESS_TOKEN}"
        })

    return session


def explore_domains():
    """Retrieve and inspect the FAOSTAT domain catalogue."""

    endpoint = f"{BASE_URL}/en/groupsanddomains"

    session = create_session()

    print("Connecting to FAOSTAT...")
    print(f"Endpoint: {endpoint}")

    try:
        response = session.get(
            endpoint,
            timeout=(10, 60)
        )

        print("HTTP status:", response.status_code)

        if response.status_code in (401, 403):
            print(
                "Access requires authentication or additional "
                "permission. Check the official API documentation."
            )
            print("Response:", response.text[:1000])
            return

        response.raise_for_status()

        # Preserve the original response bytes
        raw_directory = PROJECT_ROOT / "data" / "raw"
        raw_directory.mkdir(parents=True, exist_ok=True)

        output_file = raw_directory / "faostat_groups_and_domains.json"

        output_file.write_bytes(response.content)

        payload = response.json()

        print("\nResponse type:", type(payload).__name__)

        if isinstance(payload, dict):
            print("Top-level keys:", list(payload.keys()))

            records = payload.get("data", [])
        elif isinstance(payload, list):
            records = payload
        else:
            raise ValueError("Unexpected FAOSTAT response format.")

        print("Total domain records:", len(records))

        # Identify records relating to crop production
        matches = []

        for record in records:
            searchable_text = json.dumps(
                record,
                ensure_ascii=False
            ).lower()

            if "crop" in searchable_text or "livestock" in searchable_text:
                matches.append(record)

        print("\nRelevant agricultural domains:")

        for record in matches[:20]:
            print(json.dumps(record, indent=2))

        print("\nRaw response saved to:", output_file)

    except requests.exceptions.Timeout:
        print("The FAOSTAT request timed out.")

    except requests.exceptions.ConnectionError:
        print("Could not connect to the FAOSTAT server.")

    except requests.exceptions.HTTPError as error:
        print("HTTP error:", error)
        print("Response:", response.text[:1000])

    except requests.exceptions.RequestException as error:
        print("Request failed:", error)

    except ValueError as error:
        print("Data inspection error:", error)


if __name__ == "__main__":
    explore_domains()
