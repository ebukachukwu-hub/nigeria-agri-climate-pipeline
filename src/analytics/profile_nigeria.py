
import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_FILE = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "extracted"
    / "Production_Crops_Livestock_E_All_Data_(Normalized).csv"
)

REPORT_FILE = (
    PROJECT_ROOT
    / "reports"
    / "nigeria_agricultural_profile.json"
)

CROP_ELEMENTS = [
    "Production",
    "Area harvested",
    "Yield",
]


def create_spark_session():

    return (
        SparkSession.builder
        .appName("NigeriaAgriculturalProfile")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def load_nigeria_data(spark):

    print("Loading FAOSTAT agricultural production data...")

    if not INPUT_FILE.exists():
        raise FileNotFoundError(
            f"Input dataset not found: {INPUT_FILE}"
        )

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .option("mode", "FAILFAST")
        .csv(str(INPUT_FILE))
    )

    nigeria_df = df.filter(
        F.col("Area") == "Nigeria"
    )

    print("Nigeria records loaded successfully.")

    return nigeria_df


def profile_basic_information(df):

    print("\nBASIC PROFILE")
    print("-" * 60)

    result = df.agg(
        F.count("*").alias("total_records"),
        F.countDistinct("Item").alias("unique_commodities"),
        F.countDistinct("Element").alias("unique_elements"),
        F.countDistinct("Year").alias("unique_years"),
        F.min("Year").alias("first_year"),
        F.max("Year").alias("last_year"),
    ).first()

    profile = {
        "total_records": result["total_records"],
        "unique_commodities": result["unique_commodities"],
        "unique_elements": result["unique_elements"],
        "unique_years": result["unique_years"],
        "first_year": result["first_year"],
        "last_year": result["last_year"],
    }

    for key, value in profile.items():
        print(f"{key}: {value}")

    return profile


def profile_crop_metrics(df):

    print("\nCROP METRIC DISTRIBUTION")
    print("-" * 60)

    crop_df = df.filter(
        F.col("Element").isin(CROP_ELEMENTS)
    )

    results = (
        crop_df.groupBy("Element")
        .agg(
            F.count("*").alias("records"),
            F.countDistinct("Item").alias("commodities"),
            F.countDistinct("Year").alias("years"),
        )
        .orderBy("Element")
        .collect()
    )

    profile = {}

    for row in results:

        element = row["Element"]

        profile[element] = {
            "records": row["records"],
            "commodities": row["commodities"],
            "years": row["years"],
        }

        print(
            f"{element}: "
            f"{row['records']} records, "
            f"{row['commodities']} commodities, "
            f"{row['years']} years"
        )

    return profile


def profile_measurement_units(df):

    print("\nMEASUREMENT AND UNIT DISTRIBUTION")
    print("-" * 60)

    results = (
        df.groupBy("Element", "Unit")
        .agg(
            F.count("*").alias("records"),
            F.countDistinct("Item").alias("commodities"),
            F.countDistinct("Year").alias("years"),
        )
        .orderBy("Element", "Unit")
        .collect()
    )

    profile = []

    for row in results:

        item = {
            "element": row["Element"],
            "unit": row["Unit"],
            "records": row["records"],
            "commodities": row["commodities"],
            "years": row["years"],
        }

        profile.append(item)

        print(
            f"{row['Element']} | {row['Unit']}: "
            f"{row['records']} records, "
            f"{row['commodities']} commodities, "
            f"{row['years']} years"
        )

    return profile


def profile_top_commodities(df):

    print("\nTOP 20 COMMODITIES BY RECORD COUNT")
    print("-" * 60)

    results = (
        df.groupBy("Item")
        .agg(
            F.count("*").alias("records"),
            F.countDistinct("Year").alias("years"),
            F.countDistinct("Element").alias("measurement_types"),
        )
        .orderBy(F.desc("records"), "Item")
        .limit(20)
        .collect()
    )

    profile = []

    for row in results:

        item = {
            "commodity": row["Item"],
            "records": row["records"],
            "years": row["years"],
            "measurement_types": row["measurement_types"],
        }

        profile.append(item)

        print(item)

    return profile


def profile_year_coverage(df):

    print("\nCOMMODITY SERIES YEAR COVERAGE")
    print("-" * 60)

    series_df = (
        df.groupBy("Item", "Element", "Unit")
        .agg(
            F.min("Year").alias("first_year"),
            F.max("Year").alias("last_year"),
            F.countDistinct("Year").alias("observed_years"),
        )
    )

    results = series_df.collect()

    gaps = []

    for row in results:

        first_year = int(row["first_year"])
        last_year = int(row["last_year"])

        expected_years = last_year - first_year + 1
        observed_years = row["observed_years"]

        if observed_years < expected_years:

            gaps.append({
                "commodity": row["Item"],
                "element": row["Element"],
                "unit": row["Unit"],
                "first_year": first_year,
                "last_year": last_year,
                "expected_years": expected_years,
                "observed_years": observed_years,
                "missing_years": expected_years - observed_years,
            })

    result = {
        "total_series": len(results),
        "series_with_internal_missing_years": len(gaps),
        "series_without_internal_missing_years": (
            len(results) - len(gaps)
        ),
        "gap_details": gaps,
    }

    print(f"Total commodity-measurement-unit series: {len(results)}")
    print(
        "Series with internal missing years: "
        f"{len(gaps)}"
    )
    print(
        "Series without internal missing years: "
        f"{len(results) - len(gaps)}"
    )

    return result


def profile_crop_metric_coverage(df):

    print("\nCROP METRIC COVERAGE INTERSECTION")
    print("-" * 60)

    crop_df = df.filter(
        F.col("Element").isin(CROP_ELEMENTS)
    )

    metric_coverage = (
        crop_df.groupBy("Item")
        .agg(
            F.collect_set("Element").alias("available_metrics"),
            F.collect_set("Unit").alias("available_units"),
            F.count("*").alias("record_count"),
        )
    )

    rows = metric_coverage.collect()

    complete = []
    incomplete = []

    required_metrics = set(CROP_ELEMENTS)

    for row in rows:

        available = set(row["available_metrics"] or [])

        item = {
            "commodity": row["Item"],
            "available_metrics": sorted(available),
            "available_units": sorted(
                row["available_units"] or []
            ),
            "record_count": row["record_count"],
        }

        if available == required_metrics:
            complete.append(item)
        else:
            incomplete.append(item)

    complete.sort(key=lambda x: x["commodity"])
    incomplete.sort(key=lambda x: x["commodity"])

    print(
        f"Commodities with all three metrics: {len(complete)}"
    )

    print(
        f"Commodities without all three metrics: {len(incomplete)}"
    )

    print("\nCOMMODITIES WITH ALL THREE METRICS")

    for item in complete:
        print(item)

    print("\nFIRST 20 COMMODITIES WITHOUT ALL THREE METRICS")

    for item in incomplete[:20]:
        print(item)

    return {
        "required_metrics": CROP_ELEMENTS,
        "complete_metric_commodities": complete,
        "incomplete_commodities": incomplete,
        "complete_count": len(complete),
        "incomplete_count": len(incomplete),
    }


def profile_same_year_metric_coverage(df):

    print("\nSAME-YEAR CROP METRIC COVERAGE")
    print("-" * 60)

    crop_df = df.filter(
        F.col("Element").isin(CROP_ELEMENTS)
    )

    # First identify commodity-year combinations where
    # all three metrics are present in the same year.
    commodity_year_coverage = (
        crop_df.groupBy("Item", "Year")
        .agg(
            F.collect_set("Element").alias("available_metrics")
        )
    )

    complete_years = (
        commodity_year_coverage
        .filter(
            F.size("available_metrics") == len(CROP_ELEMENTS)
        )
    )

    results = (
        complete_years.groupBy("Item")
        .agg(
            F.countDistinct("Year").alias("years_with_all_metrics"),
            F.min("Year").alias("first_complete_year"),
            F.max("Year").alias("last_complete_year"),
        )
        .orderBy("Item")
        .collect()
    )

    profile = []

    for row in results:

        item = {
            "commodity": row["Item"],
            "years_with_all_metrics": row["years_with_all_metrics"],
            "first_complete_year": row["first_complete_year"],
            "last_complete_year": row["last_complete_year"],
        }

        profile.append(item)

        print(item)

    print(
        "\nCommodities with at least one year containing "
        f"all three metrics: {len(profile)}"
    )

    return {
        "required_metrics": CROP_ELEMENTS,
        "commodities_with_same_year_coverage": profile,
        "commodity_count": len(profile),
    }


def investigate_duplicates(df):

    print("\nDUPLICATE INVESTIGATION")
    print("-" * 60)

    # These columns identify an observation in the FAOSTAT
    # normalized dataset.
    key_columns = [
        "Area Code",
        "Item Code",
        "Element Code",
        "Year",
    ]

    duplicate_groups = (
        df.groupBy(*key_columns)
        .count()
        .filter(F.col("count") > 1)
    )

    duplicate_group_count = duplicate_groups.count()

    duplicate_record_count = (
        duplicate_groups
        .agg(F.sum("count").alias("records"))
        .first()["records"]
    )

    if duplicate_record_count is None:
        duplicate_record_count = 0

    result = {
        "duplicate_groups": duplicate_group_count,
        "records_in_duplicate_groups": duplicate_record_count,
        "key_columns": key_columns,
    }

    print(f"Duplicate groups: {duplicate_group_count}")
    print(
        "Records in duplicate groups: "
        f"{duplicate_record_count}"
    )

    return result


def profile_missing_values(df):

    print("\nMISSING VALUE PROFILE")
    print("-" * 60)

    columns_to_check = [
        "Value",
        "Flag",
        "Note",
        "Unit",
        "Year",
    ]

    result = {}

    for column in columns_to_check:

        missing_count = df.filter(
            F.col(column).isNull()
            | (F.trim(F.col(column)) == "")
        ).count()

        result[column] = missing_count

        print(f"{column}: {missing_count}")

    # Check whether missing Value records have the FAOSTAT
    # missing-data flag M.
    missing_value_df = df.filter(
        F.col("Value").isNull()
        | (F.trim(F.col("Value")) == "")
    )

    flag_distribution = (
        missing_value_df.groupBy("Flag")
        .count()
        .orderBy("Flag")
        .collect()
    )

    missing_value_flags = {
        str(row["Flag"]): row["count"]
        for row in flag_distribution
    }

    result["missing_value_flag_distribution"] = (
        missing_value_flags
    )

    print("\nFlags associated with missing Value records:")

    for flag, count in missing_value_flags.items():
        print(f"{flag}: {count}")

    return result


def build_profile(df):

    return {
        "basic_information": profile_basic_information(df),
        "crop_metrics": profile_crop_metrics(df),
        "measurement_unit_distribution": (
            profile_measurement_units(df)
        ),
        "top_commodities": profile_top_commodities(df),
        "year_coverage": profile_year_coverage(df),
        "crop_metric_coverage": profile_crop_metric_coverage(df),
        "same_year_metric_coverage": (
            profile_same_year_metric_coverage(df)
        ),
        "duplicate_investigation": investigate_duplicates(df),
        "missing_values": profile_missing_values(df),
    }


def save_report(report):

    REPORT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with open(REPORT_FILE, "w", encoding="utf-8") as file:

        json.dump(
            report,
            file,
            indent=4,
            ensure_ascii=False,
        )

    print("\nPROFILE REPORT SAVED")
    print("-" * 60)
    print(REPORT_FILE)


def main():

    spark = create_spark_session()

    try:

        nigeria_df = load_nigeria_data(spark)

        report = build_profile(nigeria_df)

        save_report(report)

        print("\nNIGERIA AGRICULTURAL PROFILING COMPLETED")

    finally:

        spark.stop()


if __name__ == "__main__":
    main()
