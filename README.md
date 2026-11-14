# Nigeria Crop Production & Climate Trends Pipeline

An end-to-end data engineering project integrating Nigerian agricultural production statistics with climate observations to explore historical relationships between crop production, rainfall and temperature.

**Project Status:** Environment Setup Completed | Data Ingestion Next

---

## 1. Project Overview

Agricultural production is influenced by several factors, including weather patterns, farming practices, soil conditions and economic conditions.

This project aims to combine agricultural production statistics from **FAOSTAT** with climate observations from **NASA POWER** to develop a reusable data pipeline for Nigeria.

Using Python and PySpark, the pipeline will ingest, clean, validate, integrate and transform datasets into analytical tables that support historical agricultural and climate analysis.

**Analytical consideration:** This project investigates statistical relationships between climate indicators and agricultural production. Correlation or association will not be interpreted as proof of causation.

## 2. Project Objectives

- Ingest agricultural production data from FAOSTAT.
- Retrieve historical rainfall and temperature observations from NASA POWER.
- Implement a Bronze, Silver and Gold data architecture.
- Standardise crop names, dates, geographical identifiers and measurement units.
- Handle missing values and investigate data-quality anomalies.
- Integrate agricultural and climate datasets.
- Produce analytical datasets for reporting and visualisation.
- Implement automated data-quality tests using pytest.
- Automate testing through GitHub Actions.

## 3. Data Sources

### FAOSTAT — Agricultural Production

Source: https://www.fao.org/faostat/

Expected indicators:

- Crop production quantity
- Harvested area
- Crop yield
- Reporting year
- Country and crop identifiers
- Measurement units

### NASA POWER — Climate Observations

Source: https://power.larc.nasa.gov/

Expected climate indicators:

- Monthly precipitation
- Temperature indicators
- Historical observations
- Geographic coordinates

The exact variables, units, geographical coverage and reporting periods will be documented after inspecting the source datasets.

## 4. Technology Stack

| Category | Technology |
|---|---|
| Programming | Python |
| Data Engineering | PySpark, Apache Spark |
| Agricultural Data | FAOSTAT API |
| Climate Data | NASA POWER API |
| Data Processing | Pandas, PyArrow |
| API Integration | Requests |
| Testing | pytest |
| Environment Management | Python venv |
| Version Control | Git and GitHub |
| CI/CD | GitHub Actions |
| Reporting | Python-based analytics and visualisations |

## 5. Development Environment

The local development environment has been configured and successfully tested.

| Component | Version |
|---|---|
| Python | 3.14.6 |
| Java | OpenJDK 25.0.4 LTS |
| PySpark | 4.3.0 |
| Pandas | 2.x — installed after compatibility adjustment |
| Requests | 2.34.2 |
| pytest | 9.2.0 |
| Operating System | Windows |
| Spark Execution | Local mode (`local[2]`) |

### Spark Verification

A local Spark session was successfully created using:

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("AgriClimateEnvironmentCheck")
    .getOrCreate()
)

print("Spark version:", spark.version)
print("Test row count:", spark.range(5).count())

spark.stop()
```

**Verification result:**

```text
Spark version: 4.3.0
Test row count: 5
```

This confirms that the local Spark environment can execute a basic DataFrame operation.

## 6. Pipeline Architecture

The project will follow a Medallion Architecture.

```text
       FAOSTAT API             NASA POWER API
            |                        |
            v                        v
       AGRICULTURAL DATA        CLIMATE DATA
            |                        |
            +-----------+------------+
                        |
                        v
                  BRONZE LAYER
                 Raw source data
                        |
                        v
                  SILVER LAYER
              Cleaning and validation
              Standardisation
              Data integration
                        |
                        v
                   GOLD LAYER
               Analytical datasets
               Annual crop indicators
               Climate summaries
                        |
                        v
              REPORTS & VISUALISATIONS
```

### Bronze Layer

Preserve ingested source data in its original form, together with relevant metadata.

### Silver Layer

Clean, standardise and validate the source datasets before integrating agricultural and climate observations.

### Gold Layer

Produce analytical datasets suitable for historical comparisons, reporting and visualisation.

## 7. Project Structure

```text
nigeria-agri-climate-pipeline/
│
├── data/
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── notebooks/
│
├── src/
│   ├── ingestion/
│   ├── transformations/
│   ├── validation/
│   └── analytics/
│
├── tests/
├── config/
├── reports/
│
├── .github/
│   └── workflows/
│
├── requirements.txt
├── README.md
└── .gitignore
```

## 8. Development Milestones

| Milestone | Deliverable | Status |
|---|---|---|
| 1 | Project workspace and folder structure | Completed |
| 2 | Git and GitHub repository setup | Completed |
| 3 | Python virtual environment and dependencies | Completed |
| 4 | Java and local Spark verification | Completed |
| 5 | FAOSTAT data ingestion | Pending |
| 6 | NASA POWER climate data ingestion | Pending |
| 7 | Bronze layer implementation | Pending |
| 8 | Silver layer cleaning and integration | Pending |
| 9 | Gold analytical datasets | Pending |
| 10 | Data-quality validation and anomaly investigation | Pending |
| 11 | Automated testing with pytest | Pending |
| 12 | GitHub Actions workflow | Pending |
| 13 | Reporting, analysis and final documentation | Pending |

Milestones will be updated as individual components are implemented and tested.

## 9. Data Quality and Validation

The pipeline will incorporate data-quality checks covering:

- Missing and invalid values.
- Duplicate records.
- Unexpected crop names and measurement units.
- Invalid geographical coordinates.
- Inconsistent reporting periods.
- Unreasonable production, yield and climate values.
- Unexpected changes in record counts after transformations.

Questionable records will be investigated and documented rather than silently removed.

## 10. Analytical Questions

The final analytical datasets will support questions such as:

1. How has the production of selected Nigerian crops changed over time?
2. How have rainfall and temperature patterns changed across selected locations?
3. Do changes in annual climate indicators correspond with changes in crop yields?
4. Which crops and geographical areas show notable historical patterns?
5. What limitations affect the reliability of the analysis?

## 11. Testing Strategy

The project will use pytest to test individual ingestion, transformation and validation functions.

Testing will cover:

- API response handling.
- Data schema consistency.
- Transformation accuracy.
- Duplicate detection.
- Missing-value handling.
- Data integration rules.
- Analytical output validation.

GitHub Actions will subsequently automate these tests whenever changes are pushed to the repository.

## 12. Project Status

**Current phase:** Environment setup completed.

**Next milestone:** FAOSTAT agricultural data ingestion.

The next implementation stage will involve retrieving Nigerian crop-production records, inspecting the API response, documenting source metadata and preserving the original data.

## 13. Author

**Enoch Kanikwu**

- GitHub: https://github.com/ebukachukwu-hub
- LinkedIn: https://www.linkedin.com/in/enoch-kanikwu-470053305

---

*Developed as an educational, analytical and professional data engineering portfolio project. Findings will be interpreted with appropriate consideration for data quality, geographical coverage and statistical limitations.*