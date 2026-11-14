# Nigeria Crop Production & Climate Trends Pipeline

An end-to-end data engineering project integrating agricultural production statistics from the Food and Agriculture Organization (FAO) with climate observations from NASA POWER to explore historical relationships between crop production, yield, rainfall and temperature in Nigeria.

The project demonstrates practical data engineering skills through automated data ingestion, distributed processing with PySpark, data quality validation, medallion architecture and analytical dataset development.

## Project Overview

Agriculture plays an important role in Nigeria's economy, food security and employment. However, agricultural production is influenced by several factors, including rainfall patterns, temperature variations and other environmental conditions.

This project aims to build a reproducible data pipeline that combines historical agricultural statistics with climate observations to support data-driven exploration of agricultural trends in Nigeria.

The project will focus on:

- Historical crop production trends in Nigeria.
- Changes in crop yield and harvested area.
- Rainfall and temperature patterns.
- Relationships between agricultural production and climate variables.
- Identification of missing, inconsistent and unusual observations.

**Important:** Statistical associations identified through this project will not automatically be interpreted as evidence of causation.

## Project Objectives

1. Retrieve historical crop production statistics from FAOSTAT.
2. Collect corresponding historical climate observations from NASA POWER.
3. Build automated and reproducible data ingestion processes.
4. Implement a medallion architecture using Bronze, Silver and Gold data layers.
5. Process and transform datasets using PySpark.
6. Perform data quality checks and investigate anomalies.
7. Develop analytical datasets for agricultural and climate trend analysis.
8. Automate testing and pipeline validation using GitHub Actions.

## Data Sources

### 1. FAOSTAT – Agricultural Production Statistics

**Source:** [FAOSTAT](https://www.fao.org/faostat/en/#data/QCL)

Selected dataset:

- Dataset code: `QCL`
- Dataset name: Production: Crops and livestock products.
- Frequency: Annual.
- Format: Normalized CSV distributed through a ZIP archive.

The dataset contains agricultural statistics such as:

- Area harvested.
- Production quantity.
- Yield.
- Country and commodity information.
- Reporting years and measurement units.

The catalogue reported 4,209,110 records for the selected dataset.

### 2. NASA POWER – Climate Observations

**Source:** [NASA POWER](https://power.larc.nasa.gov/)

NASA POWER will provide historical climate observations, including:

- Precipitation.
- Temperature.
- Other relevant meteorological variables.

The climate dataset will be aligned with agricultural production records using appropriate geographical and temporal dimensions.

## FAOSTAT API Investigation and Ingestion Decision

During the initial data-source exploration, an attempt was made to access the FAOSTAT API through:

`https://faostatservices.fao.org/api/v1/en/groupsanddomains`

The request returned:

`HTTP 401 – Unauthorized`

The response indicated that an Authorization Header was missing.

Further investigation of the official FAOSTAT API documentation established that the API requires Bearer Token authentication.

Since an authenticated API token was not available for this project, the unauthenticated API approach could not be used.

Rather than allowing this to block the ingestion process, an alternative approach was investigated.

The public FAOSTAT bulk-download catalogue was identified:

`https://bulks-faostat.fao.org/production/datasets_E.json`

The catalogue was successfully accessed without API authentication and returned 69 available datasets.

The QCL dataset was identified through its metadata, including its dataset code, update date, reported file size, record count and download location.

**Engineering decision:** The project uses FAOSTAT's public bulk-download service for the initial ingestion process instead of the authenticated API endpoint.

This approach provides access to the required agricultural statistics while supporting an automated and reproducible download workflow.

The API investigation is retained as part of the project's data-source discovery and engineering decision-making process.

## Technology Stack

| Technology | Purpose |
|---|---|
| Python | Pipeline development |
| PySpark | Distributed data processing |
| Apache Spark | Large-scale data transformation |
| Pandas | Initial data inspection and supporting analysis |
| Requests | HTTP data ingestion |
| PyArrow | Columnar data support |
| pytest | Automated testing |
| Git | Version control |
| GitHub | Source-code management |
| GitHub Actions | Continuous integration |
| NASA POWER API | Climate data collection |
| FAOSTAT | Agricultural production statistics |

## Development Environment

The project is being developed locally using Visual Studio Code.

| Component | Installed version |
|---|---|
| Python | 3.14.6 |
| Java | OpenJDK 25.0.4 LTS |
| PySpark | 4.3.0 |
| Pandas | 2.x (compatibility-adjusted) |
| Requests | 2.34.2 |
| pytest | 9.2.0 |

A Python virtual environment is used to isolate project dependencies.

### Environment Verification

Spark was successfully initialized in local mode.

Verification command:

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("AgriClimateTest")
    .getOrCreate()
)

print("Spark version:", spark.version)
print("Rows:", spark.range(5).count())

spark.stop()
```

Successful output:

```text
Spark version: 4.3.0
Rows: 5
```

## Project Architecture

The pipeline follows the Medallion Architecture.

```text
             DATA SOURCES
                   |
          -------------------
          |                 |
       FAOSTAT           NASA POWER
          |                 |
          -------------------
                   |
             INGESTION
                   |
             BRONZE LAYER
          (Raw ingested data)
                   |
             SILVER LAYER
       (Cleaned and transformed)
                   |
              GOLD LAYER
        (Analytical datasets)
                   |
          ANALYSIS & REPORTING
```

### Bronze Layer

Stores ingested source data with minimal transformation.

### Silver Layer

Contains cleaned, standardized and validated datasets.

### Gold Layer

Contains aggregated and analytical datasets designed to support agricultural and climate trend analysis.

## Repository Structure

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
│
├── config/
│
├── reports/
│
├── .github/
│   └── workflows/
│
├── requirements.txt
├── README.md
└── .gitignore
```

## Completed Development Milestones

- [x] Create project workspace and directory structure.
- [x] Initialize Git repository.
- [x] Create GitHub repository and connect remote origin.
- [x] Configure Python virtual environment.
- [x] Install project dependencies.
- [x] Install Java and verify Apache Spark.
- [x] Investigate FAOSTAT API authentication requirements.
- [x] Identify and access the public FAOSTAT bulk-download catalogue.
- [x] Identify the QCL agricultural production dataset.
- [x] Develop the FAOSTAT catalogue exploration script.
- [x] Develop automated FAOSTAT dataset downloader.
- [x] Validate downloaded ZIP archive and generate SHA-256 checksum.
- [ ] Inspect FAOSTAT CSV schema and sample records.
- [ ] Complete FAOSTAT raw-data ingestion.
- [ ] Implement NASA POWER climate data ingestion.
- [ ] Build Bronze data layer.
- [ ] Build Silver transformation layer.
- [ ] Build Gold analytical layer.
- [ ] Implement data quality validation.
- [ ] Develop automated tests using pytest.
- [ ] Configure GitHub Actions.
- [ ] Develop analytical reports and visualizations.
- [ ] Complete project documentation.

## Data Quality and Validation Strategy

The pipeline will implement checks to identify:

- Missing values in important fields.
- Duplicate records.
- Invalid or inconsistent measurement units.
- Incorrect date and year values.
- Unexpected production and yield values.
- Inconsistent country and commodity identifiers.
- Potential anomalies in climate observations.

Validation results will be documented to improve the reliability and reproducibility of the pipeline.

## Planned Analytical Questions

The final analytical datasets will support questions such as:

1. How has Nigeria's crop production changed over time?
2. Which crops have experienced the most significant changes in production?
3. How have harvested area and crop yields evolved?
4. What historical rainfall patterns can be observed?
5. How have temperature patterns changed across the observation period?
6. Are there observable associations between rainfall, temperature and crop production?
7. Which crops demonstrate greater variation in production and yield?

## Reproducibility and Version Control

The project uses Git and GitHub for version control.

Each completed development milestone is documented and pushed to the remote repository.

Raw datasets, virtual environments, temporary files and sensitive configuration files are excluded from version control through `.gitignore`.

The project maintains download metadata and SHA-256 checksums to support data provenance and reproducibility.

## Author

**Enoch Echezonachukwu Kanikwu**

B.Sc. Statistics, First Class Honours

Bowen University

- GitHub: https://github.com/ebukachukwu-hub
- LinkedIn: https://www.linkedin.com/in/enoch-kanikwu-470053305

## Project Status

<text>Active development — Data ingestion and source exploration phase.</text>

This project is being developed as a practical demonstration of data engineering, statistical analysis and data-driven agricultural research.