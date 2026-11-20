# Nigeria Crop Production & Climate Trends Pipeline

An end-to-end data engineering project that integrates historical agricultural production data from the Food and Agriculture Organization (FAO) with climate observations from NASA POWER to investigate long-term agricultural and climate trends in Nigeria.

The project uses Python, PySpark, and a medallion data architecture to build a reproducible pipeline for ingesting, processing, validating, and analysing agricultural and climate datasets.

## Project Overview

Agriculture plays an important role in Nigeria's economy, food security, and employment. However, understanding how agricultural production patterns relate to climate conditions requires reliable historical data and a structured analytical approach.

This project aims to build a data pipeline that combines historical crop production statistics with climate variables such as rainfall and temperature.

The resulting dataset will support exploratory analysis of agricultural trends and statistical relationships between crop production, yield, rainfall, and temperature.

**Important:** The project investigates statistical relationships and associations. Correlation alone does not establish that climate conditions caused a change in agricultural production.

## Project Objectives

- Ingest historical agricultural production data from FAOSTAT.
- Retrieve historical climate observations from NASA POWER.
- Build a scalable data processing workflow using PySpark.
- Implement the Medallion Architecture (Bronze, Silver, and Gold).
- Perform data cleaning, standardisation, and validation.
- Integrate agricultural and climate datasets using appropriate geographical and temporal dimensions.
- Analyse historical crop production, harvested area, yield, rainfall, and temperature trends.
- Generate analytical datasets suitable for reporting and visualisation.
- Apply software engineering practices, including version control, testing, and documentation.

## Data Sources

### 1. FAOSTAT — Agricultural Production Data

**Organisation:** Food and Agriculture Organization of the United Nations (FAO)

**Dataset:** Production: Crops and livestock products

**Dataset code:** QCL

**Source catalogue:** https://bulks-faostat.fao.org/production/datasets_E.json

**Dataset download:** https://bulks-faostat.fao.org/production/Production_Crops_Livestock_E_All_Data_(Normalized).zip

The FAOSTAT dataset contains historical agricultural statistics, including:

- Crop production
- Area harvested
- Yield
- Livestock-related production indicators
- Commodity classifications
- Geographical identifiers
- Observation flags and metadata

**Initial dataset profiling results:**

| Metric | Result |
|---|---:|
| Total records | 4,209,110 |
| Total columns | 14 |
| Geographical areas | 244 |
| Commodities | 301 |
| Measurement elements | 8 |
| Historical coverage | 1961–2024 |
| Nigeria records | 17,539 |
| Nigerian commodities | 121 |

The complete normalized dataset is downloaded as a ZIP archive. Only the main normalized CSV is extracted for the initial profiling process.

### 2. NASA POWER — Climate Data

**Organisation:** NASA

**Platform:** Prediction of Worldwide Energy Resources (POWER)

**Website:** https://power.larc.nasa.gov/

NASA POWER provides meteorological and solar-related observations that can support agricultural and environmental analysis.

Climate variables planned for this project include:

- Precipitation
- Mean air temperature
- Maximum air temperature
- Minimum air temperature

The climate dataset will be retrieved for relevant Nigerian geographical locations and aligned with agricultural observations using appropriate temporal and spatial dimensions.

## Technology Stack

| Technology | Purpose |
|---|---|
| Python | Pipeline development and automation |
| PySpark | Distributed data processing |
| Apache Spark | Data transformation and aggregation |
| Pandas | Supporting data inspection and analysis |
| Requests | API communication and data retrieval |
| JSON | Metadata and profiling reports |
| PyArrow | Columnar data processing support |
| Pytest | Automated testing |
| Git & GitHub | Version control and project management |
| VS Code | Development environment |
| NASA POWER API | Climate data acquisition |
| FAOSTAT | Agricultural data source |

## Development Environment

The project is developed locally using:

- Operating system: Windows 11
- Python: 3.14.6
- Java: OpenJDK 25.0.4 LTS (Temurin)
- PySpark: 4.3.0
- Git: GitHub version control
- IDE: Visual Studio Code

Apache Spark was successfully tested in local execution mode using a two-worker local configuration.

## Repository Structure

```text
nigeria-agri-climate-pipeline/
│
├── .github/
│   └── workflows/
│
├── config/
│
├── data/
│   ├── raw/
│   │   ├── extracted/
│   │   └── .gitkeep
│   │
│   ├── bronze/
│   │   └── .gitkeep
│   │
│   ├── silver/
│   │   └── .gitkeep
│   │
│   └── gold/
│       └── .gitkeep
│
├── notebooks/
│
├── reports/
│   ├── faostat_schema_inspection.json
│   └── faostat_full_profile.json
│
├── src/
│   ├── ingestion/
│   │   ├── explore_faostat_catalogue.py
│   │   ├── download_faostat.py
│   │   └── inspect_faostat.py
│   │
│   ├── transformations/
│   │
│   ├── validation/
│   │
│   └── analytics/
│       └── profile_faostat.py
│
├── tests/
│
├── .gitignore
├── requirements.txt
└── README.md
```

*The repository structure will expand as additional ingestion, transformation, validation, and analytical modules are implemented.*

## Data Engineering Architecture

The project follows the Medallion Architecture.

### Bronze Layer — Raw Data

The Bronze layer will contain source data ingested from FAOSTAT and NASA POWER with minimal modification.

Planned responsibilities:

- Preserve source records.
- Maintain source metadata.
- Track ingestion timestamps.
- Retain original observation flags.
- Support reproducibility and traceability.

### Silver Layer — Cleaned and Standardised Data

The Silver layer will contain validated and standardised datasets.

Planned transformations:

- Standardise column names and data types.
- Preserve geographical and commodity identifiers.
- Handle missing values appropriately.
- Validate measurement units.
- Identify duplicate records.
- Standardise dates and geographical dimensions.
- Separate crop production observations from livestock-related indicators.
- Prepare climate observations for integration.

### Gold Layer — Analytical Data

The Gold layer will contain business-ready analytical datasets.

Planned outputs:

- Annual Nigerian crop production trends.
- Commodity-level production and yield statistics.
- Harvested-area trends.
- Historical rainfall and temperature summaries.
- Integrated crop production and climate datasets.
- Analytical tables for dashboards and reporting.

## Development Progress

### Completed Milestones

- [x] Created project workspace and directory structure.
- [x] Initialised Git and connected the GitHub repository.
- [x] Created Python virtual environment.
- [x] Installed project dependencies.
- [x] Installed and verified Java.
- [x] Installed and tested PySpark.
- [x] Investigated the FAOSTAT API authentication requirements.
- [x] Identified the public FAOSTAT bulk download catalogue.
- [x] Explored the catalogue and identified the QCL dataset.
- [x] Developed a FAOSTAT catalogue exploration script.
- [x] Developed a downloader for the FAOSTAT production dataset.
- [x] Implemented ZIP integrity verification and SHA-256 checksum generation.
- [x] Inspected the downloaded archive and identified its five CSV files.
- [x] Inspected the main normalized dataset schema.
- [x] Inspected the geographical, element, flag, and item reference files.
- [x] Generated a machine-readable schema inspection report.
- [x] Extracted the main normalized CSV from the archive.
- [x] Loaded the complete FAOSTAT dataset using PySpark.
- [x] Profiled all 4,209,110 records.
- [x] Identified geographical, commodity, and measurement-element distributions.
- [x] Generated Nigeria-specific dataset statistics.
- [x] Analysed observation flags, missing values, zero values, and invalid numeric records.
- [x] Generated a machine-readable full dataset profiling report.

### Upcoming Milestones

- [ ] Conduct detailed Nigeria-specific agricultural data profiling.
- [ ] Investigate commodity-level year coverage and measurement completeness.
- [ ] Identify duplicate observations and assess data consistency.
- [ ] Develop a structured FAOSTAT ingestion process.
- [ ] Retrieve historical climate observations from NASA POWER.
- [ ] Implement Bronze-layer ingestion.
- [ ] Develop Silver-layer cleaning and transformation processes.
- [ ] Integrate agricultural and climate observations.
- [ ] Build Gold-layer analytical datasets.
- [ ] Implement data quality checks and automated validation.
- [ ] Develop automated tests using Pytest.
- [ ] Configure GitHub Actions for continuous integration.
- [ ] Develop visualisations and analytical reports.
- [ ] Complete project documentation.

## FAOSTAT Data Exploration and Engineering Decisions

### API Investigation

The initial investigation explored the FAOSTAT API endpoint:

`https://faostatservices.fao.org/api/v1/en/groupsanddomains`

The request returned HTTP 401 with a missing authorization header message.

The documented API uses Bearer-token authentication. Since an API token was not available for the initial experiment, the project adopted FAOSTAT's publicly accessible bulk download catalogue instead.

### Public Catalogue Exploration

The public catalogue was accessed successfully using:

`https://bulks-faostat.fao.org/production/datasets_E.json`

The catalogue returned 69 available datasets.

The QCL dataset was selected because it contains agricultural production statistics, including harvested area, production, and yield.

### Dataset Schema Inspection

The main normalized CSV contains 14 columns:

| Column | Description |
|---|---|
| Area Code | Geographical area identifier |
| Area Code (M49) | UN M49 geographical code |
| Area | Country or geographical area |
| Item Code | Commodity identifier |
| Item Code (CPC) | Commodity classification code |
| Item | Commodity name |
| Element Code | Measurement element identifier |
| Element | Measurement type |
| Year Code | Year identifier |
| Year | Observation year |
| Unit | Measurement unit |
| Value | Recorded observation |
| Flag | Observation status |
| Note | Additional metadata |

The downloaded archive also contains reference files for geographical areas, elements, flags, and commodity codes.

**Important engineering considerations identified:**

1. Geographical and commodity identifiers must be preserved as strings because some codes contain leading zeros.
2. Measurement elements must be distinguished because production, harvested area, and yield represent different metrics.
3. Observation flags must be retained to preserve information about the reliability and origin of reported figures.
4. Zero values should not automatically be interpreted as missing observations.
5. Missing values must be handled according to their meaning rather than replaced indiscriminately.
6. Measurement units must be considered when aggregating or comparing observations.

### Full Dataset Profiling

The complete FAOSTAT normalized dataset was loaded and profiled using PySpark.

**Overall dataset findings:**

- 4,209,110 records
- 14 columns
- 244 geographical areas
- 301 commodities
- 8 measurement elements
- Year coverage from 1961 to 2024

**Nigeria-specific findings:**

- 17,539 records
- 121 commodities
- Year coverage from 1961 to 2024

**Measurement element distribution:**

| Measurement element | Records |
|---|---:|
| Production | 1,643,611 |
| Area harvested | 893,484 |
| Yield | 839,819 |
| Producing Animals/Slaughtered | 313,081 |
| Yield/Carcass Weight | 262,197 |
| Stocks | 180,294 |
| Milk Animals | 45,801 |
| Laying | 30,823 |

**Observation flag distribution:**

| Flag | Records |
|---|---:|
| A | 1,837,346 |
| E | 1,778,734 |
| I | 387,557 |
| X | 111,118 |
| M | 94,355 |

**Data quality findings:**

| Data quality metric | Result |
|---|---:|
| Missing Value records | 94,355 |
| Missing Note records | 4,125,820 |
| Zero-value records | 73,671 |
| Invalid numeric records | 0 |

The missing `Value` count corresponds to the number of observations carrying the `M` flag.

The `Note` column is largely empty because it contains optional supplementary information.

The overall average across all measurement values is not treated as an analytical indicator because the dataset combines different measurement units and types.

**Generated profiling reports:**

- `reports/faostat_schema_inspection.json`
- `reports/faostat_full_profile.json`

## Data Quality and Validation Strategy

Data quality will be treated as a core part of the pipeline rather than an afterthought.

Planned validation checks include:

- Required-column validation.
- Data type validation.
- Null-value assessment.
- Duplicate observation detection.
- Valid geographical and commodity identifiers.
- Valid measurement units.
- Valid observation years.
- Observation flag validation.
- Numeric range checks.
- Cross-layer record reconciliation.
- Agricultural and climate date alignment.

Missing observations will be distinguished from genuine zero values, and source flags will be retained for downstream analysis.

## Analytical Questions

The completed analytical datasets will be used to investigate questions such as:

1. How has Nigerian crop production changed over time?
2. Which commodities have experienced the largest changes in production?
3. How have harvested areas and crop yields evolved?
4. How do rainfall patterns vary across the available historical period?
5. How have temperature patterns changed over time?
6. What statistical relationships exist between rainfall, temperature, and agricultural production?
7. Do different commodities exhibit different relationships with climate variables?

These analyses will account for differences in geographical coverage, measurement units, missing observations, and historical reporting practices.

## Reproducibility and Project Setup

### 1. Clone the Repository

```bash
git clone https://github.com/ebukachukwu-hub/nigeria-agri-climate-pipeline.git
```

### 2. Navigate into the Project

```bash
cd nigeria-agri-climate-pipeline
```

### 3. Create a Virtual Environment

```bash
python -m venv .venv
```

### 4. Activate the Environment

Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
```

### 5. Install Dependencies

```bash
pip install -r requirements.txt
```

### 6. Run FAOSTAT Catalogue Exploration

```bash
python -m src.ingestion.explore_faostat_catalogue
```

### 7. Download the FAOSTAT Dataset

```bash
python -m src.ingestion.download_faostat
```

### 8. Inspect the FAOSTAT Archive

```bash
python -m src.ingestion.inspect_faostat
```

### 9. Profile the Complete FAOSTAT Dataset

```bash
python -m src.analytics.profile_faostat
```

**Prerequisites:** A compatible Java installation is required for local PySpark execution. The commands above assume the project dependencies and source files have been installed or obtained successfully.

## Data and Version Control Policy

Large source datasets and generated intermediate data files are not committed to GitHub.

The repository tracks source code, configuration, documentation, tests, and selected analytical reports.

The `.gitignore` file excludes:

- Python virtual environments.
- Python cache files.
- Environment variables and secrets.
- Downloaded raw datasets.
- Extracted source CSV files.
- Generated intermediate data files.

This keeps the repository lightweight while allowing the pipeline to reproduce its datasets from their original sources.

## Future Improvements

Potential extensions include:

- Scheduling automated data ingestion.
- Incremental data processing.
- Delta Lake storage and transaction management.
- Cloud-based data storage and processing.
- Pipeline monitoring and logging.
- Automated data quality reporting.
- Interactive dashboards.
- More detailed geographical analysis.
- Additional agricultural and environmental data sources.

## Project Status

**Current stage:** FAOSTAT full dataset profiling completed. Detailed Nigeria-specific profiling is the next development milestone.

The project is being developed incrementally, with each milestone documented and version-controlled before the next stage begins.

## Author

**Kanikwu Enoch Echezonachukwu**

B.Sc. Statistics | Data Engineering | Data Analytics

- **GitHub:** https://github.com/ebukachukwu-hub
- **LinkedIn:** https://www.linkedin.com/in/enoch-kanikwu-470053305

---

*This project is an independent data engineering and analytical project focused on agricultural data and climate trends in Nigeria.*