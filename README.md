# Nigeria Agricultural Production & Climate Trends Pipeline

An end-to-end data engineering project that integrates historical agricultural production data from **FAOSTAT** with climate observations from **NASA POWER** to investigate relationships between agricultural productivity and climate conditions in Nigeria.

The project demonstrates practical data engineering skills through data ingestion, validation, profiling, transformation, data quality assessment and the development of an analytical data pipeline using Python, Apache Spark and a medallion architecture.

---

## 1. Project Overview

Agricultural production is influenced by several factors, including rainfall, temperature, land availability and agricultural practices.

This project aims to build a reproducible data pipeline that combines historical agricultural production records with climate observations to investigate how changes in climate conditions relate to agricultural production and yield in Nigeria.

The pipeline will use:

- **FAOSTAT:** Historical agricultural production, harvested area and yield data.
- **NASA POWER:** Historical meteorological and climate observations.

The resulting datasets will be cleaned, transformed and prepared for analytical use.

The project will initially focus on Nigeria, with the possibility of expanding to other African countries in future iterations.

## 2. Project Objectives

The main objectives are to:

1. Acquire historical agricultural production data from FAOSTAT.
2. Retrieve historical climate observations from NASA POWER.
3. Build reliable and reproducible ingestion processes.
4. Implement data validation and quality checks.
5. Use Apache Spark for large-scale data processing.
6. Implement a medallion architecture comprising Bronze, Silver and Gold layers.
7. Integrate agricultural and climate datasets using appropriate geographical and temporal dimensions.
8. Investigate relationships between rainfall, temperature, agricultural production and crop yield.
9. Develop analytical datasets that can support data visualisation and further statistical analysis.

## 3. Data Sources

### 3.1 FAOSTAT – Agricultural Production Data

**Source:** Food and Agriculture Organization of the United Nations.

Website: https://www.fao.org/faostat/

The project uses the FAOSTAT QCL dataset:

**Dataset code:** QCL

**Dataset name:** Production: Crops and livestock products.

Public bulk catalogue:

https://bulks-faostat.fao.org/production/datasets_E.json

Dataset download:

https://bulks-faostat.fao.org/production/Production_Crops_Livestock_E_All_Data_(Normalized).zip

The dataset contains historical agricultural production statistics for countries and territories worldwide.

The principal fields include:

- Area Code
- Area Code (M49)
- Area
- Item Code
- Item Code (CPC)
- Item
- Element Code
- Element
- Year Code
- Year
- Unit
- Value
- Flag
- Note

The main agricultural measurements of interest are:

| Measurement | Description | Unit |
|---|---|---|
| Production | Quantity of agricultural output | Tonnes (t) |
| Area harvested | Land area harvested | Hectares (ha) |
| Yield | Agricultural output per harvested area | kg/ha |

Other FAOSTAT measurements include livestock production, stocks, laying and carcass weight.

### 3.2 NASA POWER – Climate Data

**Source:** NASA Prediction of Worldwide Energy Resources.

Website: https://power.larc.nasa.gov/

NASA POWER provides meteorological and solar-related observations that can support agricultural and environmental analysis.

The planned climate variables include:

- Precipitation.
- Mean temperature.
- Minimum temperature.
- Maximum temperature.

The project will retrieve climate observations for relevant Nigerian geographical locations and align them with the agricultural dataset using appropriate temporal and spatial dimensions.

**Status:** Climate data ingestion and processing are upcoming milestones.

---

## 4. Technology Stack

| Technology | Purpose |
|---|---|
| Python | Pipeline development and automation |
| Apache Spark | Distributed data processing |
| PySpark | Python interface for Spark |
| Pandas | Data inspection and supporting analysis |
| PyArrow | Columnar data interoperability |
| Requests | HTTP requests and data ingestion |
| JSON | Metadata and profiling reports |
| pytest | Automated testing |
| Git | Version control |
| GitHub | Source code management and project documentation |
| VS Code | Development environment |

### Development Environment

- Operating System: Windows 11
- Python: 3.14.6
- Java: OpenJDK 25.0.4 LTS (Temurin)
- Apache Spark: 4.3.0
- PySpark: 4.3.0
- Git: Version control

Pandas is maintained below version 3.0 in the current environment for compatibility with the PySpark setup.

---

## 5. Project Architecture

The project follows the **Medallion Architecture**, separating data into three progressive layers.

```text
                 DATA SOURCES
                      |
          +-----------+-----------+
          |                       |
       FAOSTAT                NASA POWER
          |                       |
          v                       v
     DATA INGESTION          DATA INGESTION
          |                       |
          +-----------+-----------+
                      |
                      v
                BRONZE LAYER
             Raw ingested datasets
                      |
                      v
                SILVER LAYER
           Cleaned and validated data
                      |
                      v
                 GOLD LAYER
          Integrated analytical datasets
                      |
                      v
              ANALYTICS & INSIGHTS
```

### Bronze Layer

The Bronze layer stores ingested source data with minimal modification.

Responsibilities:

- Preserve source data.
- Maintain ingestion metadata.
- Record source information.
- Support reproducibility and traceability.

### Silver Layer

The Silver layer contains cleaned and validated datasets.

Responsibilities:

- Handle missing values appropriately.
- Standardise column names and data types.
- Validate measurement units.
- Remove or investigate duplicate records.
- Standardise dates and geographical identifiers.
- Prepare datasets for integration.

### Gold Layer

The Gold layer contains analytical datasets designed for reporting and downstream analysis.

Potential outputs include:

- Annual crop production trends.
- Annual crop yield trends.
- Historical rainfall trends.
- Temperature trends.
- Crop production and climate integration tables.
- Climate and agricultural productivity indicators.

---

## 6. Repository Structure

```text
nigeria-agri-climate-pipeline/
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
│   ├── faostat_full_profile.json
│   └── nigeria_agricultural_profile.json
│
├── src/
│   ├── ingestion/
│   │   ├── explore_faostat.py
│   │   ├── explore_faostat_catalogue.py
│   │   ├── download_faostat.py
│   │   └── inspect_faostat.py
│   │
│   ├── analytics/
│   │   ├── profile_faostat.py
│   │   └── profile_nigeria.py
│   │
│   └── __init__.py
│
├── tests/
│
├── .gitignore
├── README.md
└── requirements.txt
```

*Note: The structure represents the current project organisation and planned directories. Additional modules will be added as the pipeline develops.*

---

## 7. Implementation Progress

### Milestone 1: FAOSTAT API Investigation

The initial investigation explored the FAOSTAT API endpoint:

`https://faostatservices.fao.org/api/v1/en/groupsanddomains`

The request returned HTTP 401 with the response:

`Missing Authorization Header`

The investigation established that the API endpoint used in the experiment required an authorization header.

Rather than proceeding with an unauthenticated API request, the project adopted the publicly accessible FAOSTAT bulk download catalogue.

**Outcome:** Public bulk catalogue selected as the ingestion source.

### Milestone 2: FAOSTAT Catalogue Exploration

The public FAOSTAT catalogue was accessed successfully.

**Catalogue URL:**

`https://bulks-faostat.fao.org/production/datasets_E.json`

Results:

| Attribute | Result |
|---|---|
| HTTP response | 200 |
| Available datasets | 69 |
| Selected dataset | QCL |
| Dataset name | Production: Crops and livestock products |
| Catalogue update date | 31 December 2025 |
| Approximate file size | 33,127 KB |
| Catalogue-reported rows | 4,209,110 |

The catalogue was saved locally as:

`data/raw/faostat_catalogue.json`

### Milestone 3: FAOSTAT Data Ingestion and Validation

A Python ingestion script was developed:

`src/ingestion/download_faostat.py`

The script:

1. Reads the locally saved FAOSTAT catalogue.
2. Identifies the QCL dataset.
3. Retrieves the dataset download location.
4. Downloads the archive using streaming requests.
5. Writes the download to a temporary `.part` file.
6. Checks the downloaded file size where the server provides a content length.
7. Validates the ZIP archive using Python's ZIP utilities.
8. Calculates a SHA-256 checksum.
9. Saves download metadata.
10. Renames the validated temporary file to its final archive name.

The downloaded archive is stored locally as:

`data/raw/faostat_crop_production.zip`

Download metadata is stored as:

`data/raw/faostat_download_metadata.json`

Raw datasets are excluded from Git version control.

### Milestone 4: Raw Dataset Schema Inspection

The script:

`src/ingestion/inspect_faostat.py`

was developed to inspect the downloaded archive without manually extracting every file.

Five CSV files were identified:

1. Production_Crops_Livestock_E_All_Data_(Normalized).csv
2. Production_Crops_Livestock_E_AreaCodes.csv
3. Production_Crops_Livestock_E_Elements.csv
4. Production_Crops_Livestock_E_Flags.csv
5. Production_Crops_Livestock_E_ItemCodes.csv

The main normalized dataset contains 14 columns.

The schema inspection report is saved as:

`reports/faostat_schema_inspection.json`

Important observations:

- Geographic and commodity identifiers should be preserved as strings.
- Some geographical and CPC identifiers contain leading zeros.
- Measurement elements must be interpreted alongside their units.
- Production, harvested area and yield represent different measurements.
- Zero-valued observations should not automatically be classified as missing.
- FAOSTAT flags provide important information about the status of reported observations.

### Milestone 5: Full FAOSTAT Dataset Profiling

The full normalized dataset was profiled using Apache Spark.

Script:

`src/analytics/profile_faostat.py`

The main dataset was read with schema inference disabled to preserve identifiers as strings.

The script extracted the main normalized CSV from the ZIP archive and performed large-scale profiling.

**Full dataset results:**

| Metric | Result |
|---|---:|
| Total records | 4,209,110 |
| Columns | 14 |
| Unique geographical areas | 244 |
| Unique commodities | 301 |
| Measurement elements | 8 |
| Historical coverage | 1961–2024 |
| Nigeria records | 17,539 |
| Nigeria commodities | 121 |

**Global measurement distribution:**

| Measurement | Records |
|---|---:|
| Production | 1,643,611 |
| Area harvested | 893,484 |
| Yield | 839,819 |
| Producing Animals/Slaughtered | 313,081 |
| Yield/Carcass Weight | 262,197 |
| Stocks | 180,294 |
| Milk Animals | 45,801 |
| Laying | 30,823 |

**Global flag distribution:**

| Flag | Records |
|---|---:|
| A | 1,837,346 |
| E | 1,778,734 |
| I | 387,557 |
| X | 111,118 |
| M | 94,355 |

The global dataset contained 94,355 missing Value observations, 73,671 zero-valued observations and no invalid numeric values in the profiling exercise.

The overall average Value across all records is not suitable as a meaningful agricultural statistic because the dataset contains measurements with different units.

The full profiling report is saved as:

`reports/faostat_full_profile.json`

### Milestone 6: Nigeria-Specific Agricultural Profiling

A dedicated Nigeria profiling script was developed:

`src/analytics/profile_nigeria.py`

The script filters the main FAOSTAT dataset to Nigerian records and investigates:

- Dataset dimensions.
- Commodity coverage.
- Measurement elements and units.
- Top commodities by record count.
- Historical year coverage.
- Crop metric intersections.
- Same-year availability of agricultural measurements.
- Duplicate records.
- Missing values and FAOSTAT flags.

**Nigeria-specific results:**

| Metric | Result |
|---|---:|
| Total records | 17,539 |
| Unique commodities | 121 |
| Unique measurement elements | 8 |
| Unique years | 64 |
| First year | 1961 |
| Last year | 2024 |

#### Agricultural Metric Distribution

| Measurement | Records | Commodities |
|---|---:|---:|
| Area harvested | 3,818 | 62 |
| Production | 6,700 | 108 |
| Yield | 3,757 | 63 |

#### Measurement Units

The main agricultural measurement units identified were:

| Measurement | Unit | Records |
|---|---|---:|
| Area harvested | ha | 3,818 |
| Production | t | 6,636 |
| Production | 1000 No | 64 |
| Yield | kg/ha | 3,693 |
| Yield | No/An | 64 |

Other records include livestock-related measurements expressed in units such as animals, thousand animals, grams per animal and kilograms per animal.

#### Commodity Metric Coverage

The profiling identified:

- 62 commodities with all three primary agricultural metrics somewhere in their historical records.
- 46 commodities without all three metrics.
- 62 commodities with at least one year containing all three metrics simultaneously.

Examples include:

- Rice.
- Maize.
- Cassava.
- Sorghum.
- Millet.
- Soybeans.
- Cocoa beans.
- Groundnuts.
- Yams.
- Sugar cane.

Some entries are aggregate commodity categories rather than individual crops. These will require classification before crop-specific analytical modelling.

#### Same-Year Availability

The same-year coverage analysis checks whether Production, Area harvested and Yield are available for the same commodity in the same year.

Examples of historical coverage include:

| Commodity | Years with all three metrics |
|---|---|
| Rice | 1961–2024 |
| Cassava, fresh | 1961–2024 |
| Maize (corn) | 1961–2024 |
| Bananas | 1991–2024 |
| Green garlic | 2007–2024 |
| Kenaf | 1981–2024 |
| Plantains and cooking bananas | 1961–1990 |

This distinction is important when preparing agricultural observations for integration with annual climate data.

#### Historical Continuity

A total of 285 commodity-measurement-unit series were investigated.

| Check | Result |
|---|---:|
| Total series | 285 |
| Series with internal missing years | 0 |
| Series without internal missing years | 285 |

The absence of internal gaps means that each series has uninterrupted observations between its own first and last recorded years. It does not mean every series covers the full 1961–2024 period.

#### Duplicate Investigation

Duplicate checks were performed using the following identifying columns:

- Area Code
- Item Code
- Element Code
- Year

Results:

| Check | Result |
|---|---:|
| Duplicate groups | 0 |
| Records in duplicate groups | 0 |

#### Missing Value Investigation

| Column | Missing records |
|---|---:|
| Value | 285 |
| Flag | 0 |
| Note | 17,100 |
| Unit | 0 |
| Year | 0 |

All 285 records with missing Value observations carried the FAOSTAT flag `M`.

This confirms that the missing observations were explicitly marked as missing in the source data.

Missing observations are preserved as missing rather than being replaced with zero.

**Nigeria profiling report:**

`reports/nigeria_agricultural_profile.json`

---

## 8. Data Quality and Validation Strategy

Data quality is an important part of this project.

The pipeline will implement checks covering:

### Completeness

- Identify missing values.
- Distinguish between missing observations and valid zero values.
- Investigate missing years within time series.

### Uniqueness

- Identify duplicate observations.
- Validate geographical, commodity and measurement identifiers.

### Validity

- Validate numeric measurements.
- Check expected data types.
- Preserve identifiers containing leading zeros.

### Consistency

- Standardise geographical identifiers.
- Validate measurement units.
- Ensure that production, harvested area and yield are not mixed incorrectly.
- Maintain consistent time dimensions between agricultural and climate datasets.

### Traceability

- Preserve original source data.
- Maintain ingestion metadata.
- Record transformation steps.
- Retain reproducible profiling reports.

---

## 9. Planned Analytical Work

Once the agricultural and climate datasets have been prepared, the project will support several analytical investigations.

### Agricultural Production Trends

Investigate historical changes in the production of selected Nigerian crops.

### Crop Yield Analysis

Examine historical changes in agricultural productivity.

### Rainfall Analysis

Investigate rainfall patterns and seasonal variations.

### Temperature Analysis

Examine changes in mean, minimum and maximum temperatures.

### Climate-Agriculture Integration

Integrate agricultural production and climate observations using compatible geographical and temporal dimensions.

### Statistical Analysis

Explore relationships between climate variables and agricultural outcomes using descriptive statistics, correlation analysis and suitable statistical models.

Any observed association will be interpreted carefully. Correlation does not establish causation, and other agricultural, economic and environmental factors may influence production.

---

## 10. Running the Project

### Step 1: Clone the Repository

```bash
git clone https://github.com/ebukachukwu-hub/nigeria-agri-climate-pipeline.git
cd nigeria-agri-climate-pipeline
```

### Step 2: Create a Virtual Environment

```bash
python -m venv .venv
```

Activate it on Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
```

### Step 3: Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 4: Explore the FAOSTAT Catalogue

```bash
python -m src.ingestion.explore_faostat_catalogue
```

### Step 5: Download the FAOSTAT Dataset

```bash
python -m src.ingestion.download_faostat
```

### Step 6: Inspect the Dataset Schema

```bash
python -m src.ingestion.inspect_faostat
```

### Step 7: Profile the Full FAOSTAT Dataset

```bash
python -m src.analytics.profile_faostat
```

### Step 8: Profile Nigerian Agricultural Data

```bash
python -m src.analytics.profile_nigeria
```

The scripts expect the downloaded and extracted dataset to be available in the project's local data directories.

---

## 11. Version Control and Data Management

Git is used to track source code, documentation, reports and project development.

The following are excluded from version control:

- Virtual environments.
- Python cache files.
- Environment variables.
- Downloaded raw datasets.
- Extracted source CSV files.
- Local Bronze, Silver and Gold data files.

The repository retains `.gitkeep` files to preserve the data directory structure.

Large source datasets are retrieved through the ingestion scripts rather than committed to GitHub.

---

## 12. Project Roadmap

| Milestone | Status |
|---|---|
| Project setup and environment configuration | Completed |
| FAOSTAT API investigation | Completed |
| FAOSTAT public catalogue exploration | Completed |
| FAOSTAT dataset ingestion | Completed |
| Download integrity validation | Completed |
| Raw dataset schema inspection | Completed |
| Full FAOSTAT profiling | Completed |
| Nigeria-specific agricultural profiling | Completed |
| Commodity metric coverage analysis | Completed |
| Same-year metric availability analysis | Completed |
| Duplicate and missing-value investigation | Completed |
| NASA POWER climate data ingestion | Upcoming |
| Climate data profiling and validation | Upcoming |
| Bronze layer implementation | Upcoming |
| Silver layer transformations | Upcoming |
| Agricultural and climate data integration | Upcoming |
| Gold analytical data models | Upcoming |
| Statistical analysis and visualisation | Upcoming |
| Automated testing and pipeline orchestration | Upcoming |

---

## 13. Current Project Status

The FAOSTAT ingestion and profiling stages have been completed.

The project has successfully established:

- A public-source agricultural data ingestion process.
- Archive integrity validation.
- Automated schema inspection.
- Full-dataset profiling using Apache Spark.
- Nigeria-specific agricultural profiling.
- Commodity-level agricultural metric coverage analysis.
- Same-year metric availability investigation.
- Duplicate and missing-value checks.
- Machine-readable profiling reports.

The next major development stage is the ingestion and profiling of NASA POWER climate observations.

This will establish the foundation for integrating agricultural and climate data through the project's Bronze, Silver and Gold layers.

---

## 14. Author

**Kanikwu Enoch Echezonachukwu**

B.Sc. Statistics | Data Engineering | Data Analytics

- **GitHub:** https://github.com/ebukachukwu-hub
- **LinkedIn:** https://www.linkedin.com/in/enoch-kanikwu-470053305

---

## 15. Disclaimer

This project is developed for educational, analytical and portfolio purposes.

FAOSTAT and NASA POWER are independent data sources with different collection methods, spatial resolutions, temporal coverage and measurement definitions.

Any integrated analysis must account for these differences before drawing conclusions.

Statistical relationships identified in the project should not be interpreted as proof of causation.