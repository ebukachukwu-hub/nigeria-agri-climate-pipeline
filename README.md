# Nigeria Crop Production & Climate Trends Pipeline

An end-to-end data engineering project that integrates Nigerian agricultural production statistics with climate observations to explore historical relationships between crop production, rainfall and temperature.

## Project Overview

Agricultural production is influenced by several factors, including weather patterns, farming practices, soil conditions and economic conditions.

This project combines agricultural data from **FAOSTAT** with climate observations from **NASA POWER** to build a reusable data pipeline for Nigeria.

The pipeline will use PySpark to ingest, clean, validate, integrate and transform datasets into analytical tables.

**Important:** This is an exploratory data engineering and analysis project. Observed relationships between climate indicators and agricultural production will not be presented as proof of causation.

## Objectives

- Ingest agricultural production data from FAOSTAT.
- Retrieve historical rainfall and temperature observations from NASA POWER.
- Build a Bronze, Silver and Gold data architecture.
- Standardise crop names, dates, measurements and geographical identifiers.
- Handle missing values and investigate data-quality anomalies.
- Integrate agricultural and climate datasets.
- Produce analytical datasets for reporting and visualisation.
- Implement automated data-quality tests.
- Automate testing with GitHub Actions.

## Data Sources

### 1. FAOSTAT

Source: https://www.fao.org/faostat/

Expected variables:
- Country
- Crop
- Year
- Production quantity
- Yield
- Harvested area
- Measurement units

### 2. NASA POWER

Source: https://power.larc.nasa.gov/

Expected climate indicators:
- Precipitation
- Temperature
- Monthly observations
- Geographic coordinates

The precise indicators, units, geographical coverage and date ranges will be documented after inspecting the source responses.

## Technology Stack

| Area | Technology |
|---|---|
| Programming | Python |
| Data Engineering | PySpark, Apache Spark |
| Data Sources | FAOSTAT, NASA POWER |
| Data Processing | Pandas, PyArrow |
| Testing | pytest |
| API Requests | Requests |
| Environment Management | Python venv |
| Version Control | Git, GitHub |
| CI/CD | GitHub Actions |
| Reporting | Python-based analytics and visualisations |

## Pipeline Architecture

```text
FAOSTAT API             NASA POWER API
     |                        |
     v                        v
     +----------+-------------+
                |
                v
         BRONZE LAYER
        Raw source data
                |
                v
         SILVER LAYER
     Cleaning and validation
     Standardisation
     Climate-agriculture integration
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

## Project Structure

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
├── .github/
│   └── workflows/
│
├── requirements.txt
├── README.md
└── .gitignore
```

## Development Milestones

| Milestone | Deliverable | Status |
|---|---|---|
| 1 | Project workspace and environment setup | In progress |
| 2 | FAOSTAT data ingestion | Pending |
| 3 | NASA POWER climate data ingestion | Pending |
| 4 | Bronze layer implementation | Pending |
| 5 | Silver layer cleaning and integration | Pending |
| 6 | Gold analytical datasets | Pending |
| 7 | Data-quality validation and anomaly investigation | Pending |
| 8 | Automated testing with pytest | Pending |
| 9 | GitHub Actions workflow | Pending |
| 10 | Documentation, reporting and final analysis | Pending |

## Data Quality and Validation

The pipeline will implement checks for:

- Missing or invalid values.
- Duplicate records.
- Unexpected crop names and measurement units.
- Invalid geographical coordinates.
- Inconsistent dates and reporting periods.
- Unreasonable production, yield and climate values.
- Unexpected changes in record counts after transformations.

Validation rules will be documented and tested rather than silently dropping questionable records.

## Analytical Direction

The final Gold datasets will support questions such as:

1. How has the production of selected Nigerian crops changed over time?
2. How have rainfall and temperature patterns changed across selected locations?
3. Do changes in annual climate indicators correspond with changes in crop yield?
4. Which crops and geographical areas show notable historical patterns?
5. Where are the limitations of the available data?

## Project Status

**Current phase:** Project setup and environment configuration.

Implementation progress will be updated as each milestone is completed and tested.

## Author

**Enoch Kanikwu**

- GitHub: https://github.com/ebukachukwu-hub
- LinkedIn: https://www.linkedin.com/in/enoch-kanikwu-470053305

---

*This project is developed for educational, analytical and portfolio purposes. Findings will be interpreted with appropriate attention to data quality, geographical coverage and statistical limitations.*