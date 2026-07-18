# Econometric Data Pipeline: World Bank API Ingestion & Sanitization Engine

This repository hosts a production-grade, functional ETL (Extract, Transform, Load) architecture developed in R. It is designed to programmatically harvest, audit, and clean panel data from the World Bank API. 

Built with the technical rigor required for top-tier **Pre-Doctoral Fellowships** and quantitative macroeconomic research, the pipeline enforces strict data integrity constraints to ensure 100% reproducible empirical analysis.

---

## Architectural Breakdown & Data Integrity

Unlike standard scripting approaches, this pipeline decouples network operations from computational transformations, mitigating common structural biases in panel data preparation:

### 1. Functional Purity & Testability
* **Decoupled E&T Layers:** The ingestion layer (`Extract`) and the data cleaning layer (`Transform`) are completely isolated. The transformation pipeline acts as a **pure function**, devoid of implicit global dependencies or hidden network side-effects. This design enables seamless unit-testing and deterministic data validation.
* **Resilient Network Ingestion:** Implements automated exponential backoff retry algorithms to handle remote server instability (HTTP 5xx/429 errors). System exceptions are explicitly captured and logged rather than silently suppressed, providing complete observability.

### 2. Methodological Rigor & Econometric Defenses
* **Mitigation of Aggregation Bias:** To prevent severe measurement errors in country-level regressions, the engine programmatically ingests metadata to filter out macro-regional and institutional aggregates (`region != "Aggregates"`). This ensures the dataset isolates sovereign economic entities exclusively.
* **Defensive Panel Imputation:** Missing data points within panel structures often trigger selection biases via listwise deletion. This framework deploys bounded intra-country linear interpolation (`zoo::na.approx`). To prevent runtime crashes on sparse country profiles (e.g., conflict-affected states with singular data entries), the imputation layer features an assertive validation guardrail (`sum(!is.na(x)) > 1`).
* **Data Quality Audit Trail:** The pipeline natively monitors data loss across the execution lifecycle, tracking initial-to-final row delta counts to flag anomalous payload drops prior to storage persistence.

---

##  Execution & Replication

### 1. Prerequisites
Ensure your local R environment has the necessary dependencies installed:
```R
install.packages(c("tidyverse", "wbstats", "stringi", "writexl", "zoo"))

### 2. Replication Steps
To replicate the dataset generation and verify the pipeline logs, clone this repository and source the master orchestration script:

# Clone the repository
git clone [https://github.com/augusto-bentemuler/worldbank-data-pipeline.git](https://github.com/augusto-bentemuler/worldbank-data-pipeline.git)

# Navigate into the project boundary
cd worldbank-data-pipeline

# Run the script from your R console:
source("src/pipeline.R")
```
## Standard Outputs & Schema Tracking

Upon a successful runtime execution, the pipeline handles target indicators (NY.GDP.PCAP.CD, SP.DYN.LE00.IN, SP.POP.TOTL), normalizes character encoding to ASCII format (handling string discrepancies), maps timestamps to ISO 8601 standards, and serializes the structured data into:

/clean_worldbank_data.csv (Optimized for cross-platform data-science environments)

/clean_worldbank_data.xlsx (Formatted for immediate descriptive inspection)
