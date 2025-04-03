# Project Requirements Document: Alberta Well Data Normalization Project

## 1. Introduction & Goal

Develop a Python program to download well data from three specified Alberta sources (AER ST1, AER ST37, Petrinex), process it using the Polars library, and create a single, normalized table containing critical well information. The program aims to produce a reliable and maintainable dataset by utilizing performant data handling techniques and incorporating data quality checks.

## 2. Requirements

### 2.1. Data Acquisition
- Download well licence data from AER ST1 (`WellLicenceAllAB.csv`).
- Download well drilling data from AER ST37 (`ST37.zip`, containing relevant files).
- Download the latest monthly production volumes from Petrinex (`Vol*.csv`). The specific URL needs logic to determine the latest month.

### 2.2. Data Processing
- Use Python for scripting.
- Use the Polars library for all dataframe operations.
- Read and parse data from the downloaded CSV and potentially zipped files.

### 2.3. Data Normalization & Enrichment
- Load all available columns initially from ST1 and ST37 sources. Column selection/filtering can be performed later in the process.
- Extract "latest month oil production volume" and "latest month gas production volume" from the Petrinex source.
- Combine data from all sources into a single normalized table (e.g., a "Well Header Table").
- Implement logic to fill missing data points by leveraging information across sources. The exact columns and strategy will be refined after initial data loading and analysis.

### 2.4. Data Quality & Error Handling
- Implement checks to validate assumptions about the structure or content of the source data (e.g., expected columns, data types) using explicit schemas during loading.
- Ensure the program fails explicitly if data validation checks fail (e.g., unexpected schema changes, invalid data). Do not proceed with potentially corrupted data.

### 2.5. Output & Storage
- Store intermediate processing results (optional but recommended) and the final normalized table.
- Use Parquet or CSV format for storing data files.
- The primary final output file should be named `normalized_wells_ab.parquet`.

### 2.6. Project Structure
- Deliver the solution as a well-organized Python project (e.g., including source files `.py`, potentially a `requirements.txt` or similar).
- The structure should be modular, allowing components (download, load, normalize, output) to be run sequentially, mimicking a Directed Acyclic Graph (DAG) workflow.

## 3. Data Sources

### 3.1. AER ST1 (Well Licences)
- **URL:** `https://www2.aer.ca/t/Production/views/COM-WellLicenceAllList/WellLicenceAllAB.csv`
- **Data:** General well licence information.
- **Columns:** Load all available columns initially. Schema details (column names, types) are expected to be derived from the data.

### 3.2. AER ST37 (Drilling Data)
- **URL:** `https://www.aer.ca/documents/sts/st37/st37/ST37.zip`
- **Data:** Detailed drilling and well event information. Requires unzipping.
- **Columns:** Load all available columns initially. Schema details (column names, types) are expected to be derived from the accompanying `St37-Listofwellslayout.pdf` document (provided by user or inferred if necessary).

### 3.3. Petrinex (Production Volumes)
- **URL Pattern:** `https://www.petrinex.gov.ab.ca/publicdata/API/Files/AB/Vol/{YYYY-MM}/CSV`
    - **Note:** Requires logic to dynamically determine the latest `{YYYY-MM}` path segment. The example `2025-02` in the original description is illustrative. Implement a trial-and-error approach (e.g., check current month, previous month) to find the latest available data.
- **Data:** Monthly production volumes.
- **Columns:** Latest month oil production volume, latest month gas production volume (associated with a well identifier, likely UWI or Licence).

## 4. Technical Specifications

- **Language:** Python (e.g., 3.9+)
- **Core Library:** Polars
- **Other Potential Libraries:** `requests` (for downloading), `zipfile` (for ST37), `datetime` (for Petrinex URL).
- **Data Formats:** CSV (input), Parquet / CSV (output). Parquet is preferred for performance and type preservation.
- **Data Loading:** Utilize explicit schemas when reading data with Polars.

## 5. Success Criteria

- The program successfully downloads data from all three sources.
- The program uses Polars effectively for data manipulation.
- A normalized table is created containing data merged from the sources.
- Evidence of data filling logic (especially across UWIs within a licence) is present and functional (strategy defined post-loading).
- The program includes data validation steps (using explicit schemas) and fails gracefully on invalid or unexpected source data.
- The final output is stored in the specified format (`normalized_wells_ab.parquet`).
- The code is delivered as a structured, modular Python project.

## 6. Out of Scope

- Building a UI or API for the data.
- Real-time data processing or scheduling.
- Handling historical Petrinex data beyond the latest available month.
- Complex statistical analysis or machine learning on the data.
- Automated deployment or infrastructure setup.
