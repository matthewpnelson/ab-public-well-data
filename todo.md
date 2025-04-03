# To-Do List: Alberta Well Data Normalization Project

## Phase 1: Setup & Data Acquisition

-   [ ] **1.1. Project Setup:**
    -   [ ] Create project directory structure (e.g., `src/`, `data/`, `output/`).
    -   [ ] Initialize Git repository (optional but recommended).
    -   [ ] Create `requirements.txt` listing dependencies (e.g., `polars`, `requests`).
    -   [ ] Create main script file (e.g., `main.py`).
-   [ ] **1.2. Develop Downloader Module (`src/downloader.py`):**
    -   [ ] Implement function to download AER ST1 CSV (`WellLicenceAllAB.csv`).
    -   [ ] Implement function to download AER ST37 ZIP (`ST37.zip`).
    -   [ ] Implement logic to determine the latest valid Petrinex URL (use trial-and-error: check current month, then previous, etc.).
    -   [ ] Implement function to download latest Petrinex CSV.
    -   [ ] Add robust error handling for downloads (HTTP errors, timeouts, file not found).
    -   [ ] Add logging for download progress and errors.
    -   [ ] Define target download directory (e.g., `data/raw/`).

## Phase 2: Data Loading & Validation

-   [ ] **2.1. Develop Loader Module (`src/loader.py`):**
    -   [ ] Implement function using Polars to read AER ST1 CSV.
        -   [ ] Define expected schema/dtypes based on `example-data/WellLicenceAllAB.csv`.
        -   [ ] Load all columns initially.
    -   [ ] Implement function using Polars to read AER ST37 data.
        -   [ ] Add logic to unzip `ST37.zip`.
        -   [ ] Identify and read the relevant file(s) within the zip (requires clarification - see Q2).
        -   [ ] Identify the primary data file (likely `WellList.txt` based on context).
        -   [ ] Define expected schema/dtypes. (**Requires input**: Obtain column names/types/positions from `St37-Listofwellslayout.pdf` or user). Use `example-data/WellList.txt` for reference.
        -   [ ] Load all columns initially.
    -   [ ] Implement function using Polars to read Petrinex CSV.
        -   [ ] Define expected schema/dtypes based on available examples or expected structure.
        -   [ ] Select required columns (Latest Oil/Gas Vol).
    -   [ ] Implement data validation checks within loading functions:
        -   [ ] Verify required columns exist based on defined schemas.
        -   [ ] Check if essential identifier columns (like UWI, Licence) have excessive nulls.
        -   [ ] Add assertions to ensure loaded data meets basic expectations. If checks fail, raise an informative error.

## Phase 3: Data Normalization & Enrichment

-   [ ] **3.1. Develop Normalization Module (`src/normalize.py`):**
    -   [ ] Implement function to join ST1 and ST37 DataFrames (likely on UWI). Determine join type (left, inner).
    -   [ ] Implement function to join Petrinex data to the merged ST1/ST37 DataFrame (determine join key: UWI or Licence?).
    -   [ ] Implement data filling logic based on shared Well Licence.
        -   [ ] Identify specific columns to target for filling (requires clarification - see Q4).
        -   [ ] Develop Polars logic (`group_by`, `forward_fill`/`backward_fill`, `first`/`last`, or custom function) to fill missing values within each licence group. Define rules for handling conflicts (requires clarification - see Q4).
        -   **Note:** Finalize the specific columns and conflict resolution strategy after initial data loading/joining.

## Phase 4: Output & Orchestration

-   [ ] **4.1. Develop Output Module (`src/output.py`):**
    -   [ ] Implement function to save the final normalized DataFrame to Parquet format (preferred).
    -   [ ] Add option to save as CSV as well.
    -   [ ] Define output directory (e.g., `output/`).
    -   [ ] Ensure output filename is `normalized_wells_ab.parquet` (and potentially `.csv`).
    -   [ ] Implement logic to optionally save intermediate DataFrames if needed (requires clarification - see Q7).
    -   [ ] Determine if saving intermediate files is needed (Decision: Likely not essential if focusing on final output, keep modularity in code instead).
-   [ ] **4.2. Update Main Script (`main.py`):**
    -   [ ] Import functions from `downloader`, `loader`, `normalize`, `output`.
    -   [ ] Orchestrate the workflow: download -> load -> validate -> normalize -> save.
    -   [ ] Add argument parsing (e.g., using `argparse`) for configuration like output paths or formats (optional).
    -   [ ] Add top-level error handling and logging.

## Phase 5: Testing & Documentation

-   [ ] **5.1. Testing:**
    -   [ ] Manually review sample output data for correctness.
    -   [ ] Add basic unit tests for helper functions (e.g., Petrinex URL generation, specific Polars transformations) (optional).
-   [ ] **5.2. Documentation:**
    -   [ ] Create a `README.md` file:
        -   [ ] Project description.
        -   [ ] Setup instructions (`git clone`, `pip install -r requirements.txt`).
        -   [ ] How to run the script.
        -   [ ] Description of the output.
    -   [ ] Add comments and docstrings to Python code.
