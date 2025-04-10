import polars as pl
import logging
import zipfile
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from ydata_profiling import ProfileReport

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_aer_st1(file_path: Path, generate_profile: bool = False) -> Optional[pl.DataFrame]:
    """
    Load the AER ST1 CSV file into a Polars DataFrame.
    
    Args:
        file_path: Path to the AER ST1 CSV file.
        generate_profile: Whether to generate a data profile report.
        
    Returns:
        A Polars DataFrame containing the AER ST1 data, or None if loading fails.
    """
    try:
        logging.info(f"Loading AER ST1 data from {file_path}")
        
        # Load the CSV file with Polars
        # We use the infer_schema_length parameter to ensure proper type inference
        df = pl.read_csv(
            file_path,
            infer_schema_length=10000,
            try_parse_dates=True
        )
        
        logging.info(f"Successfully loaded AER ST1 data: {df.shape[0]} rows, {df.shape[1]} columns")
        
        logging.info(df.head(5))
        logging.info(df.columns)
        
        # Only generate profile if requested
        if generate_profile:
            # If the profile file does not yet exist, generate it
            profile_file = "data/profiles/st1.html"
            profile_dir = Path("data/profiles")
            profile_dir.mkdir(parents=True, exist_ok=True)  # Create profiles directory if it doesn't exist
            
            logging.info(f"Generating profile report for AER ST1 data")
            profile = ProfileReport(df.to_pandas(), title="AER ST1 Profiling Report")
            profile.to_file(profile_file)
            logging.info(f"Profile report saved to {profile_file}")
        
        # Basic validation - check if essential columns exist
        essential_cols = ["01.Licence Number", "02.Company Name", "03.Latitude", "04.Longitude", "05.Surface Location", "08.Licence Status", "09.Licence Status Date", "10.Non-Routine Licence (Y or N)"]
        missing_cols = [col for col in essential_cols if col not in df.columns]
        
        if missing_cols:
            logging.error(f"Essential columns missing from AER ST1 data: {missing_cols}")
            return None
            
        # Check for excessive nulls in key columns
        for col in essential_cols:
            null_percentage = (df[col].null_count() / df.shape[0]) * 100
            if null_percentage > 50:
                logging.warning(f"Column {col} has {null_percentage:.2f}% null values")
        
        # Keep only essential columns
        df = df[essential_cols]
        
        # Rename Map
        rename_map = {
            "01.Licence Number": "License Number",
            "02.Company Name": "Company Name",
            "03.Latitude": "Latitude",
            "04.Longitude": "Longitude",
            "05.Surface Location": "Surface Location",
            "08.Licence Status": "License Status",
            "09.Licence Status Date": "License Status Date",
            "10.Non-Routine Licence (Y or N)": "Is Non-Routine"
        }
        
        # Rename columns
        df = df.rename(rename_map)
        
        # Output results to a staging directory
        staging_dir = Path("data/staging")
        staging_dir.mkdir(parents=True, exist_ok=True)
        
        # Save to parquet
        staging_file = staging_dir / "st1.parquet"
        df.write_parquet(staging_file)
        
        # Save to csv
        staging_file = staging_dir / "st1.csv"
        df.write_csv(staging_file)
        
        return df
    
    except Exception as e:
        logging.error(f"Error loading AER ST1 data: {e}")
        return None


def extract_st37_zip(zip_path: Path, extract_dir: Optional[Path] = None) -> Dict[str, Path]:
    """
    Extract the contents of the ST37 ZIP file.
    
    Args:
        zip_path: Path to the ST37 ZIP file.
        extract_dir: Directory to extract files to (defaults to same directory as ZIP).
        
    Returns:
        A dictionary mapping file names to their extracted paths.
    """
    if extract_dir is None:
        extract_dir = zip_path.parent / "st37_extracted"
    
    extract_dir.mkdir(parents=True, exist_ok=True)
    
    extracted_files = {}
    
    try:
        logging.info(f"Extracting {zip_path} to {extract_dir}")
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            file_list = zip_ref.namelist()
            logging.info(f"Found {len(file_list)} files in ZIP: {file_list}")
            
            for file_name in file_list:
                zip_ref.extract(file_name, extract_dir)
                extracted_files[file_name] = extract_dir / file_name
                
        logging.info(f"Successfully extracted {len(extracted_files)} files from ST37 ZIP")
        return extracted_files
        
    except Exception as e:
        logging.error(f"Error extracting ST37 ZIP file: {e}")
        return {}


def load_aer_st37(txt_path: Path, generate_profile: bool = False) -> Optional[pl.DataFrame]:
    """
    Load the AER ST37 data into a Polars DataFrame.
    
    Args:
        txt_path: Path to the ST37 TXT file.
        generate_profile: Whether to generate a data profile report.
        
    Returns:
        A Polars DataFrame containing the AER ST37 data, or None if loading fails.
    """
    try:
        if not txt_path.exists():
            logging.error(f"ST37 TXT file not found: {txt_path}")
            return None
            
        logging.info(f"Loading AER ST37 data from {txt_path}")
        
        # Load the fixed-width text file with Polars
        # column names come from documentation (St37-Listofwellslayout.pdf)
        columns = [
            "UWI Display format",
            "UWI",
            "Update-Flag",
            "Well-Name",
            "Field Code",
            "Pool Code",
            "OS-Area- Code",
            "OS-Dep- Code",
            "License-No",
            "License-Status",
            "License-Issue-Date",
            "Licensee-Code",
            "Agent-Code",
            "Operator-Code",
            "Fin-Drl-Date",
            "Well-Total-Depth",
            "Well-Stat-Code",
            "Well-Stat-Date",
            "Fluid_Short_Description",
            "Mode_Short_Description",
            "Type_Short_Description",
            "Structure_Short_Description",
            "Scheme_Type",
            "Scheme_Sub_Type"
        ]
        # For now, we'll use a simple delimiter-based approach as a placeholder
        # In a real implementation, proper field widths and column names would be defined
        df = pl.read_csv(
            txt_path,
            separator="\t",  # Assuming pipe-delimited format - adjust as needed
            infer_schema_length=10000,
            try_parse_dates=True,
            has_header=False
        )
        
        df.columns = columns
        
        logging.info(f"Successfully loaded AER ST37 data: {df.shape[0]} rows, {df.shape[1]} columns")
        
        logging.info(df.head(5))
        logging.info(df.columns)
        
        # Only generate profile if requested
        if generate_profile:
            # Generate profile report if it doesn't exist
            profile_file = "data/profiles/st37.html"
            profile_dir = Path("data/profiles")
            profile_dir.mkdir(parents=True, exist_ok=True)  # Create profiles directory if it doesn't exist
            
            logging.info(f"Generating profile report for AER ST37 data")
            profile = ProfileReport(df.to_pandas(), title="AER ST37 Profiling Report")
            profile.to_file(profile_file)
            logging.info(f"Profile report saved to {profile_file}")
        
        # Basic validation - check for key identifier column
        if "UWI" not in df.columns:
            logging.warning("UWI column not found in ST37 data - check column names")
            
        columns_to_keep = {
            "UWI Display format": "UWI Display",
            "UWI": "UWI",
            # "Update-Flag": "Update Flag",
            "Well-Name": "Well Name",
            "Field Code": "Field Code",
            "Pool Code": "Pool Code",
            "OS-Area- Code": "OS-Area",
            # "OS-Dep- Code": "OS-Dep",
            "License-No": "License",
            "License-Status": "License Status",
            "License-Issue-Date": "License Issue Date",
            "Licensee-Code": "Licensee Code",
            # "Agent-Code": "Agent Code",
            "Operator-Code": "Operator Code",
            "Fin-Drl-Date": "Rig Release Date",
            "Well-Total-Depth": "TD",
            "Well-Stat-Code": "Status Code",
            "Well-Stat-Date": "Status Date",
            "Fluid_Short_Description": "Primary Fluid",
            "Mode_Short_Description": "Mode",
            "Type_Short_Description": "Type",
            "Structure_Short_Description": "Structure",
            "Scheme_Type": "Scheme Type",
            "Scheme_Sub_Type": "Scheme Sub-Type"
        }    
            
        df = df[list(columns_to_keep.keys())]
        df = df.rename(columns_to_keep)
        
        staging_dir = Path("data/staging")
        staging_dir.mkdir(parents=True, exist_ok=True)
        
        staging_file = staging_dir / "st37.parquet"
        df.write_parquet(staging_file)
        
        staging_file = staging_dir / "st37.csv"
        df.write_csv(staging_file)
        
        return df
        
    except Exception as e:
        logging.error(f"Error loading AER ST37 data: {e}")
        return None


def load_petrinex(file_path: Path, generate_profile: bool = False) -> Optional[pl.DataFrame]:
    """
    Load the Petrinex CSV file into a Polars DataFrame.
    
    Args:
        file_path: Path to the Petrinex CSV file.
        generate_profile: Whether to generate a data profile report.
        
    Returns:
        A Polars DataFrame containing the Petrinex data, or None if loading fails.
    """
    logging.info(f"Loading Petrinex data from {file_path}")
    
    # Try various encodings and approaches
    encodings_to_try = [
        'latin-1',  # Also known as ISO-8859-1, handles most extended ASCII characters
        'cp1252',   # Windows Western European
        'utf-8-sig', # UTF-8 with BOM
        'utf-16',   # Unicode 16-bit
        'utf-8'     # Standard UTF-8
    ]
    
    for encoding in encodings_to_try:
        try:
            logging.info(f"Trying to load with encoding: {encoding}")
            
            # Define schema overrides for problematic columns
            schema_overrides = {
                "Hours": pl.String,  # Treat Hours as string to handle special values like '***'
            }
            
            # Define null values to handle special markers
            null_values = ["", "NA", "N/A", "NULL", "None", "***", "---"]
            
            # First approach: Using Polars directly with more robust settings
            df = pl.read_csv(
                file_path,
                infer_schema_length=50000,  # Increased to better infer schema
                try_parse_dates=True,
                encoding=encoding,
                schema_overrides=schema_overrides,
                null_values=null_values,
                ignore_errors=True  # Continue parsing even if there are errors
            )
            
            logging.info(f"Successfully loaded Petrinex data with {encoding} encoding: {df.shape[0]} rows, {df.shape[1]} columns")
            
            # Display sample data for verification
            logging.info(f"Sample data (first 5 rows):")
            logging.info(df.head(5))
            logging.info(f"Columns: {df.columns}")
            
            # Only generate profile if requested
            if generate_profile:
                # Generate profile report if it doesn't exist
                profile_file = "data/profiles/petrinex.html"
                profile_dir = Path("data/profiles")
                profile_dir.mkdir(parents=True, exist_ok=True)  # Create profiles directory if it doesn't exist
                
                logging.info(f"Generating profile report for Petrinex data")
                profile = ProfileReport(df.to_pandas(), title="Petrinex Profiling Report")
                profile.to_file(profile_file)
                logging.info(f"Profile report saved to {profile_file}")
            
            # Basic validation
            # We'll need to extract oil and gas production volumes
            # Since column names may vary, we'll look for columns containing relevant keywords
            
            # oil_cols = [col for col in df.columns if "oil" in col.lower() and "vol" in col.lower()]
            # gas_cols = [col for col in df.columns if "gas" in col.lower() and "vol" in col.lower()]
            # id_cols = [col for col in df.columns if "uwi" in col.lower() or "licence" in col.lower()]
            
            # if not oil_cols:
            #     logging.warning("No oil volume columns identified in Petrinex data")
            # if not gas_cols:
            #     logging.warning("No gas volume columns identified in Petrinex data")
            # if not id_cols:
            #     logging.warning("No well identifier columns found in Petrinex data")
            
            columns_to_keep = [
                'ProductionMonth', 
                # 'OperatorBAID', 
                # 'OperatorName', 
                # 'ReportingFacilityID', 
                # 'ReportingFacilityProvinceState', 
                # 'ReportingFacilityType', 
                # 'ReportingFacilityIdentifier', 
                # 'ReportingFacilityName', 
                # 'ReportingFacilitySubType', 
                # 'ReportingFacilitySubTypeDesc', 
                # 'ReportingFacilityLocation', 
                # 'FacilityLegalSubdivision', 
                # 'FacilitySection', 
                # 'FacilityTownship', 
                # 'FacilityRange', 
                # 'FacilityMeridian', 
                # 'SubmissionDate', 
                'ActivityID', 
                'ProductID', 
                # 'FromToID', 
                # 'FromToIDProvinceState', 
                'FromToIDType', 
                'FromToIDIdentifier', 
                'Volume', 
                # 'Energy', 
                # 'Hours', 
                # 'CCICode', 
                # 'ProrationProduct', 
                # 'ProrationFactor', 
                # 'Heat'
            ]
            
            df = df[list(columns_to_keep)]
        
            staging_dir = Path("data/staging")
            staging_dir.mkdir(parents=True, exist_ok=True)
            
            staging_file = staging_dir / f"{file_path.stem}.parquet"
            df.write_parquet(staging_file)
            
            staging_file = staging_dir / f"{file_path.stem}.csv"
            df.write_csv(staging_file)
            
            # If we've made it here, we've successfully loaded the data
            return df
            
        except Exception as e:
            logging.warning(f"Failed to load with encoding {encoding}: {str(e)}")
    
    # If all direct Polars approaches fail, try with Python's built-in CSV module
    # and then convert to Polars DataFrame
    import csv
    import io
    
    for encoding in encodings_to_try:
        try:
            logging.info(f"Trying with Python's CSV module and encoding: {encoding}")
            
            # Read with Python's CSV module
            rows = []
            with open(file_path, 'r', encoding=encoding, newline='') as f:
                # Read the header first
                csv_reader = csv.reader(f)
                headers = next(csv_reader)
                
                # Read all rows, replacing problematic values
                for row in csv_reader:
                    # Replace problematic values with None
                    cleaned_row = [None if val in ["***", "---", ""] else val for val in row]
                    rows.append(cleaned_row)
            
            # Convert to Polars DataFrame with appropriate types
            df = pl.DataFrame(rows, schema=[pl.String for _ in headers])
            df.columns = headers
            
            # Convert numeric columns where possible
            for col in df.columns:
                try:
                    # Try to convert to float, but keep as string if it fails
                    df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False))
                except:
                    pass
            
            logging.info(f"Successfully loaded Petrinex data with Python CSV and {encoding} encoding")
            logging.info(f"Data shape: {df.shape[0]} rows, {df.shape[1]} columns")
            return df
                
        except Exception as e:
            logging.warning(f"Failed to load with Python CSV and encoding {encoding}: {str(e)}")
    
    # If all approaches fail
    logging.error(f"Error loading Petrinex data: Unable to read file with any encoding")
    return None


# For testing
if __name__ == "__main__":
    logging.info("Testing loader module...")
    
    # Test data paths (these would normally be returned by the downloader module)
    st1_path = Path("data/raw/WellLicenceAllAB.csv")
    st37_path = Path("data/raw/ST37.txt")
    petrinex_path = Path("data/raw/Petrinex_Vol_2025-02.csv")  # Example filename
    
    # Test loading functions if files exist
    if st1_path.exists():
        st1_df = load_aer_st1(st1_path)
        if st1_df is not None:
            logging.info(f"ST1 columns: {st1_df.columns}")
    
    if st37_path.exists():
        st37_df = load_aer_st37(st37_path)
        if st37_df is not None:
            logging.info(f"ST37 columns: {st37_df.columns}")
    
    if petrinex_path.exists():
        petrinex_df = load_petrinex(petrinex_path)
        if petrinex_df is not None:
            logging.info(f"Petrinex columns: {petrinex_df.columns}")
