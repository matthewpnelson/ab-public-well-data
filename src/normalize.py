import polars as pl
import logging
import re
from pathlib import Path
from typing import Optional, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def standardize_st1_license(license_series: pl.Series) -> pl.Series:
    """
    Standardize license numbers by removing leading 2 characters and stripping whitespace.
    
    Args:
        license_series: Polars Series containing license numbers
        
    Returns:
        Standardized license series
    """
    # First remove leading 'W ' characters, then strip any whitespace
    # Use regex to remove both leading and trailing whitespace
    return license_series.str.slice(2).str.strip_chars(' ')

def standardize_st37_license(license_series: pl.Series) -> pl.Series:
    """
    Standardize license numbers by removing whitespace.
    
    Args:
        license_series: Polars Series containing license numbers
        
    Returns:
        Standardized license series
    """
    # First remove leading 'W ' characters, then strip any whitespace
    # Use regex to remove both leading and trailing whitespace
    return license_series.str.strip_chars(' ')

def convert_uwi_display_to_petrinex_format(uwi_series: pl.Series) -> pl.Series:
    """
    Convert UWI from ST37 Display format to Petrinex format.
    
    Example conversion:
    "00/06-06-001-01W4/2" -> "100060600101W402"
    
    Steps:
    1. Strip all non-alphanumeric characters
    2. Prefix with "1"
    3. Ensure event sequence has a leading zero
    
    Args:
        uwi_series: Polars Series containing UWIs in ST37 Display format
        
    Returns:
        Polars Series with UWIs in Petrinex format
    """
    def convert_display_uwi(uwi: str) -> str:
        if not uwi or not isinstance(uwi, str):
            logging.warning(f"Invalid UWI Display format for conversion: {uwi}")
            return uwi  # Return unchanged if invalid
        
        try:
            # 1. Strip all non-alphanumeric characters
            stripped_uwi = ''.join(c for c in uwi if c.isalnum())
            
            # Check if the UWI is already in Petrinex format (starts with 1 and contains W4)
            if stripped_uwi.startswith('1') and 'W4' in stripped_uwi:
                return stripped_uwi
            
            # 2. Prefix with "1"
            prefixed_uwi = f"1{stripped_uwi}"
            
            # 3. Ensure event sequence has a leading zero
            # In the format "00/06-06-001-01W4/2", the event sequence is the last character
            # After stripping, it will be the last character of the string
            # If the length is odd, then the last character is a single digit (needs padding)
            padded_uwi = f"{prefixed_uwi[:-1]}0{prefixed_uwi[-1]}"
            
            logging.debug(f"Converted UWI Display {uwi} to Petrinex format: {padded_uwi}")
            return padded_uwi
        except Exception as e:
            logging.warning(f"Error converting UWI Display {uwi}: {e}")
            return uwi  # Return unchanged if there's an error
    
    return uwi_series.map_elements(convert_display_uwi, return_dtype=pl.Utf8)

def convert_raw_uwi_to_petrinex_format(uwi_series: pl.Series) -> pl.Series:
    """
    Convert UWI from ST37 raw format to Petrinex format.
    
    ST37 format example: '0014010606002'
    Petrinex format example: '100060600101W402'
    
    Args:
        uwi_series: Polars Series containing UWIs in ST37 raw format
        
    Returns:
        Polars Series with UWIs in Petrinex format
    """
    def convert_uwi(uwi: str) -> str:
        if not uwi or not isinstance(uwi, str) or len(uwi) < 14:
            logging.warning(f"Invalid UWI format for conversion: {uwi}")
            return uwi  # Return unchanged if invalid
        
        try:
            # Clean the UWI by removing any non-alphanumeric characters
            clean_uwi = ''.join(c for c in uwi if c.isalnum())
            
            # Check if the UWI is already in Petrinex format (contains W4)
            if 'W4' in clean_uwi:
                logging.info(f"UWI {uwi} appears to already be in Petrinex format")
                return clean_uwi
                
            # Extract components from ST37 format
            # Format: 00MRRRTTTSSLL0
            # Where:
            # 00 = Fixed prefix 
            # M = Meridian (usually 4 for Alberta)
            # RRR = Range (with leading zeros)
            # TTT = Township (with leading zeros)
            # SS = Section
            # LL = LSD (Legal Subdivision)
            # 0 = Event sequence
            
            # Extract components - assuming standard 14-character ST37 UWI
            # If UWI is shorter, adjust the slicing accordingly
            offset = max(0, 14 - len(clean_uwi))
            
            meridian = clean_uwi[2-offset] if 2-offset < len(clean_uwi) else "4"  # Default to 4 if not available
            range_ = clean_uwi[3-offset:6-offset] if 3-offset < len(clean_uwi) else "000"
            township = clean_uwi[6-offset:9-offset] if 6-offset < len(clean_uwi) else "000"
            section = clean_uwi[9-offset:11-offset] if 9-offset < len(clean_uwi) else "00"
            lsd = clean_uwi[11-offset:13-offset] if 11-offset < len(clean_uwi) else "00"
            event_seq = clean_uwi[13-offset:] if 13-offset < len(clean_uwi) else "0"
            
            # Remove leading zeros where appropriate
            range_clean = range_.lstrip('0')
            if not range_clean:
                range_clean = "0"
                
            township_clean = township.lstrip('0')
            if not township_clean:
                township_clean = "0"
            
            # Log the extracted components for debugging
            logging.debug(f"UWI {uwi} components: M={meridian}, R={range_}, T={township}, S={section}, LSD={lsd}, Seq={event_seq}")
            
            # Construct Petrinex format
            # Format: 1LLLSSTTTRRMW4ZZ where:
            # 1 = Leading 1
            # LLL = LSD (with any leading characters if alpha)
            # SS = Section
            # TTT = Township (padded to 3 digits)
            # RR = Range (padded to 2 digits)
            # M = Meridian
            # W4 = Fixed "W4" string
            # ZZ = Event sequence (padded to 2 digits)
            
            # Petrinex UWI format
            petrinex_uwi = f"1{lsd}{section}{township_clean.zfill(3)}{range_clean.zfill(2)}{meridian}W4{event_seq.zfill(2)}"
            
            logging.debug(f"Converted UWI {uwi} to Petrinex format: {petrinex_uwi}")
            return petrinex_uwi
        except Exception as e:
            logging.warning(f"Error converting UWI {uwi}: {e}")
            return uwi  # Return unchanged if there's an error
    
    return uwi_series.map_elements(convert_uwi, return_dtype=pl.Utf8)

def prepare_petrinex_data(petrinex_df: Optional[pl.DataFrame] = None, 
                          staging_dir: Path = Path('data/staging'),
                          intermediate_dir: Path = Path('data/intermediate')) -> Optional[pl.DataFrame]:
    """
    Prepare Petrinex data for merging with well data.
    
    This function:
    1. Pulls the most recent Petrinex parquet data from the staging folder
    2. Applies filters: FromToIDType=WI, ProductID=GAS or OIL, ActivityID=PROD
    3. Renames FromToIDIdentifier to UWI
    4. Pivots to get OIL Volume and GAS Volume columns for each UWI
    5. Verifies there's only one row per UWI
    6. Outputs to an intermediate folder as parquet file
    
    Args:
        petrinex_df: Optional Petrinex DataFrame (if already loaded)
        staging_dir: Directory where staging data is stored
        intermediate_dir: Directory where intermediate files will be saved
        
    Returns:
        Prepared Petrinex DataFrame ready for merging
    """
    logging.info("Preparing Petrinex data")
    
    # Step 1: Pull the most recent Petrinex parquet data if not provided
    if petrinex_df is None:
        # Find most recent Petrinex parquet file in staging directory
        staging_dir.mkdir(parents=True, exist_ok=True)
        petrinex_files = list(staging_dir.glob("Petrinex_Vol_*.parquet"))
        
        if not petrinex_files:
            # Try CSV files if no parquet found
            petrinex_files = list(staging_dir.glob("Petrinex_Vol_*.csv"))
            
            if not petrinex_files:
                logging.error("No Petrinex files found in staging directory")
                return None
            
            # Load the most recent CSV file
            most_recent_file = sorted(petrinex_files)[-1]
            logging.info(f"Loading most recent Petrinex file: {most_recent_file}")
            petrinex_df = pl.read_csv(most_recent_file)
        else:
            # Load the most recent parquet file
            most_recent_file = sorted(petrinex_files)[-1]
            logging.info(f"Loading most recent Petrinex file: {most_recent_file}")
            petrinex_df = pl.read_parquet(most_recent_file)
    
    # Production Month
    latest_month = petrinex_df['ProductionMonth'].max()
        
    
    # Check if required columns exist
    required_columns = ['FromToIDType', 'FromToIDIdentifier', 'ProductID', 'ActivityID', 'Volume']
    
    # Try to map the actual column names to the expected ones (case-insensitive)
    column_mapping = {}
    for req_col in required_columns:
        matches = [col for col in petrinex_df.columns if col.lower() == req_col.lower()]
        if matches:
            column_mapping[req_col] = matches[0]
        else:
            logging.warning(f"Required column {req_col} not found in Petrinex data")
    
    # If we don't have enough column mappings, we can't proceed
    if len(column_mapping) < len(required_columns):
        logging.error("Could not find all required columns in Petrinex data")
        return None
    
    # Rename columns to standardized names if needed
    if any(k != v for k, v in column_mapping.items()):
        petrinex_df = petrinex_df.rename(
            {v: k for k, v in column_mapping.items()}
        )
    
    # Step 2: Apply filters
    try:
        filtered_df = petrinex_df.filter(
            (pl.col('FromToIDType') == 'WI') &
            ((pl.col('ProductID') == 'GAS') | (pl.col('ProductID') == 'OIL')) &
            (pl.col('ActivityID') == 'PROD')
        )
        
        logging.info(f"Filtered Petrinex data from {petrinex_df.shape[0]} to {filtered_df.shape[0]} rows")
        
        # If we filtered out all rows, that's a problem
        if filtered_df.shape[0] == 0:
            logging.error("All rows were filtered out from Petrinex data")
            return None
    except Exception as e:
        logging.error(f"Error filtering Petrinex data: {e}")
        return None
    
    # Step 3: Rename FromToIDIdentifier to UWI
    prepared_df = filtered_df.rename({'FromToIDIdentifier': 'UWI'})
    
    # Step 4: Pivot to get OIL Volume and GAS Volume columns for each UWI
    try:
        pivoted_df = prepared_df.pivot(
            index='UWI',
            columns='ProductID',
            values='Volume',
            aggregate_function='sum'  # Sum volumes if there are multiple entries
        )

        
        # Rename the pivoted columns to be more descriptive
        rename_map = {}
        for col in pivoted_df.columns:
            if col != 'UWI':
                rename_map[col] = f"Latest Month {col} Production Volume"
        
        if rename_map:
            pivoted_df = pivoted_df.rename(rename_map)
            
                    
        pivoted_df = pivoted_df.with_columns(
                    pl.lit(latest_month).alias("Production Month")
                )
            
        logging.info(f"Pivoted Petrinex data: {pivoted_df.shape[0]} rows, {pivoted_df.shape[1]} columns")
        
        # Step 5: Verify there's only one row per UWI
        unique_uwis = pivoted_df.select('UWI').n_unique()
        if unique_uwis != pivoted_df.shape[0]:
            logging.warning(f"After pivoting, found {unique_uwis} unique UWIs but {pivoted_df.shape[0]} rows. There might be duplicate UWIs.")
    except Exception as e:
        logging.error(f"Error pivoting Petrinex data: {e}")
        return None
    
    # Step 6: Output to intermediate folder as parquet file
    intermediate_dir.mkdir(parents=True, exist_ok=True)
    output_path = intermediate_dir / "prepared_petrinex.parquet"
    
    try:
        pivoted_df.write_parquet(output_path)
        logging.info(f"Saved prepared Petrinex data to {output_path}")
    except Exception as e:
        logging.error(f"Error saving prepared Petrinex data: {e}")
    
    return pivoted_df

def merge_st1_st37(st1_df: pl.DataFrame, st37_df: pl.DataFrame) -> pl.DataFrame:
    """
    Merge AER ST1 (well license) and ST37 (well status) data.
    
    Args:
        st1_df: AER ST1 Well License data from AER website
        st37_df: AER ST37 Well Status data
        
    Returns:
        A merged DataFrame containing both license and status information
    """
    logging.info("Merging ST1 and ST37 data")
    
    # Verify required columns exist
    required_st1_cols = ["License Number"]
    required_st37_cols = ["License", "UWI Display"]
    
    for col in required_st1_cols:
        logging.info(f"St1 columns: {st1_df.columns}")
        if col not in st1_df.columns:
            logging.error(f"Required column '{col}' not found in ST1 data")
            return None
            
    for col in required_st37_cols:
        logging.info(f"St37 columns: {st37_df.columns}")
        if col not in st37_df.columns:
            logging.error(f"Required column '{col}' not found in ST37 data")
            return None
    
    # Use explicit column names
    st1_license_col = "License Number"
    st37_license_col = "License"
    
    # Standardize license numbers for joining
    st1_df = st1_df.with_columns(
        standardize_st1_license(pl.col(st1_license_col)).alias("Standardized_License")
    )
    
    st37_df = st37_df.with_columns(
        standardize_st37_license(pl.col(st37_license_col)).alias("Standardized_License")
    )
    
    # Check for standardized licenses
    st1_std_licenses = st1_df.select("Standardized_License").sample(5)
    st37_std_licenses = st37_df.select("Standardized_License").sample(5)
    
    logging.info(f"ST1 standardized license samples:\n{st1_std_licenses}")
    logging.info(f"ST37 standardized license samples:\n{st37_std_licenses}")
    
    # Merge the DataFrames on standardized license
    merged_df = st37_df.join(
        st1_df,
        on="Standardized_License",
        how="left"
    )
    
    # Check the merged result
    logging.info(f"Merged DataFrame shape: {merged_df.shape}")
    
    # Count rows with and without UWI Display
    if "UWI Display" in merged_df.columns:
        uwi_count = merged_df.filter(pl.col("UWI Display").is_not_null()).shape[0]
        logging.info(f"Rows with UWI Display: {uwi_count} ({uwi_count/merged_df.shape[0]*100:.1f}%)")
    
    return merged_df


def merge_with_petrinex(base_df: pl.DataFrame, petrinex_df: pl.DataFrame) -> pl.DataFrame:
    """
    Merge the base DataFrame (ST1+ST37) with Petrinex production data.
    
    Args:
        base_df: The base DataFrame (merged ST1 and ST37 data)
        petrinex_df: The Petrinex DataFrame with production data
        
    Returns:
        A merged DataFrame with production data
    """
    if base_df is None or petrinex_df is None:
        logging.error("Cannot merge with Petrinex: one or both DataFrames are None")
        return base_df
        
    # Make a copy to avoid modifying the original
    merged_df = base_df.clone()
    
    try:
        # Use explicit column names for joining
        base_uwi_col = "UWI Display"
        petrinex_uwi_col = "UWI"
        
        # Verify that required columns exist
        if base_uwi_col not in merged_df.columns:
            logging.error(f"Required column '{base_uwi_col}' not found in base DataFrame")
            logging.info(f"Available columns: {merged_df.columns}")
            return merged_df
            
        if petrinex_uwi_col not in petrinex_df.columns:
            logging.error(f"Required column '{petrinex_uwi_col}' not found in Petrinex DataFrame")
            logging.info(f"Available columns: {petrinex_df.columns}")
            return merged_df
            
        logging.info(f"Using {base_uwi_col} and {petrinex_uwi_col} for merging with Petrinex data")
        
        # Convert UWI Display to Petrinex format for joining
        merged_df = merged_df.with_columns(
            convert_uwi_display_to_petrinex_format(pl.col(base_uwi_col)).alias("Petrinex_UWI")
        )
        
        # Join on the Petrinex UWI column
        merged_df = merged_df.join(
            petrinex_df,
            left_on="Petrinex_UWI",
            right_on=petrinex_uwi_col,
            how="left"
        )
        
        # Remove the temporary Petrinex UWI column
        merged_df = merged_df.drop("Petrinex_UWI")
        
        logging.info(f"Merged with Petrinex data. New shape: {merged_df.shape}")
        return merged_df
        
    except Exception as e:
        logging.error(f"Error merging with Petrinex data: {e}")
        import traceback
        logging.error(traceback.format_exc())
        return base_df


def fill_missing_values(df: pl.DataFrame) -> pl.DataFrame:
    """
    Fill missing values within license groups for more complete data.
    
    Args:
        df: DataFrame with potentially missing values
        
    Returns:
        DataFrame with filled values within license groups
    """
    if df is None:
        logging.error("Cannot fill missing values: DataFrame is None")
        return df
        
    # Make a copy to avoid modifying the original
    result = df.clone()
    
    try:
        # Use explicit column names
        license_col = "Standardized_License"
        
        # Verify that the required column exists
        if license_col not in result.columns:
            logging.error(f"Required column '{license_col}' not found for filling missing values")
            logging.info(f"Available columns: {result.columns}")
            return result
            
        # For the UWI Display column, fill missing values within license groups
        if "UWI Display" in result.columns:
            result = result.with_columns(
                pl.col("UWI Display").fill_null(
                    pl.col("UWI Display").forward_fill().over(license_col)
                )
            )
            
            result = result.with_columns(
                pl.col("UWI Display").fill_null(
                    pl.col("UWI Display").backward_fill().over(license_col)
                )
            )
        
        # For production volume columns, fill missing values within license groups
        for volume_col in ["OIL Volume", "GAS Volume"]:
            if volume_col in result.columns:
                result = result.with_columns(
                    pl.col(volume_col).fill_null(
                        pl.col(volume_col).forward_fill().over(license_col)
                    )
                )
                
                result = result.with_columns(
                    pl.col(volume_col).fill_null(
                        pl.col(volume_col).backward_fill().over(license_col)
                    )
                )
        
        # Count filled values
        if "UWI Display" in result.columns:
            filled_uwi_count = result.filter(pl.col("UWI Display").is_not_null()).shape[0]
            logging.info(f"After filling, {filled_uwi_count} rows have UWI Display values")
        
        return result
        
    except Exception as e:
        logging.error(f"Error filling missing values: {e}")
        import traceback
        logging.error(traceback.format_exc())
        return df


def normalize_data(st1_df: pl.DataFrame, st37_df: pl.DataFrame, petrinex_df: pl.DataFrame) -> pl.DataFrame:
    """
    Normalize and combine data from all three sources.
    
    Args:
        st1_df: DataFrame containing AER ST1 data.
        st37_df: DataFrame containing AER ST37 data.
        petrinex_df: DataFrame containing Petrinex production data.
        
    Returns:
        A normalized DataFrame combining all sources.
    """
    logging.info("Starting data normalization process")
    
    # Step 1: Prepare Petrinex data (filter, pivot, etc.)
    prepared_petrinex_df = prepare_petrinex_data(petrinex_df)
    
    if prepared_petrinex_df is None:
        logging.warning("Using original Petrinex data as preparation failed")
        prepared_petrinex_df = petrinex_df

    # Extract latest production month from Petrinex data
    latest_month = prepared_petrinex_df.select("Production Month").max() 
    logging.info(f"Latest production month: {latest_month}")
    
    # Step 2: Merge ST1 and ST37
    merged_df = merge_st1_st37(st1_df, st37_df)
    
    # Step 3: Merge with prepared Petrinex
    merged_df = merge_with_petrinex(merged_df, prepared_petrinex_df)
    
    # Step 4: Fill missing values within licence groups
    normalized_df = fill_missing_values(merged_df)

    
    logging.info(f"Normalization complete. Final shape: {normalized_df.shape}")
    
    return normalized_df


# For testing
if __name__ == "__main__":
    logging.info("Testing normalize module...")
    
    # Set logging level to debug for more detailed output
    logging.getLogger().setLevel(logging.DEBUG)
    
    # Create test dataframes that match the actual columns from loader.py
    # ST1 test data with actual column names
    st1_test = pl.DataFrame({
        "License Number": ["W 1000", "2000", "3000"],  # Note the leading "W " in the first license
        "Company Name": ["Company A", "Company B", "Company C"],
        "Latitude": [53.1234, 53.5678, 53.9012],
        "Longitude": [-113.1234, -113.5678, -113.9012],
        "Surface Location": ["01-01-001-01W5", "02-02-002-02W5", "03-03-003-03W5"],
        "License Status": ["Issued", "Issued", "Abandoned"],
        "License Status Date": ["2023-01-01", "2023-02-01", "2023-03-01"],
        "Is Non-Routine": ["N", "N", "Y"]
    })
    
    # ST37 test data with actual column names and ST37 formatted UWIs
    st37_test = pl.DataFrame({
        "UWI": ["0014010606002", "0024020606002", "0034030606002"],  # ST37 numeric format
        "UWI Display": ["00/06-06-001-01W4/2", "00/06-06-002-02W4/2", "00/06-06-003-03W4/2"],  # ST37 display format
        "Well Name": ["Test Well 1", "Test Well 2", "Test Well 3"],
        "Field Code": ["FIELD1", "FIELD2", "FIELD3"],
        "Pool Code": ["POOL1", "POOL2", "POOL3"],
        "License": ["1000", "2000", "3000"],  # Match the ST1 license numbers after standardization
        "License Status": ["Issued", "Issued", "Abandoned"],
        "Primary Fluid": ["GAS", "OIL", "GAS"],
        "Status Code": ["Active", "Suspended", "Abandoned"],
        "Status Date": ["2023-01-15", "2023-02-15", "2023-03-15"]
    })
    
    # Create test Petrinex data with UWIs matching the expected converted format from ST37
    petrinex_test = pl.DataFrame({
        "FromToIDType": ["WI", "WI", "WI", "WI", "WI", "WI"],
        "FromToIDIdentifier": ["100060600101W402", "100060600101W402", "100060600202W402", "100060600202W402", "100060600303W402", "100060600303W402"],
        "ProductID": ["OIL", "GAS", "OIL", "GAS", "OIL", "GAS"],
        "ActivityID": ["PROD", "PROD", "PROD", "PROD", "PROD", "PROD"],
        "Volume": [100, 1000, 200, 2000, 300, 3000]
    })
    
    # Test UWI conversion function directly to verify it produces the expected format
    uwi_samples = pl.DataFrame({
        "ST37_UWI": ["00/06-06-001-01W4/0", "F1/06-06-001-01W4/0", "00/08-35-001-01W4/3"]
    })
    converted_uwis = uwi_samples.with_columns(
        convert_uwi_display_to_petrinex_format(pl.col("ST37_UWI")).alias("Petrinex_UWI")
    )
    logging.info(f"UWI conversion test:\n{converted_uwis}")
    
    # Test the prepare_petrinex_data function
    prepared_petrinex = prepare_petrinex_data(petrinex_test)
    if prepared_petrinex is not None:
        logging.info(f"Prepared Petrinex columns: {prepared_petrinex.columns}")
        logging.info(f"Prepared Petrinex data:\n{prepared_petrinex}")
    
    # Test the normalize function
    result = normalize_data(st1_test, st37_test, petrinex_test)
    logging.info(f"Test result columns: {result.columns}")
    
    # Display a sample of the result to check join keys
    if result is not None and result.shape[0] > 0:
        logging.info(f"Result sample (first 3 rows):\n{result.head(3)}")
        
        # Check for join success
        license_col = next((col for col in result.columns if 'licen' in col.lower()), None)
        if license_col:
            join_success = result.filter(pl.col("Field Code").is_not_null()).shape[0]
            logging.info(f"ST1-ST37 join success: {join_success} rows have Field Code from ST37")
            
        uwi_col = next((col for col in result.columns if 'uwi' in col.lower() and 'display' not in col.lower()), None)
        oil_vol_col = next((col for col in result.columns if 'oil' in col.lower() and 'vol' in col.lower()), None)
        if uwi_col and oil_vol_col:
            petrinex_join_success = result.filter(pl.col(oil_vol_col).is_not_null()).shape[0]
            logging.info(f"Petrinex join success: {petrinex_join_success} rows have {oil_vol_col} from Petrinex")

    # Test UWI Display conversion function directly to verify it produces the expected format
    uwi_display_samples = pl.DataFrame({
        "UWI_Display": ["00/06-06-001-01W4/2", "00/06-06-001-01W4/0", "00/05-05-001-01W4/0"]
    })
    converted_uwis = uwi_display_samples.with_columns(
        convert_uwi_display_to_petrinex_format(pl.col("UWI_Display")).alias("Petrinex_UWI")
    )
    logging.info(f"UWI Display conversion test:\n{converted_uwis}")
    
    # Check if the conversion matches the expected format
    assert converted_uwis["Petrinex_UWI"].to_list() == ["100060600101W402", "100060600101W400", "100050500101W400"]

    # Also test with None/null values to make sure conversion doesn't crash
    uwi_null_test = pl.DataFrame({
        "UWI_Display": ["00/06-06-001-01W4/2", None, ""]
    })
    converted_null_uwis = uwi_null_test.with_columns(
        convert_uwi_display_to_petrinex_format(pl.col("UWI_Display")).alias("Petrinex_UWI")
    )
    logging.info(f"UWI null handling test:\n{converted_null_uwis}")
