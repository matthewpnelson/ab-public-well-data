import polars as pl
import logging
from pathlib import Path
from typing import Optional
import json
from ydata_profiling import ProfileReport

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def save_to_parquet(df: pl.DataFrame, output_path: Path) -> bool:
    """
    Save a DataFrame to Parquet format.
    
    Args:
        df: The DataFrame to save.
        output_path: The path where the Parquet file should be saved.
        
    Returns:
        True if successful, False otherwise.
    """
    try:
        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        logging.info(f"Saving DataFrame to Parquet: {output_path}")
        df.write_parquet(output_path)
        logging.info(f"Successfully saved Parquet file: {output_path}")
        return True
        
    except Exception as e:
        logging.error(f"Error saving to Parquet: {e}")
        return False


def save_to_csv(df: pl.DataFrame, output_path: Path) -> bool:
    """
    Save a DataFrame to CSV format.
    
    Args:
        df: The DataFrame to save.
        output_path: The path where the CSV file should be saved.
        
    Returns:
        True if successful, False otherwise.
    """
    try:
        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        logging.info(f"Saving DataFrame to CSV: {output_path}")
        df.write_csv(output_path)
        logging.info(f"Successfully saved CSV file: {output_path}")
        return True
        
    except Exception as e:
        logging.error(f"Error saving to CSV: {e}")
        return False


def generate_profile_report(df: pl.DataFrame, output_path: Path) -> bool:
    """
    Generate a profile report for the DataFrame using ydata-profiling.
    
    Args:
        df: The DataFrame to profile.
        output_path: The path where the profile report should be saved.
        
    Returns:
        True if successful, False otherwise.
    """
    try:
        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        logging.info(f"Generating profile report: {output_path}")
        
        # Convert Polars DataFrame to Pandas for profiling
        pandas_df = df.to_pandas()
        
        # Generate profile report
        profile = ProfileReport(pandas_df, title="Alberta Well Data Profile Report")
        profile.to_file(output_path)
        
        logging.info(f"Successfully generated profile report: {output_path}")
        return True
        
    except Exception as e:
        logging.error(f"Error generating profile report: {e}")
        return False


def analyze_data_quality(df: pl.DataFrame) -> dict:
    """
    Analyze the quality of the data, focusing on production vs license status.
    
    Args:
        df: The DataFrame to analyze.
        
    Returns:
        A dictionary containing quality metrics.
    """
    quality_metrics = {}
    
    # Find oil and gas volume columns
    oil_vol_col = next((col for col in df.columns if 'oil' in col.lower() and 'vol' in col.lower()), None)
    gas_vol_col = next((col for col in df.columns if 'gas' in col.lower() and 'vol' in col.lower()), None)
    mode_col = next((col for col in df.columns if 'mode' in col.lower()), None)
    license_status_col = next((col for col in df.columns if 'licen' in col.lower() and 'status' in col.lower()), None)
    
    if not all([oil_vol_col, gas_vol_col]):
        logging.warning("Could not find oil or gas volume columns for quality analysis")
        return quality_metrics
    
    # Count rows with production data
    rows_with_oil = df.filter(pl.col(oil_vol_col).is_not_null() & (pl.col(oil_vol_col) > 0)).shape[0]
    rows_with_gas = df.filter(pl.col(gas_vol_col).is_not_null() & (pl.col(gas_vol_col) > 0)).shape[0]
    rows_with_production = df.filter(
        (pl.col(oil_vol_col).is_not_null() & (pl.col(oil_vol_col) > 0)) | 
        (pl.col(gas_vol_col).is_not_null() & (pl.col(gas_vol_col) > 0))
    ).shape[0]
    
    quality_metrics['rows_with_oil'] = rows_with_oil
    quality_metrics['rows_with_gas'] = rows_with_gas
    quality_metrics['rows_with_production'] = rows_with_production
    
    # Check if status columns exist
    if mode_col:
        # First check the data type of the status column
        status_dtype = df.schema[mode_col]
        is_string_status = isinstance(status_dtype, pl.String)
        
        # Count production by status
        production_by_status = df.filter(
            (pl.col(oil_vol_col).is_not_null() & (pl.col(oil_vol_col) > 0)) | 
            (pl.col(gas_vol_col).is_not_null() & (pl.col(gas_vol_col) > 0))
        ).group_by(mode_col).agg(
            pl.count().alias("count"),
            pl.sum(oil_vol_col).alias(f"total_{oil_vol_col}"),
            pl.sum(gas_vol_col).alias(f"total_{gas_vol_col}")
        )
        
        # Convert to dictionary for logging
        status_counts = production_by_status.select([mode_col, "count"]).to_dict(as_series=False)
        quality_metrics['production_by_status'] = {
            str(status): count for status, count in zip(status_counts[mode_col], status_counts["count"])
        }
        
    # Check license status if available
    if license_status_col:
        # First check the data type of the license status column
        license_status_dtype = df.schema[license_status_col]
        is_string_license_status = isinstance(license_status_dtype, pl.String)
        
        # Count production by license status
        production_by_license_status = df.filter(
            (pl.col(oil_vol_col).is_not_null() & (pl.col(oil_vol_col) > 0)) | 
            (pl.col(gas_vol_col).is_not_null() & (pl.col(gas_vol_col) > 0))
        ).group_by(license_status_col).agg(
            pl.count().alias("count"),
            pl.sum(oil_vol_col).alias(f"total_{oil_vol_col}"),
            pl.sum(gas_vol_col).alias(f"total_{gas_vol_col}")
        )
        
        # Convert to dictionary for logging
        license_status_counts = production_by_license_status.select([license_status_col, "count"]).to_dict(as_series=False)
        quality_metrics['production_by_license_status'] = {
            str(status): count for status, count in zip(license_status_counts[license_status_col], license_status_counts["count"])
        }
    
    return quality_metrics


def save_normalized_data(df: pl.DataFrame, output_dir: Path = Path("output"), 
                        save_csv: bool = True, generate_profile: bool = True) -> dict:
    """
    Save the normalized DataFrame to the specified output formats and generate quality reports.
    
    Args:
        df: The normalized DataFrame to save.
        output_dir: The directory where output files should be saved.
        save_csv: Whether to save a CSV copy in addition to Parquet.
        generate_profile: Whether to generate a profile report.
        
    Returns:
        A dictionary with keys 'parquet' and optionally 'csv', mapping to the 
        paths of successfully saved files.
    """
    output_files = {}
    
    logging.info(f"First 5 rows of normalized data:\n{df.head(5)}")
    
    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Analyze data quality
    logging.info("Analyzing data quality...")
    quality_metrics = analyze_data_quality(df)
    
    # Log quality metrics
    logging.info("Data Quality Metrics:")
    for key, value in quality_metrics.items():
        if isinstance(value, dict):
            logging.info(f"  {key}:")
            for subkey, subvalue in value.items():
                logging.info(f"    {subkey}: {subvalue}")
        else:
            logging.info(f"  {key}: {value}")
    
    # Save quality metrics to JSON
    quality_path = output_dir / "quality_metrics.json"
    try:
        with open(quality_path, 'w') as f:
            json.dump(quality_metrics, f, indent=2)
        output_files['quality_metrics'] = quality_path
        logging.info(f"Saved quality metrics to {quality_path}")
    except Exception as e:
        logging.error(f"Error saving quality metrics: {e}")
    
    # Save to Parquet (primary format)
    parquet_path = output_dir / "normalized_wells_ab.parquet"
    if save_to_parquet(df, parquet_path):
        output_files['parquet'] = parquet_path
    
    # Optionally save to CSV as well
    if save_csv:
        csv_path = output_dir / "normalized_wells_ab.csv"
        if save_to_csv(df, csv_path):
            output_files['csv'] = csv_path
    
    # Generate profile report if requested
    if generate_profile:
        profile_path = output_dir / "profile_report.html"
        if generate_profile_report(df, profile_path):
            output_files['profile_report'] = profile_path
    
    return output_files


# For testing
if __name__ == "__main__":
    logging.info("Testing output module...")
    
    # Create a simple test DataFrame
    test_df = pl.DataFrame({
        "UWI": ["100000000000", "200000000000", "300000000000"],
        "Licence Number": ["1000", "2000", "3000"],
        "Well Name": ["Test Well 1", "Test Well 2", "Test Well 3"],
        "Date": ["2023-01-01", "2023-02-01", "2023-03-01"],
        "Status": ["Active", "Suspended", "Abandoned"],
        "License Status": ["Issued", "Issued", "Abandoned"],
        "Latest Month OIL Production Volume": [100, 200, 0],
        "Latest Month GAS Production Volume": [1000, 0, 3000]
    })
    
    # Test saving the DataFrame with quality analysis and profiling
    output_files = save_normalized_data(test_df, save_csv=True)
    logging.info(f"Output files: {output_files}")
