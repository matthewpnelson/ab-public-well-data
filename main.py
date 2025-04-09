#!/usr/bin/env python3
"""
Alberta Well Data Normalization Project

This script orchestrates the workflow for downloading, processing, normalizing,
and exporting Alberta well data from multiple public sources.
"""

import argparse
import logging
import sys
from pathlib import Path

from src.downloader import download_aer_st1, download_aer_st37, download_petrinex
from src.loader import load_aer_st1, load_aer_st37, load_petrinex
from src.normalize import normalize_data
from src.output import save_normalized_data

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Alberta Well Data Normalization Pipeline")
    
    parser.add_argument(
        "--skip-download", 
        action="store_true", 
        help="Skip download step and use existing files"
    )
    
    parser.add_argument(
        "--skip-profile", 
        action="store_true", 
        help="Skip generating profile report (faster processing)"
    )
    
    return parser.parse_args()

def main():
    """Main pipeline execution function."""
    logging.info("Starting Alberta Well Data Normalization Pipeline")
    
    # Parse command line arguments
    args = parse_args()
    download_dir = Path("data/raw")
    output_dir = Path("output")
    generate_profile = not args.skip_profile
    
    # Ensure directories exist
    download_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Step 1: Download data (if not skipped)
    if not args.skip_download:
        logging.info("STEP 1: Downloading data files")
        
        st1_path = download_aer_st1(download_dir)
        if not st1_path:
            logging.error("Failed to download AER ST1 data - aborting pipeline")
            return 1
            
        st37_path = download_aer_st37(download_dir)
        if not st37_path:
            logging.error("Failed to download AER ST37 data - aborting pipeline")
            return 1
            
        petrinex_path = download_petrinex(download_dir)
        if not petrinex_path:
            logging.error("Failed to download Petrinex data - aborting pipeline")
            return 1
    else:
        logging.info("Skipping download step, using existing files")
        # Set paths to expected file locations
        st1_path = download_dir / "WellLicenceAllAB.csv"
        st37_path = download_dir / "ST37.txt"  # Updated to TXT file
        
        # For Petrinex, we need to find the most recent file
        petrinex_files = list(download_dir.glob("Petrinex_Vol_*.csv"))
        if not petrinex_files:
            logging.error("No Petrinex files found in download directory - aborting pipeline")
            return 1
        petrinex_path = sorted(petrinex_files)[-1]  # Get the most recent file (by name)
        
        # Verify all files exist
        if not st1_path.exists():
            logging.error(f"AER ST1 file not found: {st1_path} - aborting pipeline")
            return 1
        if not st37_path.exists():
            logging.error(f"AER ST37 file not found: {st37_path} - aborting pipeline")
            return 1
        if not petrinex_path.exists():
            logging.error(f"Petrinex file not found: {petrinex_path} - aborting pipeline")
            return 1
    
    logging.info(f"Using files: ST1={st1_path}, ST37={st37_path}, Petrinex={petrinex_path}")
    
    # Step 2: Load data
    logging.info("STEP 2: Loading data files")
    
    st1_df = load_aer_st1(st1_path)
    if st1_df is None:
        logging.error("Failed to load AER ST1 data - aborting pipeline")
        return 1
    
    st37_df = load_aer_st37(st37_path)
    if st37_df is None:
        logging.error("Failed to load AER ST37 data - aborting pipeline")
        return 1
    
    petrinex_df = load_petrinex(petrinex_path)
    if petrinex_df is None:
        logging.error("Failed to load Petrinex data - aborting pipeline")
        return 1
    
    # Step 3: Normalize data
    logging.info("STEP 3: Normalizing data")
    normalized_df = normalize_data(st1_df, st37_df, petrinex_df)
    
    # Log some data quality metrics
    logging.info(f"Normalized data shape: {normalized_df.shape[0]} rows, {normalized_df.shape[1]} columns")
    for col in normalized_df.columns:
        null_pct = (normalized_df[col].null_count() / normalized_df.shape[0]) * 100
        logging.info(f"Column {col}: {null_pct:.2f}% null values")
    
    # Step 4: Save output and analyze data quality
    logging.info("STEP 4: Saving normalized data and analyzing data quality")
    
    if generate_profile:
        logging.info("Generating profile report (this may take some time)")
    else:
        logging.info("Skipping profile report generation")
    
    output_files = save_normalized_data(
        normalized_df,
        output_dir=output_dir,
        save_csv=True,
        generate_profile=generate_profile
    )
    
    if 'parquet' in output_files:
        logging.info(f"Successfully saved normalized data to: {output_files['parquet']}")
        if 'csv' in output_files:
            logging.info(f"Additionally saved as CSV: {output_files['csv']}")
        if 'profile_report' in output_files:
            logging.info(f"Generated profile report: {output_files['profile_report']}")
        if 'quality_metrics' in output_files:
            logging.info(f"Saved quality metrics to: {output_files['quality_metrics']}")
        
        logging.info("Alberta Well Data Normalization Pipeline completed successfully!")
        return 0
    else:
        logging.error("Failed to save normalized data")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
