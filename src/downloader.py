import requests
import logging
from pathlib import Path
import shutil
import datetime
import zipfile
from typing import Optional
import time
import os
import subprocess

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define constants
AER_ST1_URL = "https://www2.aer.ca/t/Production/views/COM-WellLicenceAllList/WellLicenceAllAB.csv"
AER_ST37_URL = "https://static.aer.ca/prd/documents/sts/st37/ST37.zip"
PETRINEX_URL_TEMPLATE = "https://www.petrinex.gov.ab.ca/publicdata/API/Files/AB/Vol/{year}-{month:02d}/CSV"
PETRINEX_POST_URL = "https://www.petrinex.gov.ab.ca/PublicData/Files/RequestZipFiles?arg_strJurisdiction=AB"
DEFAULT_DOWNLOAD_DIR = Path("data/raw")
DEFAULT_TIMEOUT = 120  # Increased timeout from 60 to 120 seconds

# Browser-like headers to use for requests
BROWSER_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Cache-Control': 'max-age=0'
}

def download_file(url: str, output_path: Path, timeout: int = DEFAULT_TIMEOUT, max_retries: int = 2) -> bool:
    """
    Downloads a file from a given URL to a specified path, streaming the content.

    Args:
        url: The URL of the file to download.
        output_path: The local path (including filename) where the file should be saved.
        timeout: Request timeout in seconds.
        max_retries: Maximum number of retry attempts on failure.
        
    Returns:
        bool: True if download was successful, False otherwise.
    """
    logging.info(f"Attempting to download file from {url} to {output_path}...")
    
    # Ensure the output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    for attempt in range(max_retries + 1):
        if attempt > 0:
            logging.info(f"Retry attempt {attempt}/{max_retries}...")
            # Add exponential backoff
            time.sleep(2 ** attempt)
            
        try:
            with requests.get(url, timeout=timeout, headers=BROWSER_HEADERS) as response:
                response.raise_for_status()  # Raises HTTPError for bad responses
                
                # Determine file type and handle download accordingly
                file_ext = output_path.suffix.lower()
                
                if file_ext == '.csv':
                    # For CSV files, ensure proper encoding
                    encoding = response.encoding or 'utf-8'
                    logging.info(f"Downloading CSV file with encoding: {encoding}")
                    
                    with open(output_path, 'w', encoding=encoding, newline='') as f:
                        f.write(response.text)
                else:
                    # For binary files (like ZIP), use content directly
                    with open(output_path, 'wb') as f:
                        f.write(response.content)
                
                logging.info(f"Successfully downloaded {output_path.name} to {output_path.parent}")
                return True  # Indicate success

        except requests.exceptions.Timeout:
            logging.error(f"Timeout error while trying to download {url} (attempt {attempt+1}/{max_retries+1})")
            if attempt == max_retries:
                return False
        except requests.exceptions.HTTPError as http_err:
            logging.error(f"HTTP error occurred: {http_err} - Status Code: {response.status_code}")
            if 400 <= response.status_code < 500:  # Client errors unlikely to be resolved by retry
                return False
            elif attempt == max_retries:
                return False  # Server errors may be temporary, but we've reached max retries
        except requests.exceptions.RequestException as req_err:
            logging.error(f"Request error occurred: {req_err} (attempt {attempt+1}/{max_retries+1})")
            if attempt == max_retries:
                return False
    
    # All attempts failed
    return False


def download_aer_st1(download_dir: Path = DEFAULT_DOWNLOAD_DIR) -> Optional[Path]:
    """
    Downloads the AER ST1 Well Licence data CSV.

    Args:
        download_dir: The directory where the file should be saved.

    Returns:
        The Path object to the downloaded file if successful, otherwise None.
    """
    # Create output path for the file
    output_path = download_dir / "WellLicenceAllAB.csv"
    
    # Check if file already exists
    if output_path.exists():
        logging.info(f"File already exists: {output_path}. Removing before downloading fresh copy.")
        output_path.unlink()
    
    # Download the file    
    if download_file(AER_ST1_URL, output_path):
        logging.info(f"Successfully downloaded AER ST1 data to {output_path}")
        return output_path
    else:
        logging.error("Failed to download AER ST1 data")
        return None


def download_aer_st37(download_dir: Path = DEFAULT_DOWNLOAD_DIR) -> Optional[Path]:
    """
    Downloads the AER ST37 ZIP file containing well data, extracts the TXT file,
    and cleans up the ZIP file.

    Args:
        download_dir: The directory where the file should be saved.

    Returns:
        The Path object to the extracted TXT file if successful, otherwise None.
    """
    # Create output paths
    zip_output_path = download_dir / "ST37.zip"
    txt_output_path = download_dir / "ST37.txt"
    
    # Check if TXT file already exists
    if txt_output_path.exists():
        logging.info(f"ST37 TXT file already exists: {txt_output_path}. Removing before downloading fresh copy.")
        txt_output_path.unlink()
    
    # Check if ZIP file already exists
    if zip_output_path.exists():
        logging.info(f"ST37 ZIP file already exists: {zip_output_path}. Removing before downloading fresh copy.")
        zip_output_path.unlink()
    
    # Download the ZIP file
    if not download_file(AER_ST37_URL, zip_output_path):
        logging.error("Failed to download AER ST37 ZIP file")
        return None
    
    logging.info(f"Successfully downloaded AER ST37 ZIP file to {zip_output_path}")
    
    # Extract the TXT file from the ZIP
    try:
        with zipfile.ZipFile(zip_output_path, 'r') as zip_ref:
            # List the contents of the ZIP file
            file_list = zip_ref.namelist()
            logging.info(f"Files in ZIP: {file_list}")
            
            # Find the ST37 TXT file
            # The exact name may vary, so we'll check for "ST37" and ".txt" in the filename
            txt_files = [f for f in file_list if "ST37" in f.upper() and f.lower().endswith('.txt')]
            
            if not txt_files:
                logging.error("No ST37 TXT file found in the ZIP")
                return None
            
            # Use the first TXT file we find
            txt_in_zip = txt_files[0]
            logging.info(f"Extracting {txt_in_zip} from ZIP")
            
            # Check if the file is in a subdirectory or in the root of the ZIP
            if '/' in txt_in_zip:
                # If in a subdirectory, extract to a temporary directory first
                temp_dir = download_dir / "temp_st37"
                temp_dir.mkdir(exist_ok=True, parents=True)
                
                # Extract the file to the temporary directory
                zip_ref.extract(txt_in_zip, path=temp_dir)
                
                # Create a Path object to the extracted file
                extracted_txt_path = temp_dir / txt_in_zip
                
                # Move the file to the desired location
                shutil.move(extracted_txt_path, txt_output_path)
                
                # Clean up the temporary directory
                shutil.rmtree(temp_dir)
            else:
                # If the file is in the root of the ZIP, extract it directly
                try:
                    # First try to extract directly to the download_dir
                    zip_ref.extract(txt_in_zip, path=download_dir)
                    
                    # Check if the file was extracted with its original name
                    original_path = download_dir / txt_in_zip
                    
                    # If the original name is different from our target, rename it
                    if original_path != txt_output_path:
                        shutil.move(original_path, txt_output_path)
                
                except Exception as e:
                    logging.error(f"Error extracting directly: {e}")
                    
                    # Fallback: extract to a temporary file then move
                    temp_txt_path = download_dir / f"temp_{txt_in_zip}"
                    with open(temp_txt_path, 'wb') as f_out:
                        with zip_ref.open(txt_in_zip) as f_in:
                            shutil.copyfileobj(f_in, f_out)
                    
                    # Move to the desired location
                    shutil.move(temp_txt_path, txt_output_path)
        
        # Cleanup: remove the ZIP file
        logging.info(f"Cleaning up ZIP file: {zip_output_path}")
        zip_output_path.unlink()
        
        logging.info(f"Successfully extracted ST37 TXT file to {txt_output_path}")
        return txt_output_path
    
    except Exception as e:
        logging.error(f"Failed to extract ST37 TXT file: {e}")
        return None


def get_latest_petrinex_url() -> Optional[str]:
    """
    Determines the latest available Petrinex URL by trying the current month
    and then going backward month by month until a valid URL is found.
    
    Returns:
        The URL for the latest available Petrinex CSV data, or None if not found.
    """
    # Get current date
    now = datetime.datetime.now()
    
    # Try starting from two months ago (as current month data is often not available yet)
    start_date = now.replace(day=1) - datetime.timedelta(days=1)
    # We now have the last day of the previous month, so go back one more month
    start_date = start_date.replace(day=1) - datetime.timedelta(days=1)
    
    # Try for the last 6 months
    for i in range(6):
        # Calculate the month to try (moving backward from our start date)
        check_date = start_date - datetime.timedelta(days=30*i)
        year = check_date.year
        month = check_date.month
        
        # Construct the URL to check
        url = PETRINEX_URL_TEMPLATE.format(year=year, month=month)
        
        logging.info(f"Checking for Petrinex data at: {url}")
        
        # Check if this URL is valid
        try:
            with requests.head(url, timeout=10, headers=BROWSER_HEADERS) as response:
                if response.status_code == 200:
                    logging.info(f"Found valid Petrinex data URL: {url}")
                    return url
                else:
                    logging.info(f"No data available for {year}-{month:02d} (status code: {response.status_code})")
        except requests.exceptions.RequestException as e:
            logging.warning(f"Error checking URL {url}: {e}")
    
    logging.error("No valid Petrinex URL found in the last 6 months")
    return None


def download_petrinex(download_dir: Path = DEFAULT_DOWNLOAD_DIR) -> Optional[Path]:
    """
    Downloads the latest available Petrinex ZIP file containing production volumes,
    extracts the inner ZIP file, and then extracts the CSV file.

    Args:
        download_dir: The directory where the file should be saved.

    Returns:
        The Path object to the extracted CSV file if successful, otherwise None.
    """
    # Get the latest Petrinex URL
    petrinex_url = get_latest_petrinex_url()
    
    if not petrinex_url:
        logging.error("Could not determine latest Petrinex data URL")
        return None
    
    # Extract the year and month from the URL
    url_parts = petrinex_url.split('/')
    if len(url_parts) >= 2:
        date_part = url_parts[-2]  # The second-to-last part should be "YYYY-MM"
        if '-' in date_part:
            year_month = date_part
        else:
            # Fallback: use current date
            now = datetime.datetime.now()
            year_month = f"{now.year}-{now.month:02d}"
    else:
        # Fallback: use current date
        now = datetime.datetime.now()
        year_month = f"{now.year}-{now.month:02d}"
    
    # Create paths for the downloaded files
    zip_output_path = download_dir / f"Petrinex_Vol_{year_month}.zip"
    csv_output_path = download_dir / f"Petrinex_Vol_{year_month}.csv"
    
    # Check if CSV file already exists
    if csv_output_path.exists():
        logging.info(f"CSV file already exists: {csv_output_path}")
        
        # Check if it's not empty and valid
        if csv_output_path.stat().st_size > 0:
            logging.info(f"Using existing CSV file: {csv_output_path}")
            return csv_output_path
        else:
            logging.warning(f"Existing CSV file is empty, deleting it: {csv_output_path}")
            csv_output_path.unlink()
    
    # Check if ZIP file already exists (partial download)
    if zip_output_path.exists():
        logging.warning(f"ZIP file already exists, deleting it: {zip_output_path}")
        zip_output_path.unlink()
    
    # Create a temporary directory for extraction
    temp_dir = download_dir / f"temp_petrinex_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"
    temp_dir.mkdir(parents=True, exist_ok=True)
    
    # Use requests session for multiple requests with same session
    with requests.Session() as session:
        session.headers.update(BROWSER_HEADERS)
        
        try:
            # First approach: Direct download via URL
            logging.info(f"Attempting direct download from: {petrinex_url}")
            
            response = session.get(petrinex_url, timeout=DEFAULT_TIMEOUT)
            if response.status_code == 200:
                with open(zip_output_path, 'wb') as f:
                    f.write(response.content)
                logging.info(f"Successfully downloaded ZIP file to: {zip_output_path}")
            else:
                logging.warning(f"Direct download failed with status code: {response.status_code}")
                
                # Second approach: Try POST request to download page
                logging.info(f"Attempting alternative download method via POST to: {PETRINEX_POST_URL}")
                
                post_data = {
                    "Jurisdiction": "AB",
                    "DataType": "VOL",
                    "ReportingMonth": year_month
                }
                
                response = session.post(PETRINEX_POST_URL, data=post_data, timeout=DEFAULT_TIMEOUT)
                if response.status_code == 200:
                    with open(zip_output_path, 'wb') as f:
                        f.write(response.content)
                    logging.info(f"Successfully downloaded ZIP file via POST to: {zip_output_path}")
                else:
                    logging.error(f"POST download failed with status code: {response.status_code}")
                    return None
            
            # Now proceed with extraction if the file exists and has content
            if not zip_output_path.exists() or zip_output_path.stat().st_size == 0:
                logging.error(f"No valid ZIP file at {zip_output_path}")
                return None
                        
            # Extract the outer ZIP file
            logging.info(f"Extracting outer ZIP file: {zip_output_path}")
            with zipfile.ZipFile(zip_output_path, 'r') as zip_ref:
                file_list = zip_ref.namelist()
                logging.info(f"Files in ZIP: {file_list}")
                
                # Find inner ZIP files (the structure is typically a ZIP containing another ZIP)
                inner_zips = []
                csv_files = []
                
                for f in file_list:
                    if f.lower().endswith('.zip'):
                        # Extract the inner ZIP
                        zip_ref.extract(f, path=temp_dir)
                        inner_zips.append(temp_dir / f)
                    elif f.lower().endswith('.csv'):
                        # Direct CSV file (rare)
                        zip_ref.extract(f, path=temp_dir)
                        csv_files.append(temp_dir / f)
                
                # If we found direct CSV files, use them
                if csv_files:
                    logging.info(f"Found {len(csv_files)} CSV file(s) directly in outer ZIP")
                    # Use the first CSV file
                    csv_file = csv_files[0]
                    # Move to final destination
                    shutil.move(csv_file, csv_output_path)
                    logging.info(f"Moved extracted CSV to final destination: {csv_output_path}")
                    return csv_output_path
                elif not inner_zips:
                    logging.error("No inner ZIP files or CSV files found in the outer ZIP")
                    raise Exception("No CSV or ZIP files found in the outer ZIP")
            
            # Use the first inner ZIP file found
            inner_zip = inner_zips[0]
            logging.info(f"Found inner ZIP file: {inner_zip}")
            
            # Extract the inner ZIP file
            logging.info(f"Extracting inner ZIP file: {inner_zip}")
            with zipfile.ZipFile(inner_zip, 'r') as zip_ref:
                # List the contents of the inner ZIP file
                inner_file_list = zip_ref.namelist()
                logging.info(f"Files in inner ZIP: {inner_file_list}")
                
                # Find CSV files in the inner ZIP
                csv_files = [f for f in inner_file_list if f.lower().endswith('.csv')]
                
                if not csv_files:
                    logging.error("No CSV files found in the inner ZIP")
                    raise Exception("No CSV files in inner ZIP")
                
                # Use the first CSV file
                csv_in_zip = csv_files[0]
                logging.info(f"Extracting {csv_in_zip} from inner ZIP")
                
                # Extract the CSV file to the temp directory
                csv_path = temp_dir / csv_in_zip
                zip_ref.extract(csv_in_zip, path=temp_dir)
                
                # Move the extracted CSV to the final destination
                shutil.move(csv_path, csv_output_path)
                logging.info(f"Moved extracted CSV to final destination: {csv_output_path}")
            
            # Clean up the outer ZIP file after successful extraction
            if zip_output_path.exists():
                zip_output_path.unlink()
                logging.info(f"Removed ZIP file after successful extraction: {zip_output_path}")
            
            # Return the path to the extracted CSV file
            logging.info(f"Successfully extracted CSV to {csv_output_path}")
            return csv_output_path
            
        except Exception as e:
            logging.error(f"Error extracting files: {e}")
            
        finally:
            # Clean up the temporary directory
            if temp_dir.exists():
                try:
                    shutil.rmtree(temp_dir)
                    logging.info(f"Cleaned up temporary directory: {temp_dir}")
                except Exception as e:
                    logging.warning(f"Failed to clean up temporary directory: {e}")
        
        # If we get here, all extraction attempts failed
        # Check for existing files to use as fallback
        petrinex_files = list(download_dir.glob("Petrinex_Vol_*.csv"))
        if petrinex_files:
            newest_file = sorted(petrinex_files)[-1]  # Get the most recent by name
            logging.info(f"All extraction methods failed. Falling back to existing file: {newest_file}")
            return newest_file
        else:
            logging.error("All extraction methods failed and no existing files found for fallback")
            return None


# Example of how to run this specific download function (optional, usually called from main.py)
if __name__ == "__main__":
    logging.info("Running downloader directly...")
    
    # Download AER ST1 data
    st1_path = download_aer_st1()
    if st1_path:
        logging.info(f"AER ST1 download complete: {st1_path}")
    else:
        logging.error("AER ST1 download failed.")
    
    # Download AER ST37 data
    st37_path = download_aer_st37()
    if st37_path:
        logging.info(f"AER ST37 download complete: {st37_path}")
    else:
        logging.error("AER ST37 download failed.")
    
    # Download Petrinex data
    petrinex_path = download_petrinex()
    if petrinex_path:
        logging.info(f"Petrinex download complete: {petrinex_path}")
    else:
        logging.error("Petrinex download failed.")
