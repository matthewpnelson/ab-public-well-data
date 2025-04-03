import requests
import logging
from pathlib import Path
import shutil

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define constants
AER_ST1_URL = "https://www2.aer.ca/t/Production/views/COM-WellLicenceAllList/WellLicenceAllAB.csv"
DEFAULT_DOWNLOAD_DIR = Path("data/raw")

def download_file(url: str, output_path: Path):
    """
    Downloads a file from a given URL to a specified path, streaming the content.

    Args:
        url: The URL of the file to download.
        output_path: The local path (including filename) where the file should be saved.
    """
    logging.info(f"Attempting to download file from {url} to {output_path}...")
    try:
        # Ensure the output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with requests.get(url, stream=True, timeout=60) as response: # Added timeout
            response.raise_for_status() # Raises HTTPError for bad responses (4XX or 5XX)
            
            # Stream download to handle potentially large files
            with open(output_path, 'wb') as f:
                shutil.copyfileobj(response.raw, f)
            
            logging.info(f"Successfully downloaded {output_path.name} to {output_path.parent}")
            return True # Indicate success

    except requests.exceptions.Timeout:
        logging.error(f"Timeout error while trying to download {url}")
        return False
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err} - Status Code: {response.status_code}")
        return False
    except requests.exceptions.RequestException as req_err:
        logging.error(f"An error occurred during request to {url}: {req_err}")
        return False
    except Exception as e:
        logging.error(f"An unexpected error occurred during download: {e}")
        if output_path.exists(): # Attempt cleanup if partial file exists
             output_path.unlink()
        return False


def download_aer_st1(download_dir: Path = DEFAULT_DOWNLOAD_DIR) -> Path | None:
    """
    Downloads the AER ST1 Well Licence data CSV.

    Args:
        download_dir: The directory where the file should be saved.

    Returns:
        The Path object to the downloaded file if successful, otherwise None.
    """
    file_name = "WellLicenceAllAB.csv"
    output_path = download_dir / file_name
    
    if download_file(AER_ST1_URL, output_path):
        return output_path
    else:
        logging.error("Failed to download AER ST1 data.")
        return None

# Example of how to run this specific download function (optional, usually called from main.py)
if __name__ == "__main__":
    logging.info("Running downloader directly for AER ST1...")
    downloaded_path = download_aer_st1()
    if downloaded_path:
        logging.info(f"AER ST1 download complete: {downloaded_path}")
    else:
        logging.error("AER ST1 download failed.")
