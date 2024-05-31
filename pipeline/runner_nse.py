#!/usr/bin/python3

import shutil
import gzip
import pickle
import requests
from datetime import datetime
from zipfile import ZipFile
from pathlib import Path
from typing import Union
from datetime import timedelta

# Configuration
BASE_URL = "https://nsearchives.nseindia.com"
cookies_dir = "../data"
output_dir = "../data/nse/equity/nse"
TIMEOUT = 20  # seconds

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate",
    "Referer": "https://www.nseindia.com"
}

def get_or_set_cookies(session: requests.Session, cookie_path: Path):
    """
    Loads cookies from a file if it exists; otherwise, fetches them from the NSE website and saves them.

    :param session: The requests.Session object to manage cookies.
    :param cookie_path: The path to the file where cookies are stored or will be saved.
    """
    if cookie_path.exists():
        cookies = pickle.loads(cookie_path.read_bytes())
        session.cookies.update(cookies)
    else:
        response = session.get("https://www.nseindia.com", timeout=TIMEOUT)
        cookies = response.cookies
        cookie_path.write_bytes(pickle.dumps(cookies))
        session.cookies.update(cookies)
   

def download_file(session: requests.Session, url: str, dest_path: Path):
    """
    Downloads a file from a given URL and saves it to a specified path.

    :param session: The requests.Session object to use for the download.
    :param url: The URL of the file to download.
    :param dest_path: The local path where the downloaded file will be saved.
    :raises RuntimeError: If the downloaded content is an HTML page, which indicates the file was not found.
    """
    response = session.get(url, stream=True, timeout=TIMEOUT)
    if "text/html" in response.headers.get("Content-Type", ""):
        raise RuntimeError(f"NSE file not available or invalid URL: {url}")

    with open(dest_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            f.write(chunk)


def extract_file(file: Path, extract_to: Path) -> Path:
    """
    Extracts the first file from a zip archive, removes the original zip file, and returns the path to the extracted file.

    :param file: The path to the zip file to be extracted.
    :param extract_to: The directory where the file should be extracted.
    :return: The path to the newly extracted file.
    """
    with ZipFile(file) as zipf:
        first_file = zipf.namelist()[0]
        extracted_path = zipf.extract(first_file, path=extract_to)
    file.unlink()  # Remove zip
    return Path(extracted_path)


def compress_file(input_file_path):
    """
    Compresses a given file to a .csv.gz format. 
    The compressed file will have the same name but with a .csv.gz extension in the same directory.

    :parm input_file_path (str or Path): The path to the file you want to compress.
    :return bool                                   
    """
    # Ensure the input_file_path is a Path object for easier manipulation
    input_path = Path(input_file_path)

    # Construct the path for the gzipped file.
    # It will have the original name with a .csv.gz suffix.
    gz_path = input_path.with_suffix(".csv.gz")

    try:
        # Open the input file in binary read mode ('rb')
        # Open the output gzip file in binary write mode ('wb')
        with open(input_path, 'rb') as f_in, gzip.open(gz_path, 'wb') as f_out:
            # Copy the contents from the input file to the gzipped output file
            shutil.copyfileobj(f_in, f_out)
        # Optionally remove the uncompressed CSV
        input_file_path.unlink()  # deletes original .csv file
        print(f"Successfully compressed '{input_path.name}' to '{gz_path.name}'")
        return True
    except FileNotFoundError:
        print(f"Error: Input file not found at '{input_path}'")
    except Exception as e:
        print(f"An error occurred during compression: {e}")


def download_nse_equity_bhavcopy(date_str: str, output_dir: Union[str, Path]) -> Path:
    """
    Download and extract NSE equity bhavcopy for a given date.
    :param date_str: Format ddMMMyyyy (e.g., 05AUG2024)
    :param output_dir: Folder to save the extracted CSV
    :return: Path to extracted CSV file
    """
    date_obj = datetime.strptime(date_str.upper(), "%d%b%Y")

    # Ensure output folder
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Setup session and cookies
    session = requests.Session()
    session.headers.update(HEADERS)
    cookie_path = Path(cookies_dir) / "nse_cookies.pkl"
    get_or_set_cookies(session, cookie_path)

    # Download URL and destination
    zip_filename = f"BhavCopy_NSE_CM_0_0_0_{date_obj:%Y%m%d}_F_0000.csv.zip"
    zip_url = f"{BASE_URL}/content/cm/{zip_filename}"
    zip_path = Path(output_dir) / zip_filename

    # Download and extract
    download_file(session, zip_url, zip_path)
    
    # Extract zip file
    extracted = extract_file(zip_path, output_dir)

    # Rename file
    # final_filename = date_obj.strftime("%d%b%Y").upper() + ".csv"
    final_filename = date_obj.strftime("%Y%m%d") + ".csv"
    final_path = output_dir / final_filename
    Path(extracted).rename(final_path)

    # Compress the file to .csv.gz to save space
    compress_file(final_path)

    print(f"✅ Extracted file saved at: {final_path}.gz")
    return final_path


def download_bhavcopy_range(start_date: str, end_date: str, output_dir: Union[str, Path]) -> list:
    """
    Download NSE equity bhavcopies between two dates (inclusive), skipping weekends.

    :param start_date: Start date in ddMMMyyyy (e.g., "01AUG2025")
    :param end_date: End date in ddMMMyyyy (e.g., "05AUG2025")
    :param output_dir: Directory to store downloaded CSVs
    :return: List of Paths to downloaded files
    """
    start = datetime.strptime(start_date.upper(), "%d%b%Y")
    end = datetime.strptime(end_date.upper(), "%d%b%Y")

    current = start
    downloaded_files = []

    while current <= end:
        if current.weekday() < 5:  # 0–4 = Mon–Fri
            date_str = current.strftime("%d%b%Y").upper()
            try:
                file_path = download_nse_equity_bhavcopy(date_str, output_dir)
                downloaded_files.append(file_path)
            except Exception as e:
                print(f"❌ Failed for {date_str}: {e}")
        else:
            print(f"⏭️ Skipping weekend: {current.strftime('%A %d-%b-%Y')}")
        current += timedelta(days=1)

    print(f"✅ Completed. {len(downloaded_files)} files downloaded.")
    return downloaded_files


if __name__ == "__main__":
    # Example usage
    output_dir = "../data/nse/equity/2024"
    start_date = "01JAN2024"
    end_date = "31Dec2024"
    
    downloaded_files = download_bhavcopy_range(start_date, end_date, output_dir)
    print(f"Downloaded {len(downloaded_files)} files")

