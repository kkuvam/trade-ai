#!/usr/bin/python3

import os
import shutil
import gzip
import requests
from pathlib import Path
from datetime import datetime, timedelta
from typing import Union

BASE_URL = "https://www.bseindia.com/download/BhavCopy"
output_dir = "../data/bse/equity/bse"
TIMEOUT = 20  # seconds

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate",
    "Referer": "https://www.nseindia.com"
}


def download_file(url: str, dest_path: Path, headers=HEADERS):
    try:
        response = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        if response.status_code == 200:
            with open(dest_path, "wb") as f:
                f.write(response.content)
            print(f"✅ Downloaded: {dest_path}")
            return dest_path
        else:
            raise Exception(f"HTTP {response.status_code}")
    except Exception as e:
        print(f"❌ Failed for {url}: {e}")
        return None


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


def download_bse_equity_bhavcopy(date_str: str, output_dir: Union[str, Path]) -> Path:
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
    
   
    # Download URL and destination
    url = f"{BASE_URL}/Equity/BhavCopy_BSE_CM_0_0_0_{date_obj:%Y%m%d}_F_0000.CSV"
    output_dir = Path(output_dir)
    file_path = output_dir / f"{date_obj:%d%b%Y}.csv"
    
    # Download and extract
    download_file(url, file_path)
    
    # Rename file
    final_filename = date_obj.strftime("%d%b%Y").upper() + ".csv"
    final_path = output_dir / final_filename
    
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
                file_path = download_bse_equity_bhavcopy(date_str, output_dir)
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
    start_date = "01JAN2024"
    end_date = "31DEC2024"
    output_directory = "../data/bse/equity/2024"

    downloaded_files = download_bhavcopy_range(start_date, end_date, output_directory)
    print(f"Downloaded {len(downloaded_files)} files")