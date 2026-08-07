
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import logging
import pandas as pd
import requests
from tqdm import tqdm

from config import (
    BASE_URL,
    DOWNLOAD_LIMIT,
    DOWNLOAD_FOLDER,
    SELECTED_STATIONS_FILE,
    LOG_DIR,
    LOG_FILE
)
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
DOWNLOAD_FOLDER.mkdir(parents=True, exist_ok=True)

def download_station(station_id):
    filename = f"{station_id}.csv"
    destination = DOWNLOAD_FOLDER / filename
    if destination.exists():
        logging.info(f"Skipped: {filename}")
        return "Skipped"
    url = f"{BASE_URL}/{filename}"
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            destination.write_bytes(response.content)
            logging.info(f"Downloaded: {filename}")
            return "Downloaded"
        
        logging.warning(f"Missing: {filename}")
        return "Missing"

    except Exception as e:
        logging.error(f"{filename}: {e}")
        return "Failed"

stations = pd.read_csv(SELECTED_STATIONS_FILE)
station_list = stations["STATION_ID"].head(DOWNLOAD_LIMIT)

summary = {"Downloaded": 0,"Skipped": 0,"Missing": 0,"Failed": 0}
logging.info("Starting NOAA GSOD download...")
for station in tqdm(station_list):
    status = download_station(station)
    summary[status] += 1

print("\nDownload Summary\n")

for key, value in summary.items():
    print(f"{key}: {value}")

logging.info(f"Download Summary: {summary}")