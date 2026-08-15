import logging
import sys
from pathlib import Path

import pandas as pd
import requests
from tqdm import tqdm

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import (NOAA_BASE_URL,DOWNLOAD_LIMIT,RAW_LOCAL_DIR,SELECTED_STATIONS_FILE,LOG_DIR,LOG_FILE,)

LOG_DIR.mkdir(parents=True, exist_ok=True)
RAW_LOCAL_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

def download_station(station_id):
    filename = f"{station_id}.csv"
    destination = RAW_LOCAL_DIR / filename

    if destination.exists():
        logging.info("Skipped: %s", filename)
        return "Skipped"

    url = f"{NOAA_BASE_URL}/{filename}"

    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            destination.write_bytes(response.content)
            logging.info("Downloaded: %s", filename)
            return "Downloaded"

        logging.warning("Missing: %s", filename)
        return "Missing"

    except Exception as exc:
        logging.error("%s: %s", filename, exc)
        return "Failed"

stations = pd.read_csv(SELECTED_STATIONS_FILE, dtype=str)
station_list = stations["STATION_ID"].head(DOWNLOAD_LIMIT)

summary = {"Downloaded": 0, "Skipped": 0, "Missing": 0, "Failed": 0}

print(f"Stations requested: {len(station_list)}")

for station in tqdm(station_list):
    status = download_station(station)
    summary[status] += 1

print("\nDownload Summary")
for key, value in summary.items():
    print(f"{key}: {value}")

logging.info("Download Summary: %s", summary)