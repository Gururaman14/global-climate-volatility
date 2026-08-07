"""
Project configuration
"""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent

DATA_DIR = PROJECT_ROOT / "data"
METADATA_DIR = DATA_DIR / "metadata"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"

YEAR = "2024"
DOWNLOAD_LIMIT = 10

LOG_DIR = PROJECT_ROOT / "logs"
LOG_FILE = LOG_DIR / "ingestion.log"

BASE_URL = f"https://noaa-gsod-pds.s3.amazonaws.com/{YEAR}"

METADATA_URL = "https://www.ncei.noaa.gov/pub/data/noaa/isd-history.csv"

METADATA_FILE = METADATA_DIR / "isd-history.csv"

SELECTED_STATIONS_FILE = METADATA_DIR / "selected_stations.csv"

DOWNLOAD_FOLDER = RAW_DATA_DIR / YEAR