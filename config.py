import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data"
METADATA_DIR = DATA_DIR / "metadata"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
LOG_DIR = PROJECT_ROOT / "logs"
LOG_FILE = LOG_DIR / "ingestion.log"

YEAR = "2024"
DOWNLOAD_LIMIT = 80
MIN_DAYS = 20

NOAA_BASE_URL = f"https://noaa-gsod-pds.s3.amazonaws.com/{YEAR}"
METADATA_URL = "https://www.ncei.noaa.gov/pub/data/noaa/isd-history.csv"
METADATA_FILE = METADATA_DIR / "isd-history.csv"
SELECTED_STATIONS_FILE = METADATA_DIR / "selected_stations.csv"
RAW_LOCAL_DIR = RAW_DATA_DIR / YEAR

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://127.0.0.1:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "climate-data")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "climate_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "climate_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "climate_pass")
POSTGRES_SCHEMA = "climate"

LOG_DIR.mkdir(parents=True, exist_ok=True)
RAW_LOCAL_DIR.mkdir(parents=True, exist_ok=True)