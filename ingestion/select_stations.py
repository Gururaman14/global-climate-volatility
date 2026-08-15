import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import logging
import pandas as pd

from config import (
    METADATA_FILE,
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

logging.info("Reading station metadata...")

df = pd.read_csv(METADATA_FILE, dtype=str)

print(f"Original stations: {len(df)}")

df = df.replace(r'^\s*$', pd.NA, regex=True)

df = df.dropna(subset=["LAT", "LON"])

df["END"] = pd.to_numeric(df["END"], errors="coerce")

df = df[df["END"] >= 20240000]

df = df.drop_duplicates(subset=["USAF", "WBAN"])

df = df.dropna(subset=["USAF", "WBAN"])

df["STATION_ID"] = df["USAF"] + df["WBAN"]

columns = [
    "STATION_ID",
    "STATION NAME",
    "CTRY",
    "LAT",
    "LON",
    "ELEV(M)",
    "BEGIN",
    "END"
]

df = df[columns]

SELECTED_STATIONS_FILE.parent.mkdir(parents=True, exist_ok=True)

df.to_csv(SELECTED_STATIONS_FILE, index=False)

logging.info(f"Selected {len(df)} active stations.")

print(f"Active stations: {len(df)}")
print(f"Saved to {SELECTED_STATIONS_FILE}")
print(df.head())