import pandas as pd
from pathlib import Path

METADATA_FILE = Path("data/metadata/isd-history.csv")
OUTPUT_FILE = Path("data/metadata/selected_stations.csv")

df = pd.read_csv(METADATA_FILE, dtype=str)

print(f"Original stations: {len(df)}")

df = df.replace(r'^\s*$', pd.NA, regex=True)

df = df.dropna(subset=["LAT", "LON"])

df["END"] = pd.to_numeric(df["END"], errors="coerce")

df = df[df["END"] >= 20240000]

df = df.drop_duplicates(subset=["USAF", "WBAN"])

df = df.dropna(subset=["USAF", "WBAN"])

df["STATION_ID"] = df["USAF"] + df["WBAN"]
columns = ["STATION_ID","STATION NAME","CTRY","LAT","LON","ELEV(M)",  "BEGIN","END"]
df = df[columns]
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(OUTPUT_FILE, index=False)
print(f"Active stations: {len(df)}")
print(f"Saved to {OUTPUT_FILE}")
print(df.head())