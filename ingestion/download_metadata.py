import requests
from pathlib import Path
from tqdm import tqdm

METADATA_URL = "https://www.ncei.noaa.gov/pub/data/noaa/isd-history.csv"

OUTPUT_DIR = Path("data/metadata")
OUTPUT_FILE = OUTPUT_DIR / "isd-history.csv"

def download_file(url: str, destination: Path):
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        print(f"[INFO] File already exists: {destination}")
        return
    print("[INFO] Downloading station metadata...")

    response = requests.get(url, stream=True, timeout=60)
    response.raise_for_status()
    total_size = int(response.headers.get("content-length", 0))

    with open(destination, "wb") as file, tqdm(desc=destination.name,total=total_size, unit="B", unit_scale=True, unit_divisor=1024,) as progress:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                file.write(chunk)
                progress.update(len(chunk))
    print(f"[SUCCESS] Saved to {destination}")

def main():
    try:
        download_file(METADATA_URL, OUTPUT_FILE)
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Download failed: {e}")
    except Exception as e:
        print(f"[ERROR] {e}")

if __name__ == "__main__":
    main()