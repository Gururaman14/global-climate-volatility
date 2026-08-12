import sys
from pathlib import Path

import boto3
from botocore.client import Config

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_BUCKET,
    RAW_LOCAL_DIR,
    YEAR,
)

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",
)

files = sorted(RAW_LOCAL_DIR.glob("*.csv"))

print("Local raw files:", len(files))

for file in files:
    key = f"raw/{YEAR}/{file.name}"
    s3.upload_file(str(file), MINIO_BUCKET, key)

print("Uploaded to MinIO:", len(files))