import sys
from pathlib import Path

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET, RAW_LOCAL_DIR, YEAR

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",
)

try:
    s3.head_bucket(Bucket=MINIO_BUCKET)
    print(f"MinIO bucket exists: {MINIO_BUCKET}")
except ClientError as e:
    code = e.response.get("Error", {}).get("Code", "")
    if code in ("404", "400", "NoSuchBucket"):
        s3.create_bucket(Bucket=MINIO_BUCKET)
        print(f"Created MinIO bucket: {MINIO_BUCKET}")
    else:
        raise

files = sorted(RAW_LOCAL_DIR.glob("*.csv"))

print("Local raw files:", len(files))

for file in files:
    key = f"raw/{YEAR}/{file.name}"
    s3.upload_file(str(file), MINIO_BUCKET, key)

print("Uploaded to MinIO:", len(files))