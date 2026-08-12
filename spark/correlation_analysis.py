import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit, when

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from config import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_BUCKET,
    YEAR,
)

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

spark = (
    SparkSession.builder
    .appName("ClimateCorrelation")
    .master("local[2]")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

df = spark.read.parquet(
    f"s3a://{MINIO_BUCKET}/processed/volatility/{YEAR}/"
)

print("Volatility records:", df.count())

components = {
    "TEMP_ANOMALY_Z": "TEMP_ANOMALY_Z_GLOBAL",
    "TEMP_VOLATILITY_Z": "TEMP_VOLATILITY_Z_GLOBAL",
    "PRCP_VOLATILITY_Z": "PRCP_VOLATILITY_Z_GLOBAL",
    "WIND_VOLATILITY_Z": "WIND_VOLATILITY_Z_GLOBAL",
}

print("\nOriginal correlations:")

for name, component in components.items():
    r = df.stat.corr(component, "CLIMATE_VOLATILITY_SCORE")
    print(f"{name}: {r:.4f}")

print("\nLeave-one-component-out correlations:")

items = list(components.items())

for name, component in items:
    others = [c for _, c in items if c != component]

    total = sum(
        coalesce(col(c), lit(0.0))
        for c in others
    )

    n = sum(
        when(col(c).isNotNull(), 1).otherwise(0)
        for c in others
    )

    loo = df.withColumn(
        "LOO_SCORE",
        when(n > 0, total / n)
    )

    r = loo.stat.corr(component, "LOO_SCORE")
    print(f"{name}: {r:.4f}")

print("\nComponent correlations:")

for i in range(len(items)):
    for j in range(i + 1, len(items)):
        name1, col1 = items[i]
        name2, col2 = items[j]
        r = df.stat.corr(col1, col2)
        print(f"{name1} vs {name2}: {r:.4f}")

spark.stop()

print("\nSUCCESS: Correlation analysis completed.")