import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, max, sum, when

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from config import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_BUCKET,
    YEAR,
)

spark = (
    SparkSession.builder
    .appName("ClimateAggregation")
    .master("local[2]")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

input_path = f"s3a://{MINIO_BUCKET}/processed/volatility/{YEAR}/"
output_path = f"s3a://{MINIO_BUCKET}/processed/aggregated/{YEAR}/"

print("Reading volatility data from:", input_path)

df = spark.read.parquet(input_path)

print("Input records:", df.count())

agg = (
    df.groupBy("STATION", "NAME", "LATITUDE", "LONGITUDE")
    .agg(
        count("*").alias("TOTAL_DAYS"),
        sum(when(col("HIGH_VOLATILITY") == 1, 1).otherwise(0))
            .alias("HIGH_VOLATILITY_DAYS"),
        avg("HIGH_VOLATILITY").alias("HIGH_VOLATILITY_RATE"),
        avg("CLIMATE_VOLATILITY_SCORE").alias("AVG_VOLATILITY_SCORE"),
        max("CLIMATE_VOLATILITY_SCORE").alias("MAX_VOLATILITY_SCORE"),
        avg("TEMP_ANOMALY").alias("AVG_TEMP_ANOMALY"),
        avg("TEMP_VOLATILITY_Z_STATION").alias("AVG_TEMP_VOLATILITY"),
        avg("PRCP_VOLATILITY_Z_STATION").alias("AVG_PRCP_VOLATILITY"),
        avg("WIND_VOLATILITY_Z_STATION").alias("AVG_WIND_VOLATILITY"),
    )
)

print("Aggregated stations:", agg.count())

agg.write.mode("overwrite").parquet(output_path)

print("SUCCESS: Station aggregation completed.")

spark.stop()