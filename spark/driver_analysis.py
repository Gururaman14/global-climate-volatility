import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, desc

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from config import (MINIO_ENDPOINT,MINIO_ACCESS_KEY,MINIO_SECRET_KEY,MINIO_BUCKET,YEAR,MIN_DAYS,)

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

spark = (
    SparkSession.builder
    .appName("ClimateDriverAnalysis")
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
    f"s3a://{MINIO_BUCKET}/processed/monthly/{YEAR}/"
)

print("Monthly records:", df.count())

df = df.filter(col("TOTAL_DAYS") >= MIN_DAYS)

print("Records after minimum-days filter:", df.count())

print("\nHighest-risk station-months:")
df.orderBy(desc("AVG_VOLATILITY_SCORE")).select(
    "STATION",
    "NAME",
    "YEAR",
    "MONTH",
    "TOTAL_DAYS",
    "AVG_VOLATILITY_SCORE",
    "AVG_TEMP_VOLATILITY",
    "AVG_PRCP_VOLATILITY",
    "AVG_WIND_VOLATILITY",
    "HIGH_VOLATILITY_RATE"
).show(10, truncate=False)

print("\nAverage components by month:")
df.groupBy("MONTH").agg(
    avg("AVG_TEMP_VOLATILITY").alias("AVG_TEMP_VOLATILITY"),
    avg("AVG_PRCP_VOLATILITY").alias("AVG_PRCP_VOLATILITY"),
    avg("AVG_WIND_VOLATILITY").alias("AVG_WIND_VOLATILITY"),
    avg("AVG_VOLATILITY_SCORE").alias("AVG_VOLATILITY_SCORE")
).orderBy("MONTH").show(12, truncate=False)

print("\nAverage components by station:")
df.groupBy("STATION", "NAME").agg(
    avg("AVG_TEMP_VOLATILITY").alias("AVG_TEMP_VOLATILITY"),
    avg("AVG_PRCP_VOLATILITY").alias("AVG_PRCP_VOLATILITY"),
    avg("AVG_WIND_VOLATILITY").alias("AVG_WIND_VOLATILITY"),
    avg("AVG_VOLATILITY_SCORE").alias("AVG_VOLATILITY_SCORE")
).orderBy(desc("AVG_VOLATILITY_SCORE")).show(10, truncate=False)

spark.stop()

print("SUCCESS: Component analysis completed.")