import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, desc, sum

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from config import (MINIO_ENDPOINT,MINIO_ACCESS_KEY,MINIO_SECRET_KEY,MINIO_BUCKET,YEAR,MIN_DAYS,)

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

spark = (
    SparkSession.builder
    .appName("ClimateAnalysis")
    .master("local[2]")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

path = f"s3a://{MINIO_BUCKET}/processed/monthly/{YEAR}/"

df = spark.read.parquet(path)
print("Monthly records:", df.count())

df = df.filter(col("TOTAL_DAYS") >= MIN_DAYS)
print("Records after minimum-days filter:", df.count())

print("\nTop station-months by high-volatility rate:")
df.orderBy(desc("HIGH_VOLATILITY_RATE")).select(
    "STATION", "NAME", "YEAR", "MONTH",
    "TOTAL_DAYS", "HIGH_VOLATILITY_DAYS", "HIGH_VOLATILITY_RATE"
).show(10, truncate=False)

print("\nTop stations by average volatility:")
df.groupBy("STATION", "NAME").agg(
    avg("AVG_VOLATILITY_SCORE").alias("AVG_SCORE"),
    sum("HIGH_VOLATILITY_DAYS").alias("HIGH_VOLATILITY_DAYS"),
    count("*").alias("MONTHS")
).orderBy(desc("AVG_SCORE")).show(10, truncate=False)

print("\nMonthly volatility:")
df.groupBy("MONTH").agg(
    avg("HIGH_VOLATILITY_RATE").alias("AVG_HIGH_VOLATILITY_RATE"),
    avg("AVG_VOLATILITY_SCORE").alias("AVG_VOLATILITY_SCORE"),
    sum("HIGH_VOLATILITY_DAYS").alias("HIGH_VOLATILITY_DAYS")
).orderBy("MONTH").show(12, truncate=False)

print("\nHighest volatility station-months:")
df.orderBy(desc("AVG_VOLATILITY_SCORE")).select(
    "STATION", "NAME", "YEAR", "MONTH",
    "TOTAL_DAYS", "AVG_VOLATILITY_SCORE",
    "MAX_VOLATILITY_SCORE",
    "HIGH_VOLATILITY_DAYS",
    "HIGH_VOLATILITY_RATE"
).show(10, truncate=False)

spark.stop()
print("SUCCESS: Weather variability analysis completed.")