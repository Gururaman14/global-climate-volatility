import os, sys
from pathlib import Path
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, min, max

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
from config import MINIO_BUCKET, YEAR

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"

spark = (
    SparkSession.builder
    .appName("WeatherVariabilityCompositeDiagnostic")
    .master("local[2]")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

path = f"s3a://{MINIO_BUCKET}/processed/volatility/{YEAR}/"
df = spark.read.parquet(path)

components = [
    "TEMP_ANOMALY_Z_GLOBAL",
    "TEMP_VOLATILITY_Z_GLOBAL",
    "PRCP_VOLATILITY_Z_GLOBAL",
    "WIND_VOLATILITY_Z_GLOBAL"
]

df = df.withColumn(
    "COMPONENT_COUNT",
    sum(col(c).isNotNull().cast("int") for c in components)
)

print("=== COMPOSITE SCORE METHODOLOGY ===")
print("Total records:", df.count())
print("Equal component weights: 25% each when all four are available")

print("\n=== COMPONENT AVAILABILITY ===")

availability = (
    df.groupBy("COMPONENT_COUNT")
    .count()
    .orderBy("COMPONENT_COUNT")
)

availability.show()

print("\n=== COMPOSITE SCORE BY COMPONENT COUNT ===")

df.groupBy("COMPONENT_COUNT").agg(
    count("*").alias("RECORDS"),
    avg("CLIMATE_VOLATILITY_SCORE").alias("AVG_SCORE"),
    min("CLIMATE_VOLATILITY_SCORE").alias("MIN_SCORE"),
    max("CLIMATE_VOLATILITY_SCORE").alias("MAX_SCORE")
).orderBy("COMPONENT_COUNT").show()

print("\n=== METHODOLOGY NOTES ===")
print(
    "The composite score uses equal weighting because no component has "
    "a theoretically justified priority in the current study."
)
print(
    "When component values are missing, the score is calculated from the "
    "available components; therefore the effective number of components "
    "varies across observations."
)
print(
    "Equal weighting is treated as a transparent baseline assumption, "
    "not as evidence that all meteorological components are equally "
    "important in the real world."
)

print("\nSUCCESS: Composite methodology diagnostic completed.")

spark.stop()