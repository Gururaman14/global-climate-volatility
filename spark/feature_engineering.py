import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round, stddev, year, month, dayofmonth, weekofyear
from pyspark.sql.window import Window

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
os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"

spark = (
    SparkSession.builder
    .appName("WeatherVariabilityFeatureEngineering")
    .master("local[2]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

input_path = f"s3a://{MINIO_BUCKET}/processed/{YEAR}/"
output_path = f"s3a://{MINIO_BUCKET}/processed/features/{YEAR}/"

print("Reading processed data from:")
print(input_path)

df = spark.read.parquet(input_path)

print("Input records:", df.count())

# Ensure calendar fields required by downstream aggregation exist
df = (
    df
    .withColumn("YEAR", year(col("DATE")))
    .withColumn("MONTH", month(col("DATE")))
    .withColumn("DAY", dayofmonth(col("DATE")))
    .withColumn("WEEK", weekofyear(col("DATE")))
)

# Temperature range
df = df.withColumn(
    "TEMP_RANGE",
    round(col("MAX") - col("MIN"), 2)
)

# Within-year temperature deviation from each station's 2024 annual mean.
# This is not a climatological anomaly because only one year is analysed.
station_window = Window.partitionBy("STATION")

df = (
    df
    .withColumn("TEMP_MEAN", avg("TEMP").over(station_window))
    .withColumn(
        "TEMP_ANOMALY",
        round(col("TEMP") - col("TEMP_MEAN"), 2)
    )
)

# Backward-looking 7-day variability
rolling_window = (
    Window
    .partitionBy("STATION")
    .orderBy("DATE")
    .rowsBetween(-6, 0)
)

df = (
    df
    .withColumn("TEMP_7D_STD", stddev("TEMP").over(rolling_window))
    .withColumn("PRCP_7D_STD", stddev("PRCP").over(rolling_window))
    .withColumn("WDSP_7D_STD", stddev("WDSP").over(rolling_window))
)

print("Feature-engineered records:", df.count())

print("Feature-engineered schema:")
df.printSchema()

df.select(
    "STATION",
    "DATE",
    "YEAR",
    "MONTH",
    "TEMP_MEAN",
    "TEMP_ANOMALY",
    "TEMP_7D_STD",
    "PRCP_7D_STD",
    "WDSP_7D_STD",
).show(10, truncate=False)

df.write.mode("overwrite").parquet(output_path)

print("SUCCESS: Weather variability feature engineering completed.")

spark.stop()