import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, dayofmonth, month, round, stddev, weekofyear, year
from pyspark.sql.window import Window

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET, YEAR

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"

INPUT_PATH = f"s3a://{MINIO_BUCKET}/processed/{YEAR}/"
OUTPUT_PATH = f"s3a://{MINIO_BUCKET}/processed/features/{YEAR}/"

spark = (
    SparkSession.builder
    .appName("GlobalClimateVolatility-FeatureEngineering")
    .master("local[2]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Reading processed data from:")
print(INPUT_PATH)

df = spark.read.parquet(INPUT_PATH)

print("\nInput records:", df.count())

df = (
    df
    .withColumn("YEAR", year(col("DATE")))
    .withColumn("MONTH", month(col("DATE")))
    .withColumn("DAY", dayofmonth(col("DATE")))
    .withColumn("WEEK", weekofyear(col("DATE")))
)

df = df.withColumn(
    "TEMP_RANGE",
    round(col("MAX") - col("MIN"), 2)
)

station_window = Window.partitionBy("STATION")

df = df.withColumn(
    "TEMP_MEAN",
    avg("TEMP").over(station_window)
)

df = df.withColumn(
    "TEMP_ANOMALY",
    round(col("TEMP") - col("TEMP_MEAN"), 2)
)

rolling_window = (
    Window.partitionBy("STATION")
    .orderBy("DATE")
    .rowsBetween(-6, 0)
)

df = (
    df
    .withColumn("TEMP_7D_STD", round(stddev("TEMP").over(rolling_window), 2))
    .withColumn("PRCP_7D_STD", round(stddev("PRCP").over(rolling_window), 2))
    .withColumn("WDSP_7D_STD", round(stddev("WDSP").over(rolling_window), 2))
)

features = df.select(
    "STATION", "DATE", "LATITUDE", "LONGITUDE", "ELEVATION", "NAME",
    "YEAR", "MONTH", "DAY", "WEEK",
    "TEMP", "MAX", "MIN",
    "TEMP_RANGE", "TEMP_MEAN", "TEMP_ANOMALY",
    "DEWP", "SLP", "VISIB", "WDSP", "MXSPD", "GUST", "PRCP",
    "TEMP_7D_STD", "PRCP_7D_STD", "WDSP_7D_STD"
)

print("\nFeature-engineered records:", features.count())

features.write.mode("overwrite").parquet(OUTPUT_PATH)

print("\nSUCCESS: Feature engineering completed.")

spark.stop()