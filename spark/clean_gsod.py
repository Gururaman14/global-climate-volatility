import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

from config import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_BUCKET,
    YEAR,
)
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"

RAW_PATH = f"s3a://{MINIO_BUCKET}/raw/{YEAR}/"
OUTPUT_PATH = f"s3a://{MINIO_BUCKET}/processed/{YEAR}/"

spark = (
    SparkSession.builder
    .appName("GlobalClimateVolatility-Cleaning")
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

print("Reading raw data from:")
print(RAW_PATH)

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .option("pathGlobFilter", "*.csv")
    .csv(RAW_PATH)
)

records_before = df.count()
print("\nRecords before cleaning:", records_before)

df = df.dropDuplicates()

df = df.withColumn("TEMP", when(col("TEMP") == 999.9, None).otherwise(col("TEMP")))
df = df.withColumn("MAX", when(col("MAX") == 999.9, None).otherwise(col("MAX")))
df = df.withColumn("MIN", when(col("MIN") == 999.9, None).otherwise(col("MIN")))
df = df.withColumn("PRCP", when(col("PRCP") == 99.99, None).otherwise(col("PRCP")))
df = df.withColumn("SLP", when(col("SLP") == 9999.9, None).otherwise(col("SLP")))
df = df.withColumn("STP", when(col("STP") == 999.9, None).otherwise(col("STP")))
df = df.withColumn("WDSP", when(col("WDSP") == 999.9, None).otherwise(col("WDSP")))
df = df.withColumn("GUST", when(col("GUST") == 999.9, None).otherwise(col("GUST")))
df = df.withColumn("MXSPD", when(col("MXSPD") == 999.9, None).otherwise(col("MXSPD")))
df = df.withColumn("VISIB", when(col("VISIB") == 999.9, None).otherwise(col("VISIB")))
df = df.withColumn("SNDP", when(col("SNDP") == 999.9, None).otherwise(col("SNDP")))

df = df.filter(col("STATION").isNotNull())
df = df.filter(col("DATE").isNotNull())
df = df.filter(col("LATITUDE").isNotNull())
df = df.filter(col("LONGITUDE").isNotNull())

records_after = df.count()

print("\nRecords after cleaning:", records_after)
print("Records removed:", records_before - records_after)

print("\nMissing values after sentinel conversion:")
df.select([
    col(c).isNull().cast("int").alias(c)
    for c in ["TEMP", "MAX", "MIN", "PRCP", "SLP", "STP","WDSP", "GUST", "MXSPD", "VISIB", "SNDP"]
]).show()

print("\nCleaned sample:")
df.show(10, truncate=False)

print("\nWriting cleaned Parquet to:")
print(OUTPUT_PATH)

df.write.mode("overwrite").parquet(OUTPUT_PATH)

print("\nSUCCESS: Cleaned GSOD data written to Parquet.")

spark.stop()