import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, dayofmonth, month, round, stddev, weekofyear, year
from pyspark.sql.window import Window


PROJECT_ROOT = Path(__file__).resolve().parent.parent

INPUT_PATH = "s3a://climate-data/processed/2024/"
OUTPUT_PATH = "s3a://climate-data/processed/features/2024/"


os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"


spark = (
    SparkSession.builder.appName("GlobalClimateVolatility-FeatureEngineering")
    .master("local[2]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config(
        "spark.jars",
        "/home/gururaman14/diss-venv/lib/python3.14/site-packages/pyspark/jars/hadoop-aws-3.5.0.jar,"
        "/home/gururaman14/diss-venv/lib/python3.14/site-packages/pyspark/jars/software.amazon.awssdk_bundle-2.35.4.jar,"
        "/home/gururaman14/diss-venv/lib/python3.14/site-packages/pyspark/jars/analyticsaccelerator-s3-1.3.1.jar"
    )
    .config("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
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
    df.withColumn("YEAR", year(col("DATE")))
      .withColumn("MONTH", month(col("DATE")))
      .withColumn("DAY", dayofmonth(col("DATE")))
      .withColumn("WEEK", weekofyear(col("DATE")))
)


df = df.withColumn("TEMP_RANGE", round(col("MAX") - col("MIN"), 2))


station_window = Window.partitionBy("STATION")

df = df.withColumn("TEMP_MEAN", avg("TEMP").over(station_window))

df = df.withColumn(
    "TEMP_ANOMALY",
    round(col("TEMP") - col("TEMP_MEAN"), 2)
)


rolling_window = (
    Window.partitionBy("STATION")
    .orderBy("DATE")
    .rowsBetween(-6, 0)
)


df = df.withColumn(
    "TEMP_7D_STD",
    round(stddev("TEMP").over(rolling_window), 2)
)

df = df.withColumn(
    "PRCP_7D_STD",
    round(stddev("PRCP").over(rolling_window), 2)
)

df = df.withColumn(
    "WDSP_7D_STD",
    round(stddev("WDSP").over(rolling_window), 2)
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

print("\nFeature-engineered schema:")
features.printSchema()

print("\nFeature sample:")
features.show(10, truncate=False)


print("\nWriting feature dataset to:")
print(OUTPUT_PATH)


features.write.mode("overwrite").parquet(OUTPUT_PATH)


print("\nSUCCESS: Feature engineering completed.")


spark.stop()