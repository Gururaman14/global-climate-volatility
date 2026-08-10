import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when


spark = SparkSession.builder \
    .appName("GlobalClimateVolatility-Cleaning") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config(
        "spark.hadoop.fs.file.impl",
        "org.apache.hadoop.fs.RawLocalFileSystem"
    ) \
    .config("spark.hadoop.fs.permissions.umask-mode", "000") \
    .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
    .getOrCreate()


RAW_DIR = PROJECT_ROOT / "data" / "raw" / "2024"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed" / "2024"

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


files = [
    str(file)
    for file in RAW_DIR.glob("*.csv")
]

print(f"Files found: {len(files)}")


df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(files)


print(f"Raw records: {df.count()}")


df = df.select(
    "STATION",
    "DATE",
    "LATITUDE",
    "LONGITUDE",
    "ELEVATION",
    "NAME",
    "TEMP",
    "DEWP",
    "SLP",
    "VISIB",
    "WDSP",
    "MXSPD",
    "GUST",
    "MAX",
    "MIN",
    "PRCP",
    "SNDP",
    "FRSHTT"
)


df = df.filter(
    col("DATE").isNotNull()
)


df = df.filter(
    col("TEMP").isNotNull()
)


df = df.withColumn(
    "TEMP",
    when(col("TEMP") > 999, None)
    .otherwise(col("TEMP"))
)

df = df.withColumn(
    "MAX",
    when(col("MAX") > 999, None)
    .otherwise(col("MAX"))
)

df = df.withColumn(
    "MIN",
    when(col("MIN") > 999, None)
    .otherwise(col("MIN"))
)

df = df.withColumn(
    "PRCP",
    when(col("PRCP") > 999, None)
    .otherwise(col("PRCP"))
)


df = df.dropDuplicates(
    ["STATION", "DATE"]
)


print(f"Clean records: {df.count()}")


print("\nCleaned schema:")
df.printSchema()


print("\nCleaned sample:")
df.show(10, truncate=False)


OUTPUT_PATH = str(PROCESSED_DIR)

df.write \
    .mode("overwrite") \
    .parquet(OUTPUT_PATH)


print(f"\nSaved cleaned data to: {OUTPUT_PATH}")


spark.stop()