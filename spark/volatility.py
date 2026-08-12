import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import abs, avg, col, lit, stddev, when, coalesce

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET, YEAR

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

spark = (
    SparkSession.builder.appName("ClimateVolatility")
    .master("local[2]")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

df = spark.read.parquet(f"s3a://{MINIO_BUCKET}/processed/features/{YEAR}/")
print("Input records:", df.count())

# Global statistics
g = df.select(
    avg(abs(col("TEMP_ANOMALY"))).alias("ta_m"),
    stddev(abs(col("TEMP_ANOMALY"))).alias("ta_s"),
    avg("TEMP_7D_STD").alias("tv_m"), stddev("TEMP_7D_STD").alias("tv_s"),
    avg("PRCP_7D_STD").alias("pv_m"), stddev("PRCP_7D_STD").alias("pv_s"),
    avg("WDSP_7D_STD").alias("wv_m"), stddev("WDSP_7D_STD").alias("wv_s")
).first()

# Global z-scores
df = (
    df
    .withColumn("TEMP_ANOMALY_Z_GLOBAL",
        when((col("TEMP_ANOMALY").isNotNull()) & (lit(g.ta_s) > 0),
             (abs(col("TEMP_ANOMALY")) - g.ta_m) / g.ta_s))
    .withColumn("TEMP_VOLATILITY_Z_GLOBAL",
        when((col("TEMP_7D_STD").isNotNull()) & (lit(g.tv_s) > 0),
             (col("TEMP_7D_STD") - g.tv_m) / g.tv_s))
    .withColumn("PRCP_VOLATILITY_Z_GLOBAL",
        when((col("PRCP_7D_STD").isNotNull()) & (lit(g.pv_s) > 0),
             (col("PRCP_7D_STD") - g.pv_m) / g.pv_s))
    .withColumn("WIND_VOLATILITY_Z_GLOBAL",
        when((col("WDSP_7D_STD").isNotNull()) & (lit(g.wv_s) > 0),
             (col("WDSP_7D_STD") - g.wv_m) / g.wv_s))
)

# Station-level statistics
w = "STATION"
df = (
    df
    .withColumn("ta_m_s", avg(abs(col("TEMP_ANOMALY"))).over(__import__("pyspark").sql.Window.partitionBy(w)))
    .withColumn("ta_s_s", stddev(abs(col("TEMP_ANOMALY"))).over(__import__("pyspark").sql.Window.partitionBy(w)))
    .withColumn("tv_m_s", avg("TEMP_7D_STD").over(__import__("pyspark").sql.Window.partitionBy(w)))
    .withColumn("tv_s_s", stddev("TEMP_7D_STD").over(__import__("pyspark").sql.Window.partitionBy(w)))
    .withColumn("pv_m_s", avg("PRCP_7D_STD").over(__import__("pyspark").sql.Window.partitionBy(w)))
    .withColumn("pv_s_s", stddev("PRCP_7D_STD").over(__import__("pyspark").sql.Window.partitionBy(w)))
    .withColumn("wv_m_s", avg("WDSP_7D_STD").over(__import__("pyspark").sql.Window.partitionBy(w)))
    .withColumn("wv_s_s", stddev("WDSP_7D_STD").over(__import__("pyspark").sql.Window.partitionBy(w)))
)

# Station-level z-scores
df = (
    df
    .withColumn("TEMP_ANOMALY_Z_STATION",
        when((col("TEMP_ANOMALY").isNotNull()) & (col("ta_s_s") > 0),
             (abs(col("TEMP_ANOMALY")) - col("ta_m_s")) / col("ta_s_s")))
    .withColumn("TEMP_VOLATILITY_Z_STATION",
        when((col("TEMP_7D_STD").isNotNull()) & (col("tv_s_s") > 0),
             (col("TEMP_7D_STD") - col("tv_m_s")) / col("tv_s_s")))
    .withColumn("PRCP_VOLATILITY_Z_STATION",
        when((col("PRCP_7D_STD").isNotNull()) & (col("pv_s_s") > 0),
             (col("PRCP_7D_STD") - col("pv_m_s")) / col("pv_s_s")))
    .withColumn("WIND_VOLATILITY_Z_STATION",
        when((col("WDSP_7D_STD").isNotNull()) & (col("wv_s_s") > 0),
             (col("WDSP_7D_STD") - col("wv_m_s")) / col("wv_s_s")))
)

# Final station-level score: average only available components
parts = [
    col("TEMP_ANOMALY_Z_STATION"),
    col("TEMP_VOLATILITY_Z_STATION"),
    col("PRCP_VOLATILITY_Z_STATION"),
    col("WIND_VOLATILITY_Z_STATION")
]

score_sum = sum(coalesce(x, lit(0.0)) for x in parts)
score_n = sum(when(x.isNotNull(), 1).otherwise(0) for x in parts)

df = (
    df
    .withColumn("CLIMATE_VOLATILITY_SCORE",
        when(score_n > 0, score_sum / score_n))
    .withColumn("HIGH_VOLATILITY",
        when(col("CLIMATE_VOLATILITY_SCORE") >= 1.0, 1).otherwise(0))
)

out = df

print("Volatility records:", out.count())
print("Null scores:", out.filter(col("CLIMATE_VOLATILITY_SCORE").isNull()).count())
print("High-volatility days:", out.filter(col("HIGH_VOLATILITY") == 1).count())

out.write.mode("overwrite").parquet(
    f"s3a://{MINIO_BUCKET}/processed/volatility/{YEAR}/"
)

print("SUCCESS: Weather variability dataset created.")
spark.stop()