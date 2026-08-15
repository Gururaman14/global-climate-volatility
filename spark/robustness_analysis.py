import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    lit,
    max as spark_max,
    sum as spark_sum,
    when
)

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
    .appName("WeatherVariabilityRobustnessAnalysis")
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

DAILY_PATH = f"s3a://{MINIO_BUCKET}/processed/volatility/{YEAR}/"

print("Reading volatility data from:")
print(DAILY_PATH)

df = spark.read.parquet(DAILY_PATH)

print("Daily records:", df.count())

thresholds = [0.75, 1.00, 1.25, 1.50]

print("\n=== THRESHOLD SENSITIVITY ===")

threshold_results = []

for threshold in thresholds:

    daily_threshold = df.withColumn(
        "HIGH_EVENT",
        when(col("CLIMATE_VOLATILITY_SCORE") >= lit(threshold), 1).otherwise(0)
    )

    result = daily_threshold.agg(
        count("*").alias("DAILY_RECORDS"),
        spark_sum("HIGH_EVENT").alias("HIGH_VOLATILITY_DAYS"),
        avg("HIGH_EVENT").alias("HIGH_VOLATILITY_RATE")
    ).collect()[0]

    monthly = (
        daily_threshold
        .groupBy("STATION", "NAME", "YEAR", "MONTH")
        .agg(
            count("*").alias("TOTAL_DAYS"),
            spark_sum("HIGH_EVENT").alias("HIGH_VOLATILITY_DAYS"),
            avg("CLIMATE_VOLATILITY_SCORE").alias("AVG_VOLATILITY_SCORE")
        )
        .withColumn(
            "HIGH_VOLATILITY_RATE",
            col("HIGH_VOLATILITY_DAYS") / col("TOTAL_DAYS")
        )
    )

    qualified = monthly.filter(col("TOTAL_DAYS") >= 20)

    qualified_count = qualified.count()

    highest_month_row = (
        qualified
        .groupBy("MONTH")
        .agg(
            avg("HIGH_VOLATILITY_RATE").alias("AVG_HIGH_VOLATILITY_RATE")
        )
        .orderBy(col("AVG_HIGH_VOLATILITY_RATE").desc())
        .first()
    )

    highest_month = (
        int(highest_month_row["MONTH"])
        if highest_month_row
        else None
    )

    highest_month_rate = (
        float(highest_month_row["AVG_HIGH_VOLATILITY_RATE"])
        if highest_month_row
        else None
    )

    threshold_results.append({
        "THRESHOLD": threshold,
        "DAILY_RECORDS": int(result["DAILY_RECORDS"]),
        "HIGH_VOLATILITY_DAYS": int(result["HIGH_VOLATILITY_DAYS"]),
        "OVERALL_HIGH_VOLATILITY_RATE": float(result["HIGH_VOLATILITY_RATE"]),
        "QUALIFIED_STATION_MONTHS": int(qualified_count),
        "HIGHEST_AVG_RATE_MONTH": highest_month,
        "HIGHEST_MONTH_AVG_RATE": highest_month_rate,
    })

    print(
        f"Threshold={threshold:.2f} | "
        f"High-days={int(result['HIGH_VOLATILITY_DAYS'])} | "
        f"Overall-rate={float(result['HIGH_VOLATILITY_RATE']):.4f} | "
        f"Qualified station-months={qualified_count} | "
        f"Highest month={highest_month}"
    )


min_days_values = [10, 15, 20, 25, 30]

print("\n=== MINIMUM-DAYS SENSITIVITY ===")

# Use the original 1.0 threshold for this experiment.
base_daily = df.withColumn(
    "HIGH_EVENT",
    when(col("CLIMATE_VOLATILITY_SCORE") >= lit(1.0), 1).otherwise(0)
)

monthly_base = (
    base_daily
    .groupBy("STATION", "NAME", "YEAR", "MONTH")
    .agg(
        count("*").alias("TOTAL_DAYS"),
        spark_sum("HIGH_EVENT").alias("HIGH_VOLATILITY_DAYS"),
        avg("CLIMATE_VOLATILITY_SCORE").alias("AVG_VOLATILITY_SCORE")
    )
    .withColumn(
        "HIGH_VOLATILITY_RATE",
        col("HIGH_VOLATILITY_DAYS") / col("TOTAL_DAYS")
    )
)

min_days_results = []

for min_days in min_days_values:

    qualified = monthly_base.filter(
        col("TOTAL_DAYS") >= min_days
    )

    qualified_count = qualified.count()

    month_row = (
        qualified
        .groupBy("MONTH")
        .agg(
            avg("HIGH_VOLATILITY_RATE").alias("AVG_HIGH_VOLATILITY_RATE")
        )
        .orderBy(col("AVG_HIGH_VOLATILITY_RATE").desc())
        .first()
    )

    highest_month = (
        int(month_row["MONTH"])
        if month_row
        else None
    )

    highest_month_rate = (
        float(month_row["AVG_HIGH_VOLATILITY_RATE"])
        if month_row
        else None
    )

    min_days_results.append({
        "MIN_DAYS": min_days,
        "QUALIFIED_STATION_MONTHS": int(qualified_count),
        "HIGHEST_AVG_RATE_MONTH": highest_month,
        "HIGHEST_MONTH_AVG_RATE": highest_month_rate,
    })

    print(
        f"MIN_DAYS={min_days} | "
        f"Qualified station-months={qualified_count} | "
        f"Highest month={highest_month}"
    )

print("\n=== ROBUSTNESS INTERPRETATION ===")

print(
    "Threshold sensitivity evaluates whether the number of high-volatility "
    "days and the ranking of the highest-risk month change substantially "
    "when the high-volatility threshold is varied."
)

print(
    "Minimum-days sensitivity evaluates whether the number of qualified "
    "station-months and the highest-risk month are strongly dependent on "
    "the minimum observation requirement."
)

print(
    "The baseline project configuration uses a volatility threshold of 1.0 "
    "and a minimum of 20 observation days per station-month."
)


output_dir = ROOT / "data" / "analysis"
output_dir.mkdir(parents=True, exist_ok=True)

import pandas as pd

threshold_pd = pd.DataFrame(threshold_results)
threshold_file = output_dir / "threshold_sensitivity.csv"
threshold_pd.to_csv(threshold_file, index=False)

min_days_pd = pd.DataFrame(min_days_results)
min_days_file = output_dir / "min_days_sensitivity.csv"
min_days_pd.to_csv(min_days_file, index=False)

print("\nSaved:")
print(threshold_file)
print(min_days_file)

print("\nSUCCESS: Robustness analysis completed.")

spark.stop()