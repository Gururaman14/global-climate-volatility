import os,sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,count,sum,avg,max,when

PROJECT_ROOT=Path(__file__).resolve().parent.parent
sys.path.insert(0,str(PROJECT_ROOT))
os.environ["SPARK_LOCAL_IP"]="127.0.0.1"
os.environ["SPARK_LOCAL_HOSTNAME"]="localhost"

INPUT_PATH="s3a://climate-data/processed/volatility/2024/"
OUTPUT_PATH="s3a://climate-data/processed/monthly/2024/"

spark=(SparkSession.builder.appName("GlobalClimateVolatility-MonthlyAggregation").master("local[2]")
.config("spark.driver.host","127.0.0.1")
.config("spark.driver.bindAddress","127.0.0.1")
.config("spark.hadoop.fs.s3a.endpoint","http://127.0.0.1:9000")
.config("spark.hadoop.fs.s3a.access.key","minioadmin")
.config("spark.hadoop.fs.s3a.secret.key","minioadmin")
.config("spark.hadoop.fs.s3a.path.style.access","true")
.config("spark.hadoop.fs.s3a.connection.ssl.enabled","false")
.config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
.getOrCreate())

spark.sparkContext.setLogLevel("WARN")

print("Reading volatility data from:",INPUT_PATH)
df=spark.read.parquet(INPUT_PATH)
print("Input records:",df.count())

monthly=df.groupBy(
    "STATION","NAME","LATITUDE","LONGITUDE","YEAR","MONTH"
).agg(
    count("*").alias("TOTAL_DAYS"),
    sum(when(col("HIGH_VOLATILITY")==1,1).otherwise(0)).alias("HIGH_VOLATILITY_DAYS"),
    avg("HIGH_VOLATILITY").alias("HIGH_VOLATILITY_RATE"),
    avg("CLIMATE_VOLATILITY_SCORE").alias("AVG_VOLATILITY_SCORE"),
    max("CLIMATE_VOLATILITY_SCORE").alias("MAX_VOLATILITY_SCORE"),
    avg("TEMP_ANOMALY").alias("AVG_TEMP_ANOMALY"),
    avg("TEMP_VOLATILITY_Z").alias("AVG_TEMP_VOLATILITY"),
    avg("PRCP_VOLATILITY_Z").alias("AVG_PRCP_VOLATILITY"),
    avg("WIND_VOLATILITY_Z").alias("AVG_WIND_VOLATILITY")
)

print("Monthly records:",monthly.count())
monthly.show(20,truncate=False)

print("Writing monthly dataset to:",OUTPUT_PATH)
monthly.write.mode("overwrite").parquet(OUTPUT_PATH)

print("SUCCESS: Monthly volatility dataset created.")
spark.stop()