import os,sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,abs,avg,stddev,coalesce

PROJECT_ROOT=Path(__file__).resolve().parent.parent
sys.path.insert(0,str(PROJECT_ROOT))
os.environ["SPARK_LOCAL_IP"]="127.0.0.1"
os.environ["SPARK_LOCAL_HOSTNAME"]="localhost"

INPUT_PATH="s3a://climate-data/processed/features/2024/"
OUTPUT_PATH="s3a://climate-data/processed/volatility/2024/"

spark=(SparkSession.builder.appName("GlobalClimateVolatility-Scoring").master("local[2]")
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

print("Reading feature data from:",INPUT_PATH)
df=spark.read.parquet(INPUT_PATH)
print("Input records:",df.count())

stats=df.select(
    avg("TEMP_ANOMALY").alias("TEMP_ANOMALY_MEAN"),
    stddev("TEMP_ANOMALY").alias("TEMP_ANOMALY_STD"),
    avg("TEMP_7D_STD").alias("TEMP_7D_STD_MEAN"),
    stddev("TEMP_7D_STD").alias("TEMP_7D_STD_STD"),
    avg("PRCP_7D_STD").alias("PRCP_7D_STD_MEAN"),
    stddev("PRCP_7D_STD").alias("PRCP_7D_STD_STD"),
    avg("WDSP_7D_STD").alias("WDSP_7D_STD_MEAN"),
    stddev("WDSP_7D_STD").alias("WDSP_7D_STD_STD")
).collect()[0]

features=(df
.withColumn("TEMP_ANOMALY_ABS",abs(col("TEMP_ANOMALY")))
.withColumn("TEMP_ANOMALY_Z",(abs(col("TEMP_ANOMALY"))-stats["TEMP_ANOMALY_MEAN"])/stats["TEMP_ANOMALY_STD"])
.withColumn("TEMP_VOLATILITY_Z",(col("TEMP_7D_STD")-stats["TEMP_7D_STD_MEAN"])/stats["TEMP_7D_STD_STD"])
.withColumn("PRCP_VOLATILITY_Z",(col("PRCP_7D_STD")-stats["PRCP_7D_STD_MEAN"])/stats["PRCP_7D_STD_STD"])
.withColumn("WIND_VOLATILITY_Z",(col("WDSP_7D_STD")-stats["WDSP_7D_STD_MEAN"])/stats["WDSP_7D_STD_STD"]))

features=features.withColumn(
    "CLIMATE_VOLATILITY_SCORE",
    (coalesce(col("TEMP_ANOMALY_Z"),col("TEMP_ANOMALY_Z")*0)
    +coalesce(col("TEMP_VOLATILITY_Z"),col("TEMP_VOLATILITY_Z")*0)
    +coalesce(col("PRCP_VOLATILITY_Z"),col("PRCP_VOLATILITY_Z")*0)
    +coalesce(col("WIND_VOLATILITY_Z"),col("WIND_VOLATILITY_Z")*0))/4)

features=features.withColumn("HIGH_VOLATILITY",(col("CLIMATE_VOLATILITY_SCORE")>=1).cast("int"))

features=features.select(
    "STATION","DATE","LATITUDE","LONGITUDE","ELEVATION","NAME",
    "YEAR","MONTH","DAY","WEEK","TEMP","MAX","MIN","TEMP_RANGE",
    "TEMP_MEAN","TEMP_ANOMALY","DEWP","SLP","VISIB","WDSP","MXSPD",
    "GUST","PRCP","TEMP_7D_STD","PRCP_7D_STD","WDSP_7D_STD",
    "TEMP_ANOMALY_ABS","TEMP_ANOMALY_Z","TEMP_VOLATILITY_Z",
    "PRCP_VOLATILITY_Z","WIND_VOLATILITY_Z",
    "CLIMATE_VOLATILITY_SCORE","HIGH_VOLATILITY")

print("Volatility records:",features.count())
features.select("STATION","DATE","TEMP_ANOMALY","TEMP_7D_STD","PRCP_7D_STD","WDSP_7D_STD","CLIMATE_VOLATILITY_SCORE","HIGH_VOLATILITY").show(10,truncate=False)

print("Writing volatility dataset to:",OUTPUT_PATH)
features.write.mode("overwrite").parquet(OUTPUT_PATH)
print("SUCCESS: Climate volatility dataset created.")
spark.stop()