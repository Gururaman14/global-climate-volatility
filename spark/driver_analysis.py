from pyspark.sql import SparkSession
from pyspark.sql.functions import desc,avg

INPUT_PATH="s3a://climate-data/processed/monthly/2024/"

spark=(SparkSession.builder.appName("GlobalClimateVolatility-DriverAnalysis").master("local[2]")
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
print("Reading monthly data from:",INPUT_PATH)
df=spark.read.parquet(INPUT_PATH)
print("Monthly records:",df.count())

print("\nVolatility drivers for highest-risk months:")
df.select("STATION","NAME","YEAR","MONTH","AVG_VOLATILITY_SCORE","AVG_TEMP_VOLATILITY","AVG_PRCP_VOLATILITY","AVG_WIND_VOLATILITY","HIGH_VOLATILITY_RATE").orderBy(desc("AVG_VOLATILITY_SCORE")).show(10,truncate=False)

print("\nAverage volatility drivers by month:")
df.groupBy("MONTH").agg(avg("AVG_VOLATILITY_SCORE").alias("AVG_VOLATILITY_SCORE"),avg("AVG_TEMP_VOLATILITY").alias("AVG_TEMP_VOLATILITY"),avg("AVG_PRCP_VOLATILITY").alias("AVG_PRCP_VOLATILITY"),avg("AVG_WIND_VOLATILITY").alias("AVG_WIND_VOLATILITY")).orderBy(desc("AVG_VOLATILITY_SCORE")).show(12,truncate=False)

print("\nAverage volatility drivers by station:")
df.groupBy("STATION","NAME").agg(avg("AVG_VOLATILITY_SCORE").alias("AVG_VOLATILITY_SCORE"),avg("AVG_TEMP_VOLATILITY").alias("AVG_TEMP_VOLATILITY"),avg("AVG_PRCP_VOLATILITY").alias("AVG_PRCP_VOLATILITY"),avg("AVG_WIND_VOLATILITY").alias("AVG_WIND_VOLATILITY")).orderBy(desc("AVG_VOLATILITY_SCORE")).show(10,truncate=False)

print("\nHighest temperature volatility:")
df.orderBy(desc("AVG_TEMP_VOLATILITY")).select("STATION","NAME","YEAR","MONTH","AVG_TEMP_VOLATILITY","AVG_VOLATILITY_SCORE").show(10,truncate=False)

print("\nHighest precipitation volatility:")
df.orderBy(desc("AVG_PRCP_VOLATILITY")).select("STATION","NAME","YEAR","MONTH","AVG_PRCP_VOLATILITY","AVG_VOLATILITY_SCORE").show(10,truncate=False)

print("\nHighest wind volatility:")
df.orderBy(desc("AVG_WIND_VOLATILITY")).select("STATION","NAME","YEAR","MONTH","AVG_WIND_VOLATILITY","AVG_VOLATILITY_SCORE").show(10,truncate=False)

spark.stop()
print("SUCCESS: Volatility driver analysis completed.")