from pyspark.sql import SparkSession
from pyspark.sql.functions import desc,avg,sum,count

INPUT_PATH="s3a://climate-data/processed/monthly/2024/"

spark=(SparkSession.builder.appName("GlobalClimateVolatility-Analysis").master("local[2]")
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

print("\nTop stations by high-volatility rate:")
df.orderBy(desc("HIGH_VOLATILITY_RATE")).select("STATION","NAME","YEAR","MONTH","TOTAL_DAYS","HIGH_VOLATILITY_DAYS","HIGH_VOLATILITY_RATE").show(10,truncate=False)

print("\nTop stations by average volatility score:")
df.groupBy("STATION","NAME").agg(avg("AVG_VOLATILITY_SCORE").alias("AVG_SCORE"),sum("HIGH_VOLATILITY_DAYS").alias("HIGH_VOLATILITY_DAYS"),count("*").alias("MONTHS")).orderBy(desc("AVG_SCORE")).show(10,truncate=False)

print("\nMonthly volatility summary:")
df.groupBy("MONTH").agg(avg("HIGH_VOLATILITY_RATE").alias("AVG_HIGH_VOLATILITY_RATE"),avg("AVG_VOLATILITY_SCORE").alias("AVG_VOLATILITY_SCORE"),sum("HIGH_VOLATILITY_DAYS").alias("HIGH_VOLATILITY_DAYS")).orderBy(desc("AVG_HIGH_VOLATILITY_RATE")).show(12,truncate=False)

print("\nHighest volatility months:")
df.orderBy(desc("AVG_VOLATILITY_SCORE")).select("STATION","NAME","YEAR","MONTH","AVG_VOLATILITY_SCORE","MAX_VOLATILITY_SCORE","HIGH_VOLATILITY_DAYS","HIGH_VOLATILITY_RATE").show(10,truncate=False)

spark.stop()
print("SUCCESS: Climate volatility analysis completed.")