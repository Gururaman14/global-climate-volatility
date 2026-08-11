from pyspark.sql import SparkSession

INPUT_PATH="s3a://climate-data/processed/volatility/2024/"

spark=(SparkSession.builder.appName("GlobalClimateVolatility-CorrelationAnalysis").master("local[2]")
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
print("Volatility records:",df.count())

components=["TEMP_ANOMALY_Z","TEMP_VOLATILITY_Z","PRCP_VOLATILITY_Z","WIND_VOLATILITY_Z"]

print("\nCorrelation with climate volatility score:")
for component in components:
    correlation=df.stat.corr(component,"CLIMATE_VOLATILITY_SCORE")
    print(f"{component}: {correlation:.4f}")

print("\nComponent correlations:")
for i in range(len(components)):
    for j in range(i+1,len(components)):
        col1=components[i]
        col2=components[j]
        correlation=df.stat.corr(col1,col2)
        print(f"{col1} vs {col2}: {correlation:.4f}")

spark.stop()
print("SUCCESS: Correlation analysis completed.")