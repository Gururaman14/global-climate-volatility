from pyspark.sql import SparkSession
import psycopg2

spark=(SparkSession.builder.appName("GlobalClimateVolatility-PostgreSQL").master("local[2]").config("spark.driver.host","127.0.0.1").config("spark.driver.bindAddress","127.0.0.1").config("spark.hadoop.fs.s3a.endpoint","http://127.0.0.1:9000").config("spark.hadoop.fs.s3a.access.key","minioadmin").config("spark.hadoop.fs.s3a.secret.key","minioadmin").config("spark.hadoop.fs.s3a.path.style.access","true").config("spark.hadoop.fs.s3a.connection.ssl.enabled","false").config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

input_path="s3a://climate-data/processed/monthly/2024/"
print("Reading monthly data from:",input_path)

df=spark.read.parquet(input_path)
print("Monthly records:",df.count())

rows=df.collect()

conn=psycopg2.connect(host="localhost",port=5432,database="climate_db",user="climate_user",password="climate_pass")
cursor=conn.cursor()
cursor.execute("TRUNCATE TABLE climate.monthly_volatility")

insert_sql="""INSERT INTO climate.monthly_volatility (station,name,latitude,longitude,year,month,total_days,high_volatility_days,high_volatility_rate,avg_volatility_score,max_volatility_score,avg_temp_anomaly,avg_temp_volatility,avg_prcp_volatility,avg_wind_volatility) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

for row in rows:
    cursor.execute(insert_sql,(row.STATION,row.NAME,row.LATITUDE,row.LONGITUDE,row.YEAR,row.MONTH,row.TOTAL_DAYS,row.HIGH_VOLATILITY_DAYS,row.HIGH_VOLATILITY_RATE,row.AVG_VOLATILITY_SCORE,row.MAX_VOLATILITY_SCORE,row.AVG_TEMP_ANOMALY,row.AVG_TEMP_VOLATILITY,row.AVG_PRCP_VOLATILITY,row.AVG_WIND_VOLATILITY))

conn.commit()
cursor.execute("SELECT COUNT(*) FROM climate.monthly_volatility")
print("PostgreSQL records:",cursor.fetchone()[0])

cursor.close()
conn.close()
spark.stop()
print("SUCCESS: Monthly data loaded into PostgreSQL.")