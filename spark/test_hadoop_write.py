import os
from pathlib import Path
from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed" / "hadoop_test"

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"

spark = (SparkSession.builder.appName("HadoopWriteTest").master("local[1]").config("spark.driver.host", "127.0.0.1").config("spark.driver.bindAddress", "127.0.0.1").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

data = [(1,"Test"),(2,"Spark"),(3,"Hadoop")]
df = spark.createDataFrame(data, ["id","name"])

OUTPUT_DIR.parent.mkdir(parents=True, exist_ok=True)

print("Writing test Parquet to:")
print(OUTPUT_DIR)

df.write.mode("overwrite").parquet(str(OUTPUT_DIR))

print("SUCCESS: Parquet write completed.")

spark.stop()