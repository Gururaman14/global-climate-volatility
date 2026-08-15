import os, sys
from pathlib import Path
import pandas as pd
from scipy.stats import t as t_dist
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET, YEAR

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"

spark = (
    SparkSession.builder
    .appName("WeatherVariabilityComponentRobustness")
    .master("local[2]")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

path = f"s3a://{MINIO_BUCKET}/processed/volatility/{YEAR}/"
df = spark.read.parquet(path)

components = [
    "TEMP_ANOMALY_Z_GLOBAL",
    "TEMP_VOLATILITY_Z_GLOBAL",
    "PRCP_VOLATILITY_Z_GLOBAL",
    "WIND_VOLATILITY_Z_GLOBAL"
]
score = "CLIMATE_VOLATILITY_SCORE"

print("Daily records:", df.count())

df = df.withColumn(
    "SCORE_COMPONENT_COUNT",
    sum(col(c).isNotNull().cast("int") for c in components)
)

print("\n=== COMPONENT AVAILABILITY ===")
df.groupBy("SCORE_COMPONENT_COUNT").count().orderBy("SCORE_COMPONENT_COUNT").show()

full = df.select(*components, score).toPandas()
four_df = df.filter(col("SCORE_COMPONENT_COUNT") == 4)
four = four_df.select(*components, score).toPandas()

print("Four-component observations:", len(four))

def corr(x, y):
    d = pd.concat([x, y], axis=1).dropna()
    n = len(d)
    if n < 4:
        return None
    r = float(d.iloc[:, 0].corr(d.iloc[:, 1]))
    if abs(r) >= 0.999999:
        p = 0.0
    else:
        t = r * ((n - 2) / (1 - r ** 2)) ** 0.5
        p = 2 * t_dist.sf(abs(t), n - 2)
    return r, p, n

print("\n=== FULL DATA LOO CORRELATIONS ===")
for c in components:
    result = corr(full[c], full[[x for x in components if x != c]].mean(axis=1))
    print(f"{c}: r={result[0]:.4f}, p={result[1]:.6f}, n={result[2]}")

print("\n=== FOUR-COMPONENT LOO CORRELATIONS ===")
results = []

for c in components:
    result = corr(four[c], four[[x for x in components if x != c]].mean(axis=1))
    print(f"{c}: r={result[0]:.4f}, p={result[1]:.6f}, n={result[2]}")
    results.append({"Component": c, "r": result[0], "p": result[1], "n": result[2]})

out = ROOT / "data" / "analysis"
out.mkdir(parents=True, exist_ok=True)
pd.DataFrame(results).to_csv(out / "component_count_robustness.csv", index=False)

print("\nSUCCESS: Component-count robustness analysis completed.")
spark.stop()