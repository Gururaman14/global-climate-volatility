import os
import sys
import math
from pathlib import Path

import pandas as pd
from scipy.stats import t as t_dist

from pyspark.sql import SparkSession

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
    .appName("WeatherVariabilityCorrelationAnalysis")
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

INPUT_PATH = f"s3a://{MINIO_BUCKET}/processed/volatility/{YEAR}/"

print("Reading volatility data from:", INPUT_PATH)

df = spark.read.parquet(INPUT_PATH)

print("Volatility records:", df.count())

components = [
    "TEMP_ANOMALY_Z_GLOBAL",
    "TEMP_VOLATILITY_Z_GLOBAL",
    "PRCP_VOLATILITY_Z_GLOBAL",
    "WIND_VOLATILITY_Z_GLOBAL",
]

score_col = "CLIMATE_VOLATILITY_SCORE"

required = components + [score_col]
missing = [c for c in required if c not in df.columns]

if missing:
    raise ValueError(
        "Required columns are missing: " + ", ".join(missing)
    )


def fisher_ci(r, n):
    if n <= 3 or pd.isna(r):
        return float("nan"), float("nan")

    r = max(min(float(r), 0.999999), -0.999999)

    z = 0.5 * math.log((1 + r) / (1 - r))
    se = 1 / math.sqrt(n - 3)

    low_z = z - 1.96 * se
    high_z = z + 1.96 * se

    low = (math.exp(2 * low_z) - 1) / (math.exp(2 * low_z) + 1)
    high = (math.exp(2 * high_z) - 1) / (math.exp(2 * high_z) + 1)

    return low, high


def correlation_stats(pdf, x_col, y_col):
    temp = pdf[[x_col, y_col]].dropna()
    n = len(temp)

    if n < 4:
        return None

    r = temp[x_col].corr(temp[y_col])

    if pd.isna(r):
        return None

    r = float(r)

    if abs(r) >= 0.999999:
        p_value = 0.0
    else:
        t_value = r * math.sqrt((n - 2) / (1 - r ** 2))
        p_value = 2 * t_dist.sf(abs(t_value), df=n - 2)

    ci_low, ci_high = fisher_ci(r, n)

    return {
        "n": n,
        "r": r,
        "p": float(p_value),
        "ci_low": ci_low,
        "ci_high": ci_high,
    }


def format_p(p):
    if pd.isna(p):
        return "NA"
    if p < 0.001:
        return "<0.001"
    return f"{p:.4f}"


pdf = df.select(*required).toPandas()

results = []

print("\n=== ORIGINAL COMPONENT-TO-COMPOSITE CORRELATIONS ===")

for component in components:
    result = correlation_stats(pdf, component, score_col)

    if result:
        results.append({
            "Analysis": "Original",
            "Component": component,
            **result
        })


print("\n=== LEAVE-ONE-COMPONENT-OUT CORRELATIONS ===")

for component in components:
    remaining = [c for c in components if c != component]
    loo_col = f"LOO_{component}"

    pdf[loo_col] = pdf[remaining].mean(axis=1, skipna=True)

    result = correlation_stats(pdf, component, loo_col)

    if result:
        results.append({
            "Analysis": "LOO",
            "Component": component,
            **result
        })


# Bonferroni correction across the 8 main component-to-score tests
main_results = [r for r in results if r["Analysis"] in ("Original", "LOO")]
m = len(main_results)

for result in main_results:
    result["p_bonferroni"] = min(result["p"] * m, 1.0)


print("\n=== MAIN CORRELATION RESULTS ===")

for result in main_results:
    print(
        f"{result['Analysis']} | "
        f"{result['Component']}: "
        f"r={result['r']:.4f}, "
        f"p={format_p(result['p'])}, "
        f"Bonferroni p={format_p(result['p_bonferroni'])}, "
        f"95% CI=[{result['ci_low']:.4f}, {result['ci_high']:.4f}], "
        f"n={result['n']}"
    )


print("\n=== COMPONENT-TO-COMPONENT CORRELATIONS ===")

component_results = []

for i in range(len(components)):
    for j in range(i + 1, len(components)):
        x = components[i]
        y = components[j]

        result = correlation_stats(pdf, x, y)

        if result:
            component_results.append({
                "Component 1": x,
                "Component 2": y,
                **result
            })

            print(
                f"{x} vs {y}: "
                f"r={result['r']:.4f}, "
                f"p={format_p(result['p'])}, "
                f"95% CI=[{result['ci_low']:.4f}, "
                f"{result['ci_high']:.4f}], "
                f"n={result['n']}"
            )


# Save the main results as a CSV for notebook/report use
results_df = pd.DataFrame(main_results)

output_dir = ROOT / "data" / "analysis"
output_dir.mkdir(parents=True, exist_ok=True)

output_file = output_dir / "correlation_results.csv"

results_df.to_csv(output_file, index=False)

print("\nSaved correlation results to:")
print(output_file)

print("\n=== INTERPRETATION ===")
print(
    "Original component-to-composite correlations can be inflated "
    "by part-whole correlation because each component contributes "
    "directly to the composite score."
)

print(
    "Leave-one-component-out correlations reduce this direct "
    "self-correlation by comparing each component with the mean "
    "of the remaining components."
)

print(
    "Bonferroni-adjusted p-values account for multiple testing "
    "across the eight original and leave-one-component-out tests."
)

print(
    "These results describe statistical associations and should "
    "not be interpreted as causal drivers."
)

print("\nSUCCESS: Statistical correlation analysis completed.")

spark.stop()