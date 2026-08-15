#!/bin/bash
set -e

echo "=== GLOBAL CLIMATE VOLATILITY PIPELINE ==="

echo "[1/10] Selecting stations..."
python ingestion/select_stations.py

echo "[2/10] Downloading GSOD data..."
python ingestion/fetch_gsod.py

echo "[3/10] Uploading raw data to MinIO..."
python ingestion/upload_raw_minio.py

echo "[4/10] Cleaning raw GSOD data..."
python spark/clean_gsod.py

echo "[5/10] Feature engineering..."
python spark/feature_engineering.py

echo "[6/10] Calculating climate volatility..."
python spark/volatility.py

echo "[7/10] Station aggregation..."
python spark/aggregation.py

echo "[8/10] Monthly aggregation..."
python spark/monthly_aggregation.py

echo "[9/10] Loading monthly data into PostgreSQL..."
spark-submit --packages org.postgresql:postgresql:42.7.7 ingestion/load_monthly_postgres.py

echo "[10/10] Running analyses..."
python spark/analysis.py
python spark/correlation_analysis.py
python spark/driver_analysis.py

echo "=== PIPELINE COMPLETED SUCCESSFULLY ==="