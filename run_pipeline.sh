#!/bin/bash
set -e

echo "=== GLOBAL CLIMATE VOLATILITY PIPELINE ==="

echo "[1/7] Cleaning raw GSOD data..."
python spark/clean_gsod.py

echo "[2/7] Feature engineering..."
python spark/feature_engineering.py

echo "[3/7] Calculating climate volatility..."
python spark/volatility.py

echo "[4/7] Station aggregation..."
python spark/aggregation.py

echo "[5/7] Monthly aggregation..."
python spark/monthly_aggregation.py

echo "[6/7] Loading monthly data into PostgreSQL..."
python ingestion/load_monthly_postgres.py

echo "[7/7] Running analyses..."
python spark/analysis.py
python spark/correlation_analysis.py
python spark/driver_analysis.py

echo "=== PIPELINE COMPLETED SUCCESSFULLY ==="