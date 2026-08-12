# Global Climate Volatility

Distributed climate data engineering pipeline using Apache Spark, MinIO, PostgreSQL, and NOAA GSOD data.

## Overview

This project processes 2024 NOAA GSOD observations from weather stations and develops a statistical weather-variability framework using Spark. The pipeline performs data cleaning, feature engineering, station-level standardisation, volatility scoring, aggregation, statistical analysis, PostgreSQL loading, and Power BI reporting.

## Architecture

NOAA GSOD
→ Station Selection
→ Local Raw Data
→ MinIO
→ Spark Cleaning
→ Feature Engineering
→ Volatility Scoring
→ Station Aggregation
→ Monthly Aggregation
→ PostgreSQL
→ Analysis
→ Power BI

## Pipeline

1. Select active NOAA weather stations.
2. Download 2024 GSOD station data.
3. Upload raw observations to MinIO.
4. Clean GSOD sentinel values using PySpark.
5. Generate temporal, anomaly, and rolling-volatility features.
6. Calculate station-level climate-volatility scores.
7. Aggregate daily results by station.
8. Aggregate results by station and month.
9. Load monthly results into PostgreSQL.
10. Perform volatility, correlation, and component analysis.
11. Analyse the results using Jupyter.
12. Visualise the results using Power BI.

## Final Results

- 26,545 daily observations processed.
- 79 weather stations analysed.
- 941 station-month records generated.
- 903 station-month records met the minimum 20-day observation threshold.
- 1,537 high-volatility days identified.
- January recorded the highest average high-volatility rate in the qualified 2024 analysis.
- Correlation analysis includes leave-one-component-out testing to reduce part-whole bias when assessing relationships between volatility components and the composite score.

## Corrected Leave-One-Component-Out Correlations

- Temperature anomaly: 0.0261
- Temperature volatility: 0.1466
- Precipitation volatility: -0.0538
- Wind volatility: -0.2666

These values are used for interpretation instead of treating component-to-composite correlations as independent driver evidence.

## Dashboard

The Power BI dashboard provides:

- Climate-volatility KPIs.
- Monthly volatility trends.
- Top high-volatility stations.
- Geographic station distribution.
- Interactive month filtering.

Dashboard file:

`dashboard/Global Climate Volatility Dashboard.pbix`

## Technologies

- Python
- PySpark / Apache Spark
- MinIO / S3
- PostgreSQL
- Docker
- Parquet
- Power BI
- Jupyter
- Git

## Project Structure

```text
global-climate-volatility/
├── dashboard/
├── data/
│   └── metadata/
├── docs/
├── ingestion/
├── notebooks/
├── spark/
├── config.py
├── docker-compose.yml
├── requirements.txt
├── run_pipeline.sh
├── README.md
└── readme.txt
```