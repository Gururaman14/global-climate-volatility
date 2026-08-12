# Global Climate Volatility

Distributed climate data engineering pipeline using Apache Spark, MinIO, PostgreSQL, and NOAA GSOD data.

## Overview

This project processes climate observations to identify and analyse climate volatility across weather stations. The pipeline performs data cleaning, feature engineering, volatility scoring, aggregation, statistical analysis, and dashboard reporting.

## Architecture

NOAA GSOD
→ Spark Cleaning
→ Feature Engineering
→ Climate Volatility
→ Station/Monthly Aggregation
→ MinIO / Parquet
→ PostgreSQL
→ Power BI

## Pipeline

1. Clean GSOD climate data using PySpark.
2. Generate temperature, precipitation, and wind volatility features.
3. Calculate a climate volatility score and high-volatility classification.
4. Aggregate results by station and month.
5. Store processed datasets as Parquet in MinIO.
6. Load monthly results into PostgreSQL.
7. Perform volatility, driver, and correlation analysis.
8. Visualise results using Power BI.

## Results

- 26,545 daily volatility records processed.
- 941 station-month records generated.
- 856 station-month records contained valid high-volatility rates.
- 979 high-volatility days identified.
- Temperature volatility correlation with the climate volatility score: 0.5692.
- Precipitation volatility correlation: 0.5216.
- Temperature anomaly correlation: 0.3387.
- Wind volatility correlation: 0.2502.

January and December showed the highest overall high-volatility rates in the 2024 analysis.

## Dashboard

The Power BI dashboard provides:

- Climate volatility KPIs.
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
- Git

## Running the Pipeline

Make sure the required services are running and the Python environment is activated.

```bash
chmod +x run_pipeline.sh
./run_pipeline.sh

c
C
Q

exit


