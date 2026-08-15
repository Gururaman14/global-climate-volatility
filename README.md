# Global Climate Volatility

Distributed climate data engineering pipeline using Apache Spark, MinIO, PostgreSQL, and NOAA GSOD data.

## Overview

This project processes 2024 NOAA GSOD observations from weather stations and develops a statistical weather-variability framework using Spark. The pipeline performs data cleaning, feature engineering, station-level standardisation, volatility scoring, aggregation, statistical analysis, PostgreSQL loading, and Power BI reporting.

## Research Question

How can a distributed Apache Spark pipeline be used to quantify and compare
within-year meteorological variability across weather stations in the 2024
NOAA GSOD dataset, and what seasonal, spatial, and component-level
associations can be identified from the resulting variability scores?

## Objectives

1. Design and implement an automated distributed Apache Spark pipeline for
   NOAA GSOD weather observations.

2. Clean and transform daily observations and derive standardised
   meteorological variability features.

3. Construct and compare composite meteorological variability scores across
   stations and months.

4. Analyse seasonal, spatial and component-level patterns in the resulting
   variability measures.

5. Evaluate the robustness and interpretation of the composite score,
   including part-whole correlation and methodological sensitivity.

## Related Work

Research on meteorological and climate variability commonly uses
standardised indices, rolling variability measures, and component-specific
indicators to identify unusual or extreme conditions. Established approaches
include precipitation indices such as the Standardized Precipitation Index
(SPI), temperature and precipitation extremes represented through ETCCDI
indices, and statistical standardisation methods based on historical or
reference distributions.

This project differs from a conventional single-variable index by combining
standardised temperature, precipitation and wind variability measures into a
single descriptive composite score and processing the observations through a
distributed Apache Spark pipeline.

The composite score is not intended to replace established climate indices or
to provide a validated physical measure of climate risk. Instead, it provides
a reproducible analytical framework for comparing within-year meteorological
variability across the selected 2024 station sample.

The leave-one-component-out analysis further examines the relationship between
individual components and the remaining components after reducing direct
part-whole correlation. These results are interpreted as inter-component
associations rather than causal drivers.

The methodology is therefore positioned as a data-engineering and statistical
analytics framework rather than a replacement for established climatological
indices.

### Literature Gap

The implemented framework focuses on distributed processing and descriptive
multi-component variability analysis. It does not claim that the composite
score is an established climatological index. Comparison with established
indices and validation against an independent event or impact dataset remain
future work.

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
5. Generate temporal, within-year temperature deviation, and rolling-volatility features.
6. Calculate station-level meteorological variability scores.
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
- 1,539 high-volatility days identified.
- January recorded the highest average high-volatility rate in the qualified 2024 analysis.
- Correlation analysis includes leave-one-component-out testing to reduce part-whole bias when assessing relationships between volatility components and the composite score.

## Corrected Leave-One-Component-Out Correlations

- Temperature anomaly: 0.0261
- Temperature volatility: 0.1466
- Precipitation volatility: -0.0538
- Wind volatility: -0.2666

These values are used for interpretation rather than treating component-to-composite correlations as independent evidence of component association.

## Limitations

- The 79 analysed stations constitute a non-random convenience sample and are
  not intended to provide globally representative estimates.
- The analysis covers a single year, 2024, and therefore measures within-year
  meteorological variability rather than long-term climatological change.
- The composite score uses equal weighting across the available components.
- Some observations contain fewer than four available components, which may
  affect the effective scale of the composite score.

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