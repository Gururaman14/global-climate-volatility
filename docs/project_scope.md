# Project Scope

## Final Implemented Scope

The final implementation analyses NOAA Global Surface Summary of the Day
(GSOD) observations for 2024 using a reproducible sample of weather stations.

The delivered pipeline processes 79 weather stations and produces daily and
station-month meteorological variability measures through an automated
workflow using Apache Spark, MinIO, PostgreSQL, Parquet, Python, Jupyter and
Power BI.

The original project plan considered a broader multi-year analysis covering
2014–2024, a larger station population, multiple geographic regions, and an
additional NOAA Storm Events dataset. These elements were not implemented in
the final submission and are therefore treated as potential future
extensions rather than completed project results.

The final implementation is intentionally scoped to the 2024 observation
period and the available station sample so that the complete distributed
ingestion, cleaning, feature engineering, volatility scoring, aggregation,
statistical analysis, database loading, notebook analysis and dashboard
workflow could be implemented and validated within the module timeframe.

The project therefore focuses on within-year meteorological variability
rather than long-term climatological change.

## Research Question

How can a distributed Apache Spark pipeline be used to quantify and compare
within-year meteorological variability across weather stations in the 2024
NOAA GSOD dataset, and what seasonal, spatial, and component-level
associations can be identified from the resulting variability scores?

## Supporting Questions

1. Which stations and station-months exhibit the highest relative
   meteorological variability during 2024?

2. How are temperature, precipitation and wind variability associated with
   the composite variability score after accounting for part-whole
   correlation?

## Objectives

1. Design and implement an automated distributed Apache Spark pipeline for
   NOAA GSOD weather observations.

2. Clean and transform daily meteorological observations and derive
   standardised measures of temperature, precipitation, wind and within-year
   temperature deviation.

3. Construct a composite meteorological variability score and compare
   variability across weather stations and months.

4. Analyse seasonal, spatial and component-level patterns in the resulting
   variability measures.

5. Evaluate the robustness and interpretation of the composite score,
   including part-whole correlation and selected methodological sensitivity
   checks.

## Final Dataset Scope

- Source: NOAA Global Surface Summary of the Day (GSOD)
- Observation year: 2024
- Weather stations: 79
- Daily observations: 26,545
- Station-month records: 941
- Qualified station-month records: 903
- Minimum monthly observation threshold: 20 days

## Out of Scope

The final implementation does not claim to provide:

- long-term climatological trends
- climate-change detection
- causal identification of weather variability drivers
- globally representative station coverage
- multi-year climatological normals
- external validation against a separate event dataset

These areas are identified as limitations or future extensions.

## Composite Score Design

The composite meteorological variability score uses equal weighting across
the four standardised components: temperature deviation, temperature
variability, precipitation variability and wind variability.

Equal weighting was selected as a transparent baseline because the study does
not establish a theoretical or empirical basis for assigning greater weight
to any individual component.

When one or more component values are unavailable, the score is calculated
from the available components. This means that the effective number of
components can vary between observations. This is treated as a limitation of
the current implementation and is examined through a complete-case
component-robustness analysis.

The equal-weight composite should therefore be interpreted as a descriptive
index for comparing meteorological variability within the study dataset,
rather than as a validated physical measure of overall weather risk.


## Station Sampling

The final dataset contains 79 weather stations from the selected NOAA GSOD
station list. The stations were selected using a fixed metadata-file ordering
and download limit during the implementation stage.

This sampling approach is therefore a non-random convenience sample and
should not be interpreted as geographically representative of global weather
conditions.

The analysis is consequently framed as a demonstration of distributed
meteorological variability analysis across the selected station sample rather
than as a globally representative climatological study.