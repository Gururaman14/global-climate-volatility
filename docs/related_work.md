# Related Work

## Distributed Climate and Weather Data Processing

Large-scale meteorological datasets require scalable data-processing
frameworks because observations are collected across many stations and
time periods. Distributed processing frameworks such as Apache Spark can
support parallel data cleaning, transformation, feature engineering and
aggregation. This project applies Spark to NOAA GSOD observations to
demonstrate a reproducible distributed weather-variability pipeline.

## Meteorological Variability and Anomaly Detection

Previous meteorological studies commonly use anomalies, rolling statistics,
standardisation and variability measures to identify unusual weather
conditions. In this project, station-level temperature anomalies and
rolling variability measures are derived from daily observations. These
features are standardised before being combined into a composite
variability score.

## Composite Climate and Weather Indicators

Composite indicators can combine multiple dimensions of environmental
variability into a single interpretable measure. However, composite scores
introduce methodological concerns because the components contribute
directly to the resulting score. Therefore, simple component-to-composite
correlations can produce part-whole correlation.

This project addresses this issue using leave-one-component-out
correlations. Each component is compared against the mean of the remaining
components rather than directly against a score containing that component.

## Research Gap

Existing approaches commonly focus on individual meteorological variables,
anomaly detection, variability measurement, or scalable data processing.
The contribution of this project is the integration of these elements into
a distributed Spark pipeline that produces station-level and
station-month-level weather-variability measures.

The project additionally evaluates the robustness of the composite score
through threshold sensitivity, minimum-observation sensitivity and
component-availability analysis.

## Position of the Present Study

The project does not claim to measure long-term climate change or establish
causal climate drivers. Instead, it provides a within-year statistical
framework for quantifying and comparing meteorological variability across
the selected 2024 NOAA GSOD stations.