# dbt Project

This dbt project implements the transformation path for the current TLC
pipeline:

`bronze -> silver -> gold -> ops/quarantine`

## Layer Overview

- `bronze`: raw monthly landing files registered as Delta-backed datasets
- `silver`: canonical typed models with time enrichments and DQ flags
- `gold`: analytics-ready fact and aggregate models for trend analysis
- `ops`: partition-level DQ metrics derived from Silver
- `quarantine`: row-level exception datasets for high-signal anomalies

Reference data path:

- `taxi_zone_lookup_raw`
- `dim_taxi_zones_v1`

## Service-specific Build Path

Yellow:

- `yellow_tripdata_raw`
- `yellow_tripdata_silver`
- `yellow_tripdata_dq_metrics_v1`
- `yellow_trips_v1`
- `yellow_daily_metrics_v1`
- `yellow_hourly_zone_metrics_v1`

Green:

- `green_tripdata_raw`
- `green_tripdata_silver`
- `green_tripdata_dq_metrics_v1`
- `green_trips_v1`
- `green_daily_metrics_v1`
- `green_hourly_zone_metrics_v1`

Unified Gold:

- `trips_v1`
- `daily_metrics_v1`
- `hourly_zone_metrics_v1`

## Typical Dev Commands

```bash
dbt build --project-dir dbt/nyc_taxi_lakehouse \
  --profiles-dir dbt/profiles \
  --select yellow_trips_v1+
```

```bash
dbt build --project-dir dbt/nyc_taxi_lakehouse \
  --profiles-dir dbt/profiles \
  --select green_trips_v1+
```

The Airflow DAG runs Bronze, Silver, and Gold in separate stages so stage
success can be tracked in `ops/pipeline_state/`.
The DAG also stages official TLC taxi zone reference data into Landing before
dbt consumes it through Bronze and Silver.
