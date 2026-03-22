# dbt Project

This dbt project is prepared for the future Bronze -> Silver path.

## Target flow

- `source('bronze', 'yellow_tripdata_raw')`
- `source('bronze', 'green_tripdata_raw')`
- `stg_yellow_tripdata`
- `stg_green_tripdata`
- `yellow_trips_v1`
- `green_trips_v1`

## Current repo state

- ingestion currently stops at Landing
- Bronze loading/registration is still pending
- the dbt models below remain ready for when Bronze exists:
  - `models/staging/_sources.yml`
  - `stg_yellow_tripdata`
  - `stg_green_tripdata`
  - `yellow_trips_v1`
  - `green_trips_v1`

## Typical dev commands

```bash
.venv/bin/dbt parse --project-dir dbt/nyc_taxi_lakehouse
```

Only `dbt parse` is expected to work end-to-end right now. Before `dbt run`,
implement Bronze and register the raw tables as:

- `bronze.yellow_tripdata_raw`
- `bronze.green_tripdata_raw`
