# Ingestion

This folder contains the current ingestion implementation for one NYC TLC
`dataset-month`.

## Active Path

The current workflow stops at Landing. Bronze is intentionally deferred until
the raw storage component is defined in a way that stays aligned with the
target architecture.

Use:

```bash
python ingestion/ingest_tlc_to_landing.py \
  --service green \
  --year 2020 \
  --month 1 \
  --landing-root s3://nyc-data-platform-dev/dev
```

The script downloads one source parquet file and persists it into a
deterministic Landing location under the root you provide. The root can be a
local filesystem path such as `/dbfs/tmp/nyc_taxi_lakehouse` or an S3 prefix
such as `s3://nyc-data-platform-dev/dev`. For example:

```text
s3://nyc-data-platform-dev/dev/landing/green/year=2020/month=01/green_tripdata_2020-01.parquet
s3://nyc-data-platform-dev/dev/landing/yellow/year=2020/month=01/yellow_tripdata_2020-01.parquet
```

## What It Does

- reads the source descriptor from `ingestion/sources/`
- resolves the deterministic Landing object URI for the selected dataset-month
- downloads the TLC parquet file directly into that Landing destination
- keeps Landing separate from the future Bronze loading step
