# Ingestion

This folder contains the landing ingestion implementation for one NYC TLC
`dataset-month`, plus the shared configuration used by the Airflow DAG.

## Active Path

The ingestion entrypoint downloads one monthly TLC parquet into a deterministic
Landing path. The operational DAG then continues with:

`Landing -> Bronze -> Silver -> Gold`

The default lakehouse convention is:

`s3://<lakehouse-bucket>/<env>/`

For local runtime, the repo defaults to the shared `test` lakehouse namespace:

`s3://nyc-data-platform-test/test`

## Standalone CLI

```bash
python3 ingestion/ingest_tlc_to_landing.py \
  --service green \
  --year 2020 \
  --month 1 \
  --landing-root s3://nyc-data-platform-test/test
```

Example deterministic paths:

```text
s3://nyc-data-platform-test/test/landing/green/year=2020/month=01/green_tripdata_2020-01.parquet
s3://nyc-data-platform-test/test/landing/yellow/year=2020/month=01/yellow_tripdata_2020-01.parquet
```

## Runtime Configuration

The ingestion and DAG runtime resolve the lakehouse root with this precedence:

1. `LAKEHOUSE_ROOT`
2. `LAKEHOUSE_BUCKET_URI` + `LAKEHOUSE_ENV`
3. default `s3://nyc-data-platform-test/test`

The transformation version is resolved independently through:

1. `TRANSFORMATION_VERSION`
2. default `local-dev`

## What It Does

- reads the source descriptor from `ingestion/sources/`
- resolves the deterministic Landing object URI for the selected dataset-month
- downloads the TLC parquet file directly into that Landing destination
- records current and historical source metadata under `ops/source_metadata/`
- leaves stage-level operational markers under `ops/pipeline_state/`
- can leave reprocessing memory objects under `ops/reprocess_queue/`
