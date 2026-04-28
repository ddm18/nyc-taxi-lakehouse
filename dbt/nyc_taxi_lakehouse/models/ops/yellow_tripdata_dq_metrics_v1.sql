{{
  config(
    unique_key="dataset_month",
    on_schema_change="sync_all_columns"
  )
}}

select
  cast(service_type as string) as service_type,
  cast(source_year as int) as metric_year,
  cast(source_month as int) as metric_month,
  cast(dataset_month as string) as dataset_month,
  cast(count(*) as bigint) as row_count,
  cast(avg(case when pickup_ts is null then 1.0 else 0.0 end) as double) as null_pickup_ts_rate,
  cast(avg(case when pu_location_id is null then 1.0 else 0.0 end) as double) as null_pu_location_rate,
  cast(avg(case when do_location_id is null then 1.0 else 0.0 end) as double) as null_do_location_rate,
  cast(avg(case when is_valid_duration then 0.0 else 1.0 end) as double) as invalid_duration_rate,
  cast(avg(case when is_valid_distance then 0.0 else 1.0 end) as double) as invalid_distance_rate,
  cast(avg(case when is_valid_total_amount then 0.0 else 1.0 end) as double) as invalid_total_amount_rate,
  cast(avg(case when is_outlier_trip_duration then 1.0 else 0.0 end) as double) as outlier_trip_duration_rate,
  cast(avg(case when is_outlier_trip_distance then 1.0 else 0.0 end) as double) as outlier_trip_distance_rate,
  cast(avg(case when is_outlier_total_amount then 1.0 else 0.0 end) as double) as outlier_total_amount_rate,
  cast(avg(case when coalesce(trip_distance, 0.0) = 0 then 1.0 else 0.0 end) as double) as zero_distance_rate,
  cast(avg(case when coalesce(tip_amount, 0.0) = 0 then 1.0 else 0.0 end) as double) as zero_tip_rate,
  cast(avg(case when coalesce(fare_amount, 0.0) < 0 or coalesce(total_amount, 0.0) < 0 then 1.0 else 0.0 end) as double) as negative_amount_rate,
  cast(max(ingested_at) as timestamp) as last_ingested_at
from {{ ref('yellow_tripdata_silver') }}
where source_year = {{ var('year') | int }} and source_month = {{ var('month') | int }}
group by service_type, source_year, source_month, dataset_month
