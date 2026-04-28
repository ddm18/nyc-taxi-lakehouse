with raw as (
  select
    cast('yellow' as string) as service_type,
    cast(source_year as int) as source_year,
    cast(source_month as int) as source_month,
    cast(dataset_month as string) as dataset_month,
    cast(source_file as string) as source_file,
    cast(ingested_at as timestamp) as ingested_at,
    cast(VendorID as bigint) as vendor_id,
    cast(tpep_pickup_datetime as timestamp) as pickup_ts,
    cast(tpep_dropoff_datetime as timestamp) as dropoff_ts,
    cast(passenger_count as double) as passenger_count,
    cast(trip_distance as double) as trip_distance,
    cast(RatecodeID as double) as rate_code_id,
    cast(store_and_fwd_flag as string) as store_and_fwd_flag,
    cast(PULocationID as bigint) as pu_location_id,
    cast(DOLocationID as bigint) as do_location_id,
    cast(payment_type as bigint) as payment_type,
    cast(fare_amount as double) as fare_amount,
    cast(extra as double) as extra,
    cast(mta_tax as double) as mta_tax,
    cast(tip_amount as double) as tip_amount,
    cast(tolls_amount as double) as tolls_amount,
    cast(improvement_surcharge as double) as improvement_surcharge,
    cast(total_amount as double) as total_amount,
    cast(congestion_surcharge as double) as congestion_surcharge,
    cast(airport_fee as double) as airport_fee
  from {{ ref('yellow_tripdata_raw') }}
),
derived as (
  select
    *,
    cast(to_date(pickup_ts) as date) as pickup_date,
    cast(hour(pickup_ts) as int) as pickup_hour,
    cast(weekofyear(pickup_ts) as int) as pickup_week,
    cast(quarter(pickup_ts) as int) as pickup_quarter,
    cast(
      case
        when month(pickup_ts) in (12, 1, 2) then 'winter'
        when month(pickup_ts) in (3, 4, 5) then 'spring'
        when month(pickup_ts) in (6, 7, 8) then 'summer'
        when month(pickup_ts) in (9, 10, 11) then 'fall'
      end as string
    ) as season_name,
    cast(
      (unix_timestamp(dropoff_ts) - unix_timestamp(pickup_ts)) / 60.0 as double
    ) as trip_duration_minutes,
    cast(
      case
        when pickup_ts is not null
          and dropoff_ts is not null
          and unix_timestamp(dropoff_ts) >= unix_timestamp(pickup_ts)
          and unix_timestamp(dropoff_ts) - unix_timestamp(pickup_ts) <= 86400
        then true else false
      end as boolean
    ) as is_valid_duration,
    cast(
      case
        when trip_distance is not null and trip_distance >= 0 and trip_distance <= 100
        then true else false
      end as boolean
    ) as is_valid_distance,
    cast(
      case
        when total_amount is not null and total_amount >= 0 and total_amount <= 1000
        then true else false
      end as boolean
    ) as is_valid_total_amount,
    cast(case when trip_distance > 60 then true else false end as boolean) as is_outlier_trip_distance,
    cast(case when total_amount > 300 then true else false end as boolean) as is_outlier_total_amount
  from raw
)
select
  service_type,
  source_year,
  source_month,
  dataset_month,
  source_file,
  ingested_at,
  vendor_id,
  pickup_ts,
  dropoff_ts,
  pickup_date,
  pickup_hour,
  pickup_week,
  pickup_quarter,
  season_name,
  passenger_count,
  trip_distance,
  rate_code_id,
  store_and_fwd_flag,
  pu_location_id,
  do_location_id,
  payment_type,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  improvement_surcharge,
  total_amount,
  congestion_surcharge,
  airport_fee,
  trip_duration_minutes,
  cast(
    case
      when is_valid_duration and trip_duration_minutes > 0 and trip_distance > 0
      then trip_distance / (trip_duration_minutes / 60.0)
    end as double
  ) as avg_speed_mph,
  cast(case when trip_distance > 0 then total_amount / trip_distance end as double) as fare_per_mile,
  cast(case when fare_amount > 0 then tip_amount / fare_amount end as double) as tip_pct,
  cast(case when coalesce(airport_fee, 0.0) > 0 then true else false end as boolean) as has_airport_fee,
  cast(case when coalesce(congestion_surcharge, 0.0) > 0 then true else false end as boolean) as has_congestion_surcharge,
  is_valid_duration,
  is_valid_distance,
  is_valid_total_amount,
  cast(case when trip_duration_minutes > 240 then true else false end as boolean) as is_outlier_trip_duration,
  is_outlier_trip_distance,
  is_outlier_total_amount,
  cast(
    case
      when is_valid_duration and is_valid_distance and is_valid_total_amount
      then true else false
    end as boolean
  ) as is_valid_trip,
  cast(
    (case when trip_duration_minutes > 240 then 1 else 0 end)
    + (case when is_outlier_trip_distance then 1 else 0 end)
    + (case when is_outlier_total_amount then 1 else 0 end)
    as int
  ) as dq_warning_count
from derived
