with trips as (
  select * from {{ ref('yellow_tripdata_silver') }}
),
zones as (
  select * from {{ ref('dim_taxi_zones_v1') }}
)
select
  cast(trips.service_type as string) as service_type,
  cast(trips.source_year as int) as source_year,
  cast(trips.source_month as int) as source_month,
  cast(trips.dataset_month as string) as dataset_month,
  cast(trips.source_file as string) as source_file,
  cast(trips.ingested_at as timestamp) as ingested_at,
  cast(trips.vendor_id as bigint) as vendor_id,
  cast(trips.pickup_ts as timestamp) as pickup_ts,
  cast(trips.dropoff_ts as timestamp) as dropoff_ts,
  cast(trips.passenger_count as double) as passenger_count,
  cast(trips.trip_distance as double) as trip_distance,
  cast(trips.rate_code_id as double) as rate_code_id,
  cast(trips.store_and_fwd_flag as string) as store_and_fwd_flag,
  cast(trips.pu_location_id as bigint) as pu_location_id,
  cast(trips.do_location_id as bigint) as do_location_id,
  cast(pu.zone_name as string) as pu_zone_name,
  cast(do.zone_name as string) as do_zone_name,
  cast(pu.borough as string) as pu_borough,
  cast(do.borough as string) as do_borough,
  cast(pu.service_zone as string) as pu_service_zone,
  cast(do.service_zone as string) as do_service_zone,
  cast(coalesce(pu.is_airport_zone, false) as boolean) as pu_is_airport_zone,
  cast(coalesce(do.is_airport_zone, false) as boolean) as do_is_airport_zone,
  cast(coalesce(not pu.is_mapped_zone, true) as boolean) as has_unmapped_pu_zone,
  cast(coalesce(not do.is_mapped_zone, true) as boolean) as has_unmapped_do_zone,
  cast(trips.payment_type as bigint) as payment_type,
  cast(trips.fare_amount as double) as fare_amount,
  cast(trips.extra as double) as extra,
  cast(trips.mta_tax as double) as mta_tax,
  cast(trips.tip_amount as double) as tip_amount,
  cast(trips.tolls_amount as double) as tolls_amount,
  cast(trips.improvement_surcharge as double) as improvement_surcharge,
  cast(trips.total_amount as double) as total_amount,
  cast(trips.congestion_surcharge as double) as congestion_surcharge,
  cast(trips.airport_fee as double) as airport_fee,
  cast(year(trips.pickup_ts) as int) as pickup_year,
  cast(month(trips.pickup_ts) as int) as pickup_month,
  cast(trips.pickup_date as date) as pickup_date,
  cast(trips.pickup_hour as int) as pickup_hour,
  cast(trips.pickup_week as int) as pickup_week,
  cast(trips.pickup_quarter as int) as pickup_quarter,
  cast(trips.season_name as string) as season_name,
  cast(dayofweek(trips.pickup_ts) as int) as dow,
  cast(case when dayofweek(trips.pickup_ts) in (1, 7) then true else false end as boolean) as is_weekend,
  cast(
    case
      when trips.pickup_hour between 5 and 11 then 'morning'
      when trips.pickup_hour between 12 and 16 then 'afternoon'
      when trips.pickup_hour between 17 and 21 then 'evening'
      else 'night'
    end as string
  ) as day_part,
  cast(
    case
      when dayofweek(trips.pickup_ts) not in (1, 7)
        and trips.pickup_hour in (7, 8, 9, 16, 17, 18, 19)
      then true else false
    end as boolean
  ) as is_peak_commute,
  cast(trips.trip_duration_minutes as double) as trip_duration_minutes,
  cast(trips.avg_speed_mph as double) as avg_speed_mph,
  cast(trips.fare_per_mile as double) as fare_per_mile,
  cast(trips.tip_pct as double) as tip_pct,
  cast(trips.has_airport_fee as boolean) as has_airport_fee,
  cast(trips.has_congestion_surcharge as boolean) as has_congestion_surcharge,
  cast(
    case
      when coalesce(pu.is_airport_zone, false) or coalesce(do.is_airport_zone, false)
      then true else false
    end as boolean
  ) as is_airport_trip,
  cast(
    case
      when pu.borough is not null and do.borough is not null and pu.borough = do.borough
      then true else false
    end as boolean
  ) as is_intra_borough_trip,
  cast(
    case
      when pu.borough is not null and do.borough is not null and pu.borough <> do.borough
      then true else false
    end as boolean
  ) as is_inter_borough_trip,
  cast(case when coalesce(trips.tip_amount, 0.0) = 0 then true else false end as boolean) as is_zero_tip,
  cast(case when coalesce(trips.trip_distance, 0.0) = 0 then true else false end as boolean) as is_zero_distance,
  cast(
    case
      when coalesce(trips.fare_amount, 0.0) < 0
        or coalesce(trips.total_amount, 0.0) < 0
        or coalesce(trips.fare_amount, 0.0) > 750
        or coalesce(trips.total_amount, 0.0) > 1000
      then true else false
    end as boolean
  ) as has_implausible_fare,
  cast(trips.is_valid_duration as boolean) as is_valid_duration,
  cast(trips.is_valid_distance as boolean) as is_valid_distance,
  cast(trips.is_valid_total_amount as boolean) as is_valid_total_amount,
  cast(trips.is_outlier_trip_duration as boolean) as is_outlier_trip_duration,
  cast(trips.is_outlier_trip_distance as boolean) as is_outlier_trip_distance,
  cast(trips.is_outlier_total_amount as boolean) as is_outlier_total_amount,
  cast(trips.is_valid_trip as boolean) as is_valid_trip,
  cast(trips.dq_warning_count as int) as dq_warning_count
from trips
left join zones as pu
  on trips.pu_location_id = pu.location_id
left join zones as do
  on trips.do_location_id = do.location_id
