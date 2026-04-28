select
  cast(service_type as string) as service_type,
  cast(dataset_month as string) as dataset_month,
  cast(source_year as int) as source_year,
  cast(source_month as int) as source_month,
  cast(source_file as string) as source_file,
  cast(ingested_at as timestamp) as ingested_at,
  cast(pickup_ts as timestamp) as pickup_ts,
  cast(dropoff_ts as timestamp) as dropoff_ts,
  cast(pu_location_id as bigint) as pu_location_id,
  cast(do_location_id as bigint) as do_location_id,
  cast(pu_zone_name as string) as pu_zone_name,
  cast(do_zone_name as string) as do_zone_name,
  cast(pu_borough as string) as pu_borough,
  cast(do_borough as string) as do_borough,
  cast(total_amount as double) as total_amount,
  cast(fare_amount as double) as fare_amount,
  cast(trip_distance as double) as trip_distance,
  cast(trip_duration_minutes as double) as trip_duration_minutes,
  cast(is_valid_trip as boolean) as is_valid_trip,
  cast(dq_warning_count as int) as dq_warning_count,
  cast(has_unmapped_pu_zone as boolean) as has_unmapped_pu_zone,
  cast(has_unmapped_do_zone as boolean) as has_unmapped_do_zone,
  cast(has_implausible_fare as boolean) as has_implausible_fare,
  cast(is_zero_distance as boolean) as is_zero_distance,
  cast(is_zero_tip as boolean) as is_zero_tip,
  cast(is_outlier_trip_duration as boolean) as is_outlier_trip_duration,
  cast(is_outlier_trip_distance as boolean) as is_outlier_trip_distance,
  cast(is_outlier_total_amount as boolean) as is_outlier_total_amount,
  cast(
    case
      when not is_valid_trip then 'invalid_trip'
      when has_unmapped_pu_zone or has_unmapped_do_zone then 'unmapped_zone'
      when has_implausible_fare then 'implausible_fare'
      when is_outlier_trip_duration then 'outlier_trip_duration'
      when is_outlier_trip_distance then 'outlier_trip_distance'
      when is_outlier_total_amount then 'outlier_total_amount'
      when is_zero_distance then 'zero_distance'
      when is_zero_tip then 'zero_tip'
      else 'other'
    end as string
  ) as quarantine_reason,
  current_timestamp() as quarantined_at
from {{ ref('trips_v1') }}
where
  not is_valid_trip
  or has_unmapped_pu_zone
  or has_unmapped_do_zone
  or has_implausible_fare
  or is_outlier_trip_duration
  or is_outlier_trip_distance
  or is_outlier_total_amount
  or is_zero_distance
