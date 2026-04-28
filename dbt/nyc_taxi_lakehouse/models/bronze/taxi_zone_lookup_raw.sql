{{
  config(
    materialized="table"
  )
}}

{% set landing_path = reference_landing_path("taxi_zones", "taxi_zone_lookup.csv") %}

select
  cast(_c0 as bigint) as LocationID,
  cast(_c1 as string) as Borough,
  cast(_c2 as string) as Zone,
  cast(_c3 as string) as service_zone,
  input_file_name() as source_file,
  current_timestamp() as ingested_at
from csv.`{{ landing_path }}`
where _c0 <> 'LocationID'
