{{
  config(
    materialized="table",
    partition_by=none
  )
}}

select
  cast(LocationID as bigint) as location_id,
  cast(Borough as string) as borough,
  cast(Zone as string) as zone_name,
  cast(service_zone as string) as service_zone,
  cast(
    case
      when lower(Borough) in ('ewr', 'unknown', 'n/a') then false
      when lower(Zone) like '%airport%' then true
      else false
    end as boolean
  ) as is_airport_zone,
  cast(
    case
      when lower(Borough) not in ('unknown', 'n/a') and lower(Zone) <> 'n/a'
      then true else false
    end as boolean
  ) as is_mapped_zone
from {{ ref('taxi_zone_lookup_raw') }}
