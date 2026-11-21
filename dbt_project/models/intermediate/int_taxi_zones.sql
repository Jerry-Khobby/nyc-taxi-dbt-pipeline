-- Taxi zones with an is_airport flag. The airport rule lives here once, so
-- the trip models and dim_zones all use the same definition.

with zones as (

    select * from {{ ref('stg_taxi_zones') }}

)

select
    location_id,
    borough,
    zone_name,
    service_zone,
    -- Newark (service zone EWR), JFK and LaGuardia (service zone Airports).
    coalesce(service_zone in ('Airports', 'EWR'), false) as is_airport,
    loaded_at
from zones
