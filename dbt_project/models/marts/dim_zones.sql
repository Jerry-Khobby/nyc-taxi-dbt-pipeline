select
    location_id,
    borough,
    zone_name,
    service_zone,
    is_airport
from {{ ref('int_taxi_zones') }}