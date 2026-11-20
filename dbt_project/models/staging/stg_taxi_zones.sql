with source as (

    select * from {{ source('raw', 'taxi_zones') }}

),

renamed as (

    select
        cast(locationid as integer)     as location_id,
        nullif(trim(borough), '')       as borough,
        nullif(trim(zone), '')          as zone_name,
        nullif(trim(service_zone), '')  as service_zone,
        loaded_at
    from source
    where locationid is not null

)

-- One row per location_id, in case the lookup ever contains a duplicate.
select distinct on (location_id) *
from renamed
order by location_id, loaded_at desc
