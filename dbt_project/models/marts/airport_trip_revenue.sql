-- One row per airport and pickup month. Each trip is attributed to a single
-- airport in int_trips_enriched, so the from/to counts add up to trip_count.

with airport_trips as (

    select
        airport_location_id,
        pickup_month,
        is_airport_pickup,
        trip_distance,
        fare_amount,
        total_amount
    from {{ ref('fct_trips') }}
    where is_airport_trip

),

airports as (

    select location_id, zone_name as airport_name
    from {{ ref('dim_zones') }}
    where is_airport

),

aggregated as (

    select
        airport_location_id,
        pickup_month,
        count(*)                                        as trip_count,
        count(*) filter (where is_airport_pickup)       as trips_from_airport,
        count(*) filter (where not is_airport_pickup)   as trips_to_airport,
        round(avg(trip_distance), 2)                    as avg_trip_distance,
        round(avg(fare_amount), 2)                      as avg_fare,
        sum(fare_amount)                                as fare_revenue,
        sum(total_amount)                               as total_revenue
    from airport_trips
    group by airport_location_id, pickup_month

)

select
    aggregated.airport_location_id,
    airports.airport_name,
    aggregated.pickup_month,
    aggregated.trip_count,
    aggregated.trips_from_airport,
    aggregated.trips_to_airport,
    aggregated.avg_trip_distance,
    aggregated.avg_fare,
    aggregated.fare_revenue,
    aggregated.total_revenue
from aggregated
left join airports
    on aggregated.airport_location_id = airports.location_id
order by aggregated.pickup_month, aggregated.airport_location_id