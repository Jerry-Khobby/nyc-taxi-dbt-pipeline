-- One row per pickup zone, date and hour.

with trips as (

    select
        pickup_location_id,
        pickup_date,
        pickup_day_of_week,
        pickup_day_name,
        pickup_hour,
        passenger_count,
        trip_distance,
        fare_amount,
        total_amount
    from {{ ref('fct_trips') }}

),

zones as (

    select location_id, zone_name, borough
    from {{ ref('dim_zones') }}

),

aggregated as (

    select
        pickup_location_id,
        pickup_date,
        pickup_day_of_week,
        pickup_day_name,
        pickup_hour,
        count(*)                          as trip_count,
        sum(passenger_count)              as total_passengers,
        round(avg(trip_distance), 2)      as avg_trip_distance,
        round(avg(fare_amount), 2)        as avg_fare,
        sum(fare_amount)                  as fare_revenue,
        sum(total_amount)                 as total_revenue
    from trips
    group by
        pickup_location_id,
        pickup_date,
        pickup_day_of_week,
        pickup_day_name,
        pickup_hour

)

select
    aggregated.pickup_location_id,
    zones.zone_name,
    zones.borough,
    aggregated.pickup_date,
    aggregated.pickup_day_of_week,
    aggregated.pickup_day_name,
    aggregated.pickup_hour,
    aggregated.trip_count,
    aggregated.total_passengers,
    aggregated.avg_trip_distance,
    aggregated.avg_fare,
    aggregated.fare_revenue,
    aggregated.total_revenue
from aggregated
left join zones
    on aggregated.pickup_location_id = zones.location_id