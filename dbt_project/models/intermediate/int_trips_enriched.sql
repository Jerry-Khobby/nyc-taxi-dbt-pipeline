-- Cleaned trips with the derived columns the marts need: duration, speed,
-- tip percentage, pickup time attributes and airport flags.
-- One row per trip_id.

with trips as (

    select * from {{ ref('stg_yellow_trips') }}

),

zones as (

    select location_id, is_airport from {{ ref('int_taxi_zones') }}

),

enriched as (

    select
        trips.trip_id,
        trips.vendor_id,
        trips.rate_code_id,
        trips.payment_type_id,
        trips.pickup_location_id,
        trips.dropoff_location_id,

        trips.pickup_at,
        trips.dropoff_at,
        trips.passenger_count,
        trips.trip_distance,
        trips.is_store_and_forward,

        trips.fare_amount,
        trips.extra_amount,
        trips.mta_tax_amount,
        trips.tip_amount,
        trips.tolls_amount,
        trips.improvement_surcharge_amount,
        trips.congestion_surcharge_amount,
        trips.airport_fee_amount,
        trips.cbd_congestion_fee_amount,
        trips.total_amount,

        -- Duration and speed. Staging guarantees dropoff_at > pickup_at;
        -- nullif guards the division anyway.
        round(
            extract(epoch from trips.dropoff_at - trips.pickup_at) / 60.0,
            2
        ) as trip_duration_minutes,
        round(
            trips.trip_distance
            / nullif(extract(epoch from trips.dropoff_at - trips.pickup_at) / 3600.0, 0),
            2
        ) as avg_speed_mph,

        -- Tips are only recorded for card payments (payment_type_id = 1), so
        -- tip_pct is null for every other payment type. Percentage of the fare.
        trips.payment_type_id = 1 as is_card_payment,
        case
            when trips.payment_type_id = 1
                then round(trips.tip_amount / nullif(trips.fare_amount, 0) * 100, 2)
        end as tip_pct,

        -- Pickup time attributes.
        cast(trips.pickup_at as date)                       as pickup_date,
        cast(date_trunc('month', trips.pickup_at) as date)  as pickup_month,
        cast(extract(hour from trips.pickup_at) as integer) as pickup_hour,
        -- ISO day of week: 1 = Monday ... 7 = Sunday.
        cast(extract(isodow from trips.pickup_at) as integer) as pickup_day_of_week,
        trim(to_char(trips.pickup_at, 'Day'))                as pickup_day_name,
        extract(isodow from trips.pickup_at) in (6, 7)       as is_weekend,

        -- Airport flags. Zones missing from the lookup count as non-airport.
        coalesce(pickup_zones.is_airport, false)  as is_airport_pickup,
        coalesce(dropoff_zones.is_airport, false) as is_airport_dropoff,
        coalesce(pickup_zones.is_airport, false)
            or coalesce(dropoff_zones.is_airport, false) as is_airport_trip,
        -- The airport a trip is attributed to. If both ends are airports
        -- (for example JFK to LaGuardia), the pickup airport wins.
        case
            when pickup_zones.is_airport then trips.pickup_location_id
            when dropoff_zones.is_airport then trips.dropoff_location_id
        end as airport_location_id,

        trips.source_file,
        trips.source_month,
        trips.loaded_at

    from trips
    left join zones as pickup_zones
        on trips.pickup_location_id = pickup_zones.location_id
    left join zones as dropoff_zones
        on trips.dropoff_location_id = dropoff_zones.location_id

)

select * from enriched
