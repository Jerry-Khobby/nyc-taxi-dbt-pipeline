with source as (

    select * from {{ source('raw', 'yellow_trips') }}

),

renamed as (

    select
        cast(vendorid as integer)                      as vendor_id,
        cast(tpep_pickup_datetime as timestamp)        as pickup_at,
        cast(tpep_dropoff_datetime as timestamp)       as dropoff_at,
        cast(passenger_count as integer)               as passenger_count,
        cast(trip_distance as numeric(14, 2))          as trip_distance,
        cast(ratecodeid as integer)                    as rate_code_id,
        case store_and_fwd_flag
            when 'Y' then true
            when 'N' then false
        end                                            as is_store_and_forward,
        cast(pulocationid as integer)                  as pickup_location_id,
        cast(dolocationid as integer)                  as dropoff_location_id,
        cast(payment_type as integer)                  as payment_type_id,

        cast(fare_amount as numeric(14, 2))            as fare_amount,
        cast(extra as numeric(14, 2))                  as extra_amount,
        cast(mta_tax as numeric(14, 2))                as mta_tax_amount,
        cast(tip_amount as numeric(14, 2))             as tip_amount,
        cast(tolls_amount as numeric(14, 2))           as tolls_amount,
        cast(improvement_surcharge as numeric(14, 2))  as improvement_surcharge_amount,
        cast(congestion_surcharge as numeric(14, 2))   as congestion_surcharge_amount,
        -- Spelled Airport_fee in some months; lowercased at load time.
        cast(airport_fee as numeric(14, 2))            as airport_fee_amount,
        -- Only present from 2025 onwards; null for older months.
        cast(cbd_congestion_fee as numeric(14, 2))     as cbd_congestion_fee_amount,
        cast(total_amount as numeric(14, 2))           as total_amount,

        source_file,
        -- yellow_tripdata_2024-01.parquet -> 2024-01-01
        to_date(substring(source_file from '\d{4}-\d{2}'), 'YYYY-MM') as source_month,
        loaded_at
    from source

),

filtered as (

    select * from renamed
    where {{ valid_trip_filter() }}

),

keyed as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'vendor_id',
            'pickup_at',
            'dropoff_at',
            'pickup_location_id',
            'dropoff_location_id',
            'passenger_count',
            'trip_distance',
            'payment_type_id',
            'fare_amount',
            'total_amount',
        ]) }} as trip_id,
        *
    from filtered

)

-- TLC has no trip ID, and exact duplicate rows occur. Keep one per trip_id.
select distinct on (trip_id) *
from keyed
order by trip_id, loaded_at desc
