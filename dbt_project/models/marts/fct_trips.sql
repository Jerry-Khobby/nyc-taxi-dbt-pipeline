{{
    config(
        materialized='incremental',
        unique_key='trip_id',
        incremental_strategy='delete+insert',
        on_schema_change='append_new_columns',
        indexes=[
            {'columns': ['pickup_date']},
            {'columns': ['pickup_location_id']},
            {'columns': ['airport_location_id']}
        ]
    )
}}

-- One row per trip. Incremental: only trips loaded since the last run are
-- processed. >= (not >) reprocesses the latest batch, which is safe because
-- unique_key replaces rows instead of duplicating them.

with trips as (

    select * from {{ ref('int_trips_enriched') }}

    {% if is_incremental() %}
    where loaded_at >= (select max(loaded_at) from {{ this }})
    {% endif %}

)

select * from trips