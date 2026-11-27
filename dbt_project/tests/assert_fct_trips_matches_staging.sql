-- fct_trips should contain exactly the trips in stg_yellow_trips.
-- Returns a row only if the counts differ.

with fct as (
    select count(*) as fct_rows from {{ ref('fct_trips') }}
),

stg as (
    select count(*) as stg_rows from {{ ref('stg_yellow_trips') }}
)

select fct_rows, stg_rows
from fct
cross join stg
where fct_rows <> stg_rows