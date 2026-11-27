-- One row per weekday and hour. Card payments only: tips are not recorded
-- for cash, so including them would drag the averages toward zero.

with card_trips as (

    select
        pickup_day_of_week,
        pickup_day_name,
        pickup_hour,
        tip_pct,
        tip_amount,
        fare_amount
    from {{ ref('fct_trips') }}
    where is_card_payment
      and tip_pct is not null

)

select
    pickup_day_of_week,
    pickup_day_name,
    pickup_hour,
    count(*)                                   as card_trip_count,
    round(avg(tip_pct), 2)                     as avg_tip_pct,
    round(
        cast(percentile_cont(0.5) within group (order by tip_pct) as numeric),
        2
    )                                          as median_tip_pct,
    round(sum(tip_amount) / nullif(sum(fare_amount), 0) * 100, 2)
                                               as weighted_tip_pct
from card_trips
group by
    pickup_day_of_week,
    pickup_day_name,
    pickup_hour