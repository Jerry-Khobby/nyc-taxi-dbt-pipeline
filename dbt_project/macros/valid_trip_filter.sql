{#
    Cleaning rules for yellow taxi trips, used in a WHERE clause.
    Expects the renamed staging columns: fare_amount, trip_distance,
    pickup_at, dropoff_at and source_month.

    Rows kept:
      - positive fare
      - distance above 0 and below max_trip_distance_miles
      - drop-off after pickup, and duration below max_trip_duration_minutes
      - pickup inside the month of the file the row came from
#}
{% macro valid_trip_filter() %}
    fare_amount > 0
    and trip_distance > 0
    and trip_distance < {{ var('max_trip_distance_miles') }}
    and dropoff_at > pickup_at
    and dropoff_at < pickup_at + interval '{{ var("max_trip_duration_minutes") }} minutes'
    and pickup_at >= source_month
    and pickup_at < source_month + interval '1 month'
{% endmacro %}
