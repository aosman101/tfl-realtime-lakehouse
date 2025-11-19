with arrivals as (
  select
    cast(null as varchar)    as line_id,
    cast(null as varchar)    as stop_id,
    cast(null as varchar)    as platform_name,
    cast(null as varchar)    as destination_name,
    cast(null as int)        as time_to_station_s,
    cast(null as timestamp)  as event_ts,
    cast(null as timestamp)  as ingested_at
  where 1=0
)

select * from arrivals