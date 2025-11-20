
  
    
    

    create  table
      "tfl"."main_staging"."stg_arrivals__dbt_tmp"
  
    as (
      with arrivals as (
  select
    cast(lineId as varchar)      as line_id,
    cast(stopId as varchar)      as stop_id,
    platformName                 as platform_name,
    destinationName              as destination_name,
    cast(timeToStation as int)   as time_to_station_s,
    try_cast(timestamp as timestamp) as event_ts,
    now()                        as ingested_at
  from read_parquet(
    '../data/raw/date=*/arrivals_*.parquet',
    hive_partitioning=true
  )
)

select * from arrivals
    );
  
  