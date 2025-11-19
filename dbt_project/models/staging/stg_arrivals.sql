{#- Check for raw parquet files; allow builds to succeed even if none exist yet. -#}
{%- set default_path = "../data/raw/date=*/arrivals_*.parquet" -%}
{%- set raw_path = env_var("RAW_ARRIVALS_PATH", default_path) -%}
{%- set files_exist = true -%}
{%- if execute -%}
  {%- set escaped = raw_path.replace("'", "''") -%}
  {%- set count_sql -%}
    select count(*) as cnt from glob('{{ escaped }}')
  {%- endset -%}
  {%- set result = run_query(count_sql) -%}
  {%- if result is not none and result.columns[0][0] == 0 -%}
    {%- set files_exist = false -%}
  {%- endif -%}
{%- endif -%}

with arrivals as (
  {%- if files_exist %}
  select
    cast(lineId as varchar)      as line_id,
    cast(stopId as varchar)      as stop_id,
    platformName                 as platform_name,
    destinationName              as destination_name,
    cast(timeToStation as int)   as time_to_station_s,
    try_cast(timestamp as timestamp) as event_ts,
    now()                        as ingested_at
  from read_parquet(
    '{{ raw_path }}',
    hive_partitioning=true
  )
  {%- else %}
  select
    cast(null as varchar)    as line_id,
    cast(null as varchar)    as stop_id,
    cast(null as varchar)    as platform_name,
    cast(null as varchar)    as destination_name,
    cast(null as int)        as time_to_station_s,
    cast(null as timestamp)  as event_ts,
    cast(null as timestamp)  as ingested_at
  where 1=0
  {%- endif %}
)

select * from arrivals
