with arrivals as (
  select line_id, stop_id, event_ts
  from "tfl"."main_staging"."stg_arrivals"
  where event_ts is not null
),
ordered as (
  select *,
         lag(event_ts) over(partition by line_id, stop_id order by event_ts) as prev_ts
  from arrivals
),
gaps as (
  select line_id, stop_id,
         extract('epoch' from event_ts - prev_ts) as headway_s,
         date_trunc('hour', event_ts) as hour
  from ordered
  where prev_ts is not null
)
select
  line_id, stop_id, hour,
  avg(headway_s)           as avg_headway_s,
  quantile(headway_s, 0.5) as p50_headway_s,
  quantile(headway_s, 0.9) as p90_headway_s
from gaps
group by 1,2,3