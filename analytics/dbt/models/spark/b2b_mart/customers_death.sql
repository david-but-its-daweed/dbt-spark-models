{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'bigquery_load': 'true'
    }
) }}

with day as (
  SELECT attribution_day, 1 as for_join
 from UNNEST(GENERATE_DATE_ARRAY(
 '2022-04-01', current_date(), INTERVAL 1 day)) as attribution_day
)
,
gmv as (
    select distinct
    t as date_payed, 
    g.order_id,
    g.gmv_initial,
    g.initial_gross_profit,
    g.final_gross_profit,
    g.owner_email,
    g.owner_role,
    g.user_id,
    grade,
    1 as for_join,
    min(t) over (partition by g.user_id, grade) as min_date_payed
    FROM {{ ref('gmv_by_sources') }} g
    left join (select distinct user_id, coalesce(grade, 'unknown') as grade from {{ ref('users_daily_table') }}
          where partition_date_msk = (select max(partition_date_msk) from {{ ref('users_daily_table') }})
        and date_payed is not null
        ) u on u.user_id = g.user_id
    where g.owner_role != 'Dev'
)



select 
repeated_order,
dead,
lag(dead) over (partition by user_id, grade order by attribution_month , previous_time_payed , date_payed is null desc, dead) as was_dead,
user_id,
gmv_initial,
order_id,
grade,
date_payed,
previous_time_payed,
min_date_payed,
attribution_month
from
(
select
distinct 
case when previous_time_payed is not null and order_id is not null then 1 else 0 end as repeated_order,
case when attribution_day >=  previous_time_payed + interval 6 month and date_payed is null then 1 else 0 end as dead,
user_id,
gmv_initial,
order_id,
grade,
date_payed,
previous_time_payed,
min_date_payed,
date(extract(year from attribution_day), extract(month from attribution_day), 1) as attribution_month
from (
select u.user_id, g.order_id, u.grade, g.gmv_initial, g.date_payed, attribution_day, u.min_date_payed,
max(case when g2.date_payed < attribution_day then g2.date_payed end) over (partition by u.user_id, u.grade order by attribution_day) as previous_time_payed
from 
(select distinct user_id, grade, min_date_payed, for_join from gmv) u
left join
day d on u.for_join = d.for_join
left join gmv g on u.user_id = g.user_id and d.attribution_day = g.date_payed and g.grade = u.grade
left join gmv g2 on u.user_id = g2.user_id and g2.grade = u.grade
where attribution_day >= u.min_date_payed
)
)
order by attribution_month desc, previous_time_payed desc, date_payed is null, dead desc
