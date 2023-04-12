{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}

with deals as (select distinct
interaction_id, user_id, deal_id, estimated_gmv, deal_type,
               status, status_int, current_date
from {{ ref('fact_deals') }} where partition_date_msk = 
(select max(partition_date_msk) from {{ ref('fact_deals') }})
),

interactions as (
    select distinct interaction_id, source, type, campaign,
    current_source, current_type, current_campaign
    from {{ ref('fact_interactions') }}
),

users as (
select distinct user_id, grade, grade_probability
from {{ ref('users_daily_table') }}
    where partition_date_msk = (
    select max(partition_date_msk) from {{ ref('users_daily_table') }}
    )
),


t as (select distinct 
deal_id, 
status,
date(min_date) - dayofweek(min_date) + 1 as week,
min_date,
deal_created_date,
status_int,
lead(min_date) over (partition by deal_id order by min_date) as next_status_date,
lag(min_date) over (partition by deal_id order by min_date) as prev_status_date,
lead(status) over (partition by deal_id order by min_date) as next_status,
lag(status_int) over (partition by deal_id order by min_date) as prev_status_int,
lead(status_int) over (partition by deal_id order by min_date) as next_status_int,
lag(status) over (partition by deal_id order by min_date) as prev_status,
'day' as attribution
from
(
select distinct 
deal_id, status, min_date, max_date, deal_created_date, status_int
from
(select distinct
case when status in (10, 20, 30) then 'Pre-Estimate'
when status in (40, 50, 60) then 'Quotation'
when status in (70, 80) then 'Forming order & singing'
when status in (90) then 'Manufacturing & Shipping'
else 100
end as status,
case when status in (10, 20, 30) then 10
when status in (40, 50, 60) then 40
when status in (70, 80) then 70
when status in (90) then 90
else 'Closed'
end as status_int,
max(date) over (partition by deal_id , case when status in (10, 20, 30) then 'Pre-Estimate'
when status in (40, 50, 60) then 'Quotation'
when status in (70, 80) then 'Forming order & singing'
when status in (90) then 'Manufacturing & Shipping'
else 'Closed'
end) as max_date,
min(date) over (partition by deal_id, case when status in (10, 20, 30) then 'Pre-Estimate'
when status in (40, 50, 60) then 'Quotation'
when status in (70, 80) then 'Forming order & singing'
when status in (90) then 'Manufacturing & Shipping'
else 'Closed'
end) as min_date,
millis_to_ts_msk(ctms) as deal_created_date,
deal_id
from
(
select deal_id, millis_to_ts_msk(statuses.ctms) as date, ctms,
statuses.status as status, statuses.moderatorId as owner_id

from
(select entityId as deal_id, explode(statusHistory) as statuses, ctms
    from {{ source('mongo', 'b2b_core_issues_daily_snapshot') }}
    where type = 4 
    )
    order by date desc, deal_id
)
)
)
),


t1 as (
select 
deal_id, 
status,
w.week,
min_date,
deal_created_date,
status_int,
next_status_date,
prev_status_date,
next_status,
prev_status_int,
next_status_int,
prev_status,
'day' as attribution
from
(
select distinct 
deal_id, 
status,
week,
min_date,
deal_created_date,
status_int,
next_status_date,
prev_status_date,
next_status,
prev_status_int,
next_status_int,
prev_status,
1 as for_join
from
t
) t
left join (
    select
    explode(sequence(to_date('2022-01-01'), current_date(), interval 1 week)) as week, 
    1 as for_join
    ) w on t.for_join = w.for_join
where deal_created_date <= w.week
and (next_status_date >= w.week
    or next_status_date is null)
and (min_date <= w.week + interval 1 week)
),


t2 as 
( select 
    deal_id, 
    status,
    explode(sequence(week_month_ago, week, interval 1 week)) as week,
min_date,
deal_created_date,
status_int,
next_status_date,
prev_status_date,
next_status,
prev_status_int,
next_status_int,
prev_status,
'month' as attribution
from
(
select distinct 
deal_id, 
status,
week + interval 1 month as week,
week as week_month_ago,
min_date,
deal_created_date,
status_int,
next_status_date,
prev_status_date,
next_status,
prev_status_int,
next_status_int,
prev_status
from
t
)
)


select 
    *,
    row_number() over (partition by week, attribution, deal_id order by min_date) as week_event_number,
    row_number() over (partition by week, attribution, deal_id order by min_date desc) as week_event_number_desc
from
(
select distinct
t1.*, 
CASE WHEN 
MAX(d.status_int > 100 and t1.status_int = 100 and (prev_status_int is null or prev_status_int = 10)) OVER (PARTITION BY t1.deal_id)
THEN "fast_rejected" ELSE "normal" end as deal_normality,
d.interaction_id, d.user_id, d.estimated_gmv, d.deal_type,
d.status as current_status, d.status_int as current_status_int, d.current_date as current_status_date,
i.source, i.type, i.campaign,
i.current_source, i.current_type, i.current_campaign,
grade, grade_probability
from (
    select * from t1
    where week <= current_date()
    union all 
    select * from t2
    where week <= current_date()
) t1
left join deals d on t1.deal_id = d.deal_id
left join interactions i on i.interaction_id = d.interaction_id
left join users u on d.user_id = u.user_id
)
