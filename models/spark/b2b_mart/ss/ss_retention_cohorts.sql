{{ config(
    schema='b2b_mart',
    materialized='view',
    partition_by={
         "field": "cohort_date"
    },
    meta = {
      'model_owner' : '@kirill_melnikov',
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}

with 
interactions as (
select user_id, friendly_source, utm_campaign from `b2b_mart.fact_marketing_utm_interactions` 
where first_visit_flag ) ,
ss_users as (select user_id,questionnaire_grade,Marketing_Lead_Type, 1 as ss_user
 from  {{ ref('ss_users_table') }}
 ), 
cohort as (
select 
user_id, 
min(event_msk_date) as cohort_date,
DATE_TRUNC( min(event_msk_date), WEEK(MONDAY))  as cohort_week, 
DATE_TRUNC( min(event_msk_date), MONTH)  as cohort_month,
DATE_DIFF(DATE_TRUNC( current_date() ,WEEK(MONDAY)) , DATE_TRUNC( min(event_msk_date), WEEK(MONDAY)) , WEEK) as max_week_number, 
DATE_DIFF(DATE_TRUNC( current_date() ,MONTH ) , DATE_TRUNC( min(event_msk_date), MONTH) , MONTH) as max_month_number
from  {{ ref('ss_events_startsession') }}
where landing IN ('pt-br', 'es-mx') AND bot_flag != 1 
group by 1),
activity as (
  select user_id, 
       event_msk_date 
  from  {{ ref('ss_events_startsession') }} st
  group by all 
),
activity_week as (
  select 
  cohort.user_id, 
  DATE_DIFF( event_msk_date , cohort_week , WEEK) as  week_number,
  max(1) as is_active 
  from cohort 
  join activity using(user_id)
  group by all 
),
activity_month as (
  select 
  cohort.user_id, 
  DATE_DIFF( event_msk_date , cohort_month , WEEK) as  month_number,
  max(1) as is_active 
  from cohort 
  join activity using(user_id)
  group by all 
),

deals as (select 
                 deal_created_date,
                 deal_id, 
                 user_id,
                 coalesce(final_gmv, 0) final_gmv

from {{ ref('fact_deals_with_requests') }}
where deal_type != 'Sample'
),
base_s as (
select * , 
DATE_DIFF(DATE_TRUNC( deal_created_date, MONTH), cohort_month, Month) as month_number, 
DATE_DIFF(DATE_TRUNC( deal_created_date, WEEK(MONDAY)), cohort_week, WEEK) as week_number
from cohort
join deals using(user_id)
where deal_created_date >= cohort_date and 
),
agg_week as (
  select user_id,
     week_number, 
     count(deal_id) deals, sum(final_gmv) as gmv
 from base_s 
where week_number is not null  
group by ALL
),
agg_month as (
  select user_id,
     month_number, 
    count(deal_id) deals, sum(final_gmv) as gmv

 from base_s 
where month_number is not null  
group by ALL
),
counter as (SELECT x 
FROM UNNEST(GENERATE_ARRAY(0, 500)) AS x),
week_retention as (
select 
'week' as retention_detalization,
cohort.*,
x as period_number, 
coalesce(is_active, 0) as is_active, 
coalesce(deals,0) as deals,
coalesce(gmv,0) as gmv
from cohort 
join counter on counter.x <= max_week_number
left join agg_week on agg_week.user_id = cohort.user_id and agg_week.week_number = counter.x 
left join activity_week on activity_week.user_id = cohort.user_id and activity_week.week_number = counter.x 
),
month_retention as (
select 
'month' as retention_detalization,
cohort.*,
x as period_number, 
coalesce(is_active, 0) as is_active, 
coalesce(deals,0) as deals,
coalesce(gmv,0) as gmv
from cohort 
join counter on counter.x <= max_month_number
left join agg_month on agg_month.user_id = cohort.user_id and agg_month.month_number = counter.x 
left join activity_month on activity_month.user_id = cohort.user_id and activity_month.month_number = counter.x
),
data_ as (
select * from month_retention 
union all
select * from week_retention
)
select 
data_.*, 
friendly_source, 
utm_campaign , 
questionnaire_grade,
Marketing_Lead_Type, 
coalesce( ss_user, 0) as ss_user
from data_
left join interactions using(user_id)
left join ss_users using(user_id)


