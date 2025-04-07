{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@mkirusha',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}

with 
cart as (select user_id, 
      event_msk_date,
      product_id,
      actionType,
      min(event_ts_msk) event_ts_msk
      from {{ ref('ss_events_cart') }}
where actionType = 'add_to_cart'
group by All
),
interactions as (
select 
user_id,
visit_ts_msk,
visit_date,
traffic_type, 
utm_source,
utm_medium,
utm_campaign,
friendly_source, 
number_visit,
first_visit_flag, 
last_visit_flag
 from  {{ ref('fact_marketing_utm_interactions') }}
    where (friendly_source is not null or first_visit_flag is True )
),

data as (
select 
user_id,
event_msk_date,
product_id,
actionType,
visit_ts_msk,
visit_date,
utm_campaign,
utm_source,
utm_medium,
friendly_source as source,
traffic_type as type,
number_visit as number_of_interactions,
row_number() over (partition by user_id,event_msk_date,
product_id,
actionType  order by cast(visit_ts_msk as date) desc, traffic_type ) qualify_rn,
row_number() over (partition by user_id,event_msk_date,
product_id,
actionType,traffic_type order by visit_ts_msk  desc ) qualify_rn_type,
datediff(event_msk_date, visit_date) AS days_difference
from cart 
left join interactions using(user_id ) 
where  1=1
and cast(event_ts_msk as date) >=  visit_date
),
adding_window as (
select 
*,
row_number() over (partition by user_id,user_id,event_msk_date,
product_id,
actionType 
order by case when type = 'organic' then 100 else 1  end ) filter_rn  
from data 
where qualify_rn = 1 
or ( qualify_rn_type = 1 and type != 'organic' and days_difference < 14)
)
 
select 
user_id,event_msk_date,
product_id,
actionType,
visit_ts_msk,
visit_date,
utm_campaign,
utm_source,
utm_medium,
source,
type,
number_of_interactions,
qualify_rn,
qualify_rn_type,
days_difference
from adding_window    
where filter_rn = 1 
