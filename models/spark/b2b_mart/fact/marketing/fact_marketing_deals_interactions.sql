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
deals as (select 
distinct 
user_id,
deal_id,
created_ts_msk, 
country
 from  {{ ref('fact_deals') }}
where next_effective_ts_msk is null  
and country != 'RU'
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
 from   {{ ref('fact_marketing_utm_interactions') }}
    where (friendly_source is not null or first_visit_flag is True )
),
dop_info as (select 
user_id, 
utm_campaign as first_utm_campaign ,
utm_source as first_utm_sourceas ,
utm_medium as first_utm_medium ,
friendly_source as first_source ,
traffic_type as first_type 
from interactions
where first_visit_flag is True),
data as (
select 
user_id,
deal_id,
country,
visit_ts_msk,
visit_date,
utm_campaign,
utm_source,
utm_medium,
friendly_source as source,
traffic_type as type,
first_utm_campaign ,
first_utm_sourceas ,
first_utm_medium,
first_source,
first_type,
number_visit as number_of_interactions,
row_number() over (partition by user_id,deal_id order by cast(visit_ts_msk as date) desc, traffic_type ) qualify_rn,
row_number() over (partition by user_id,deal_id,traffic_type order by visit_ts_msk  desc ) qualify_rn_type,
datediff(created_ts_msk, visit_date) AS days_difference
from deals 
left join interactions using(user_id ) 
left join dop_info using(user_id ) 
where  1=1
and cast(created_ts_msk as date) >=  visit_date
order by user_id,deal_id, visit_ts_msk),
adding_window as (
select 
*,
row_number() over (partition by user_id,deal_id order by case when type = 'organic' then 100 else 1  end ) filter_rn  
from data 
where qualify_rn = 1 
or ( qualify_rn_type = 1 and type != 'organic' and days_difference < 14)
)
 
select 
user_id,
deal_id,
visit_ts_msk,
visit_date,
utm_campaign,
utm_source,
utm_medium,
source,
type,
first_utm_campaign ,
first_utm_sourceas ,
first_utm_medium,
first_source,
first_type,
number_of_interactions,
qualify_rn,
qualify_rn_type,
days_difference
from adding_window    
where filter_rn = 1 
