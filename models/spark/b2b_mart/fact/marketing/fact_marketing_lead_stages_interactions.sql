{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='delta',
    meta = {
      'model_owner' : '@mkirusha',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}

with 
 statuses AS (
    SELECT
        deal_id,
        MAX(1) AS achieved_paid,
        min(cast(event_ts_msk as date)) as achieved_paid_date
    FROM {{ ref('fact_deals_status_history') }}
    WHERE status_name_small_deal LIKE '%ProcurementConfirmation' OR status_name LIKE '%PaymentToMerchant'
    GROUP BY 1
),
 cart_activation as (
 select user_id,
     min(timestamp(event_ts_msk)) as mql_ts_ms,
     max(1) user_MQL
  from b2b_mart.ss_events_cart 
 where actionType  = 'add_to_cart'
 group by 1 
 ),
 deal_activation as (
  select 
        user_id,
        min(case when deal_type = 'Sample' then  deal_created_ts end) as mql_ts_ms, 
        min(case when deal_type != 'Sample' then  deal_created_ts end) as sql_ts_ms, 
        max(case when deal_type != 'Sample' then 1 else 0 end) as user_SQL,
        max(case when deal_type = 'Sample' then 1 else 0 end) as user_MQL, 
        max(case when deal_type != 'Sample' and achieved_paid_date is not null  then 1 else 0  end ) as user_customer, 
        min(case when deal_type != 'Sample' and achieved_paid_date is not null  then achieved_paid_date end ) as customer_msk_date,
        min(case when deal_type != 'Sample' and achieved_paid_date is not null  then deal_created_ts  end ) as customer_msk_date_allocation

  from  {{ ref('fact_deals_with_requests') }} 
  left join statuses using(deal_id) 
  where deal_status not in ('Test')
  group by 1
 ),
 user_stages as (

 select user_id,
        timestamp(registration_start)  as lead_ts_ms, 

        coalesce(cart_activation.mql_ts_ms,deal_activation.mql_ts_ms, deal_activation.sql_ts_ms ) as mql_ts_ms,
        deal_activation.sql_ts_ms,
        customer_msk_date,
        customer_msk_date_allocation
  from {{ ref('ss_users_table') }} 
  left join cart_activation using(user_id)
  left join deal_activation using(user_id)
  where user_customer = 1 or deal_activation.user_SQL = 1 or coalesce(cart_activation.user_MQL,deal_activation.user_MQL,0 ) = 1  or questionnaire_grade is not null   
 ),
 Lead_events as (

select 
user_id, 
'Lead' as stage,
lead_ts_ms as stage_ts_ms,
cast(lead_ts_ms as Date) as stage_date,
lead_ts_ms as allocation_ts_ms
from 
user_stages 

union all

select 
user_id, 
'MQL' as stage, 
mql_ts_ms as stage_ts_ms,
cast(mql_ts_ms as Date) as stage_date,
mql_ts_ms as allocation_ts_ms
from 
user_stages 
where mql_ts_ms is not null 


union all

select 
user_id, 
'SQL' as stage, 
sql_ts_ms as stage_ts_ms,
cast(sql_ts_ms as Date) as stage_date,
sql_ts_ms as allocation_ts_ms
from 
user_stages 
where sql_ts_ms is not null 

union all 

select 
user_id, 
'Customer' as stage, 
customer_msk_date_allocation as stage_ts_ms,
cast(customer_msk_date as Date) as stage_date,
customer_msk_date_allocation as allocation_ts_ms
from 
user_stages 
where sql_ts_ms is not null ),
interactions as (
select 
user_id,
timestamp(visit_ts_msk) visit_ts_msk,
visit_date,
traffic_type, 
utm_source,
utm_medium,
utm_campaign,
friendly_source, 
number_visit,
first_visit_flag, 
last_visit_flag
 from {{ ref('fact_marketing_utm_interactions') }}
    where (friendly_source is not null or first_visit_flag is True )
), 
data as (

select
user_id, 
 stage, 
 stage_ts_ms,
 stage_date,
allocation_ts_ms, 
traffic_type as type,
friendly_source,
utm_campaign,
utm_source,
utm_medium,
visit_date,
row_number() over (partition by user_id, stage order by cast(visit_ts_msk as date) desc, traffic_type ) qualify_rn,
row_number() over (partition by user_id, stage,traffic_type order by visit_ts_msk  desc ) qualify_rn_type,
datediff(stage_date, visit_date) AS days_difference
from Lead_events
left join interactions using(user_id ) 
where  1=1
and cast(allocation_ts_ms as date) >=  visit_date ) ,

adding_window as (
select 
*,
row_number() over (partition by user_id,cast(allocation_ts_ms as date),
stage
order by case when type = 'organic' then 100 else 1  end ) filter_rn  
from data 
where qualify_rn = 1 
or ( qualify_rn_type = 1 and type != 'organic' and days_difference < 14)
)
select * from adding_window 
where filter_rn = 1 

