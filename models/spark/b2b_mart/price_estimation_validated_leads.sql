{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'bigquery_load': 'true'
    }
) }}


with 
dim_users AS (
  SELECT DISTINCT user_id, owner_id, created_ts_msk as user_created_ts
  FROM {{ ref('dim_user') }}
  where not fake or fake is null
  and next_effective_ts_msk is null
),

users as (
    select distinct a.*, n.owner_id, email as owner_email, user_created_ts from
    (select user_id, 
        first_value(validation_status) over (partition by user_id order by event_ts_msk desc) as validation_status,
        date(min(event_ts_msk) over (partition by user_id)) as min_validation_time
    from {{ ref('fact_user_change') }}
    ) a inner join dim_users n on a.user_id = n.user_id
    left join {{ ref('dim_user_admin') }} ua on ua.admin_id = n.owner_id
    where validation_status = 'validated'
    ),

orders as (
    select distinct * from
    (
     select user_id, fo.order_id,
        fo.created_ts_msk as partition_date_msk,
        first_value(status) over (partition by user_id, fo.order_id order by event_ts_msk desc) as current_status,
        first_value(sub_status) over (partition by user_id, fo.order_id order by event_ts_msk desc) as current_sub_status,
        date(min(case when sub_status = "priceEstimation" or (status = 'selling' and sub_status != 'new') then event_ts_msk end) 
            over (partition by user_id, fo.order_id)) as min_price_estimation_time,
        date(min(case when sub_status = "delivered" then event_ts_msk end) over (partition by user_id, fo.order_id)) as min_delivered_time,
        min_manufactured_ts_msk as min_manufacturing_time,
        date(max(case when status not in ('cancelled', 'closed', 'claimed') then event_ts_msk end) over (partition by user_id)) as max_total_time,
        date(lag(case when status not in ('cancelled', 'closed', 'claimed') then event_ts_msk end) over (partition by user_id, fo.order_id order by event_ts_msk)) as current_status_time,
        lag(fos.status) over (partition by user_id, fo.order_id order by case when status != 'cancelled' then 0 else 1 end, event_ts_msk) as last_status,
        lag(fos.sub_status) over (partition by user_id, fo.order_id order by case when status != 'cancelled' then 0 else 1 end, event_ts_msk) as last_sub_status,
      row_number() over (partition by user_id, fo.order_id order by event_ts_msk desc) as rn
    from {{ ref('fact_order') }} fo
    left join
        {{ ref('fact_order_statuses_change') }} fos on fo.order_id = fos.order_id 
    )
    where rn = 1
)
    
select distinct 
u.user_id, 
o.order_id,
user_created_ts,
partition_date_msk,
validation_status,
min_validation_time,
owner_email,
current_status,
current_sub_status,
min_price_estimation_time,
min_manufacturing_time,
min_delivered_time,
current_status_time,
last_status,
last_sub_status,
max_total_time,
sum(case when min_manufacturing_time is not null then 1 else 0 end) over (partition by u.user_id) as orders
from users u 
left join orders o on u.user_id = o.user_id
where min_validation_time <= min_price_estimation_time or min_price_estimation_time is null
