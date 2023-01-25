{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'bigquery_load': 'true'
    }
) }}


with 
not_jp_users AS (
  SELECT DISTINCT u.user_id
  FROM {{ ref('fact_user_request') }} f
  LEFT JOIN {{ ref('fact_order') }} u ON f.user_id = u.user_id
  WHERE is_joompro_employee = TRUE
),

users as (
    select distinct a.* from
    (select user_id, 
        first_value(validation_status) over (partition by user_id order by event_ts_msk desc) as validation_status,
        date(min(event_ts_msk) over (partition by user_id)) as min_validation_time
    from {{ ref('fact_user_change') }}
    ) a left join not_jp_users n on a.user_id = n.user_id
    where validation_status = 'validated' and n.user_id is null
    ),
    
user_admin as (
    select distinct user_id, owner_id from
    {{ ref('dim_user') }}
    where next_effective_ts_msk is null
),

user_order as (
    select distinct user_id, order_id
    from {{ ref('fact_order') }}
    where next_effective_ts_msk is null
),

admin AS (
    SELECT distinct 
        user_id,
        email as owner_email
    FROM {{ ref('dim_user_admin') }} ua
    left join user_admin uo on ua.admin_id = uo.owner_id
),

orders as (
    select distinct * from
    (select user_id, o.order_id,
        i.partition_date_msk,
        first_value(status) over (partition by user_id, interaction_id order by event_ts_msk desc) as current_status,
        first_value(sub_status) over (partition by user_id, interaction_id order by event_ts_msk desc) as current_sub_status,
        date(min(case when sub_status = "priceEstimation" or (status = 'selling' and sub_status != 'new') then event_ts_msk end) over (partition by user_id, interaction_id)) as min_price_estimation_time,
        date(first_value(event_ts_msk) over (partition by user_id, interaction_id order by case when status not in ('cancelled', 'closed', 'claimed') then 0 else 1 end, event_ts_msk desc)) as current_status_time,
        first_value(status) over (partition by user_id, interaction_id order by case when status != 'cancelled' then 0 else 1 end, event_ts_msk desc) as last_status,
        first_value(sub_status) over (partition by user_id, interaction_id order by case when status != 'cancelled' then 0 else 1 end, event_ts_msk desc) as last_sub_status
    from {{ ref('fact_order_change') }} o 
    left join user_order u on o.order_id = u.order_id
    left join (select distinct order_id, interaction_id, partition_date_msk from {{ ref('fact_interactions') }} where rn = 1) i on o.order_id = i.order_id
    )
)
    
select distinct 
u.user_id, 
o.order_id,
partition_date_msk,
validation_status,
min_validation_time,
owner_email,
current_status,
current_sub_status,
min_price_estimation_time,
current_status_time,
last_status,
last_sub_status
from users u 
left join orders o on u.user_id = o.user_id
left join admin a on u.user_id = a.user_id
where min_validation_time <= min_price_estimation_time or min_price_estimation_time is null
