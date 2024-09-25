{{ config(
    schema='b2b_mart',
    materialized='view',
    partition_by={
         "field": "event_msk_date"
    },
    meta = {
      'model_owner' : '@kirill_melnikov',
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}


with bots as (
    select
        device_id,
        max(1) as bot_flag
    from threat.bot_devices_joompro
    where is_device_marked_as_bot or is_retrospectively_detected_bot
    group by 1
),
reg_info  as (
select 
user_id,
min(event_msk_date) as registration_end 
from  {{ ref('ss_events_authentication') }} 
where registration = 1 
group by 1 
),
users_data as (
select user_id,
registration_start, 
registration_end
from {{ ref('ss_users_table') }} 
left join reg_info using(user_id)
),
cart_data_product as (
select 
user_id, 
event_msk_date,
product_id
from {{ ref('ss_events_cart') }}
where actionType = 'add_to_cart'
group by 1,2,3) ,

pre_data as (
    select
        type,
        event_ts_msk,
        cast(event_ts_msk as DATE) as event_msk_date,
        bot_flag,
        user['userId'] as user_id,
        payload.pageurl,
        payload.pagename,
        payload.productId,
        payload.position,
        lead(event_ts_msk)
            over (partition by user['userId'], payload.productId order by event_ts_msk)
            as lead_ts_msk,
        cast(lead(event_ts_msk)
            over (partition by user['userId'], payload.productId order by event_ts_msk)
            as date)
            as lead_msk_date,
        lead(type)
            over (partition by user['userId'],  payload.productId  order by event_ts_msk)
            as lead_type,
        lead(payload.position)
            over (partition by user['userId'],payload.productId order by event_ts_msk)
            as lead_position,
        lead(payload.productId)
            over (partition by user['userId'],payload.productId order by event_ts_msk)
            as lead_product,
        lead(payload.index)
            over (partition by user['userId'],payload.productId order by event_ts_msk)
            as lead_index

    from  {{ source('b2b_mart', 'device_events') }} de 
    left join bots on bots.device_id = de.device.id
    where
        partition_date >= '2024-09-10' and (
            type in (
                'productClick', 'productPreview'
            )
            and payload.position is not null 
           
        )
), 
dd as (
select pre_data.*,
       registration_start, registration_end, 
       case when c.product_id is not null then 1 else 0 end as target_product_in_cart
from pre_data
left join users_data using(user_id)
left join cart_data_product c  on pre_data.user_id = c.user_id and pre_data.lead_product = c.product_id 
and pre_data.event_msk_date = c.event_msk_date
where pageurl not like '%.ru%' and bot_flag is null
),
product_index as (
select 
position,
productId,
min(lead_index) calculate_index,
max(event_ts_msk) as last_date 
 from dd 
where lead_type = 'productClick' 
group by 1,2
),
ss_users as (select 
user_id,
categories,
company_op_field_name
from 
{{ ref('ss_users_table') }}  )

select dd.user_id,
       categories,
       company_op_field_name,
       dd.event_msk_date,
       dd.productId,
       dd.position,
       max(case when dd.lead_index is not null then 1 else 0 end ) click,
       max(case when target_product_in_cart is not null then 1 else 0 end ) as  target_product_in_cart,
       min(dd.lead_index) as index,
       min(calculate_index) as calculate_index    
from dd 
left join ss_users using(user_id)
left join product_index on dd.productId =  product_index.productId 
and  dd.position =  product_index.position 
and dd.event_ts_msk <= product_index.last_date
where type = 'productPreview'
group by 1,2,3,4,5,6
