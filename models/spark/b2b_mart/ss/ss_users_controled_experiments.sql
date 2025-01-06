{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'model_owner' : '@mkirusha',
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}


with 
bots as (
                            select device_id, max(1) as bot_flag
                        from threat.bot_devices_joompro 
                        where is_device_marked_as_bot or is_retrospectively_detected_bot
                        group by 1 ), 
                        
first_sesion  as (
   select   
      user['userId'] user_id ,
      min(event_ts_msk ) as start_int_ts
     from {{ source('b2b_mart', 'device_events') }}
     where  partition_date >= '2024-04-01'
            and  type = 'sessionStart'
            group by 1 ),
exp as (
 select 
            user['userId'] as user_id,
            explode(device.experiments),
            bot_flag, 
            event_ts_msk
        from {{ source('b2b_mart', 'device_events') }} de
      left join bots on de.device.id = bots.device_id
            where  
                    partition_date >= '2024-01-01'
            and  type = 'sessionStart'
       
            )
    select 
    user_id, 
    key as test,
    value as group,
    min(start_int_ts) as first_session_ts, 
    max(bot_flag) as bot_flag,
    min(event_ts_msk) as start_exp 
    from exp 
    join first_sesion using(user_id)
    where key is not null
    group by 1,2,3
