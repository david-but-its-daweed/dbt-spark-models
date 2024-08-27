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

pre_data as (

    select
        type,
        event_ts_msk,
        cast(event_ts_msk as DATE) as event_msk_date,
        bot_flag,
        payload.pageurl,
        payload.pagename,
        payload.categoryid,
        user['userId'] as user_id,
        lead(event_ts_msk)
            over (partition by user['userId'] order by event_ts_msk)
            as lead_ts_msk,
        cast(lead(event_ts_msk)
            over (partition by user['userId'] order by event_ts_msk)
            as date)
            as lead_msk_date,
        lead(type)
            over (partition by user['userId'] order by event_ts_msk)
            as lead_type,
        lead(payload.position)
            over (partition by user['userId'] order by event_ts_msk)
            as lead_position,
        lead(payload.categoryid)
            over (partition by user['userId'] order by event_ts_msk)
            as lead_categoryid,
        lead(payload.source)
            over (partition by user['userId'] order by event_ts_msk)
            as lead_source

    from {{ source('b2b_mart', 'device_events') }} de 
    left join bots on bots.device_id = de.device.id
    where
        partition_date >= '2024-04-01' and (
            type in (
                'contactUsClick',
                'searchBarSelect',
                'searchBarEmit',
                'learnMoreClick',
                'categoryClick',
                'productClick',
                'blogOpen',
                'click',
                'contactUsClick'
            )
            or (type = 'pageView' and payload.pagename = 'main')
        )
)

select * from pre_data
where pagename = 'main'
