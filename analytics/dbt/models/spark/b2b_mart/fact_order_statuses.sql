{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'bigquery_load': 'true'
    }
) }}

select distinct 
order_id,
event_ts_msk, 
    subStatus as sub_status, 
    status
    from
(select order_id, 
    event_ts_msk, 
    subStatus, 
    status,
    row_number() over (partition by order_id, 
    subStatus, 
    status order by time desc) as rn
    from
(select 
    order_id, 
    TIMESTAMP(millis_to_ts_msk(statuses.updatedTime)) as event_ts_msk, 
    min(event_ts_msk) as time,
    statuses.subStatus, 
    statuses.status
    from
    (
    SELECT  payload.orderId AS order_id,
        explode(payload.statusHistory) as statuses,
        event_ts_msk
    FROM {{ source('b2b_mart', 'operational_events') }}
    WHERE type  ='orderChangedByAdmin'
      )
    group by 
    order_id, 
    TIMESTAMP(millis_to_ts_msk(statuses.updatedTime)), 
    statuses.subStatus, 
    statuses.status
)
)
where rn = 1
