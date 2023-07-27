{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'bigquery_load': 'true'
    }
) }}

select distinct 
    fos.order_id,
    fos.event_ts_msk, 
    fos.subStatus as sub_status, 
    fos.status,
    coalesce(fos.current_status = fos.status and fos.current_sub_status = fos.subStatus, false)  as current_status,
    cast(ks.id as int) as status_id,
    coalesce(cast(kss.id as int), cast(ks.id as int)*100) as sub_status_id
    from
(select order_id, 
    event_ts_msk, 
    subStatus, 
    status,
    current_status,
    current_sub_status,
    row_number() over (partition by order_id, 
    subStatus, 
    status order by time desc) as rn
    from
(select 
        order_id, 
        TIMESTAMP(millis_to_ts_msk(statuses.updatedTime)) as event_ts_msk, 
        min(event_ts_msk) as time,
        statuses.subStatus, 
        statuses.status,
        current_status,
        current_sub_status
        from
        (
        SELECT 
            payload.orderId AS order_id,
            explode(payload.statusHistory) as statuses,
            event_ts_msk,
            first_value(payload.status) over (partition by payload.orderId order by event_ts_msk desc) as current_status,
            first_value(payload.subStatus) over (partition by payload.orderId order by event_ts_msk desc) as current_sub_status
        FROM {{ source('b2b_mart', 'operational_events') }}
        WHERE type  ='orderChangedByAdmin'
          ) status
    left join (select distinct status, id from {{ ref('key_order_status') }}) k1 on status.statuses.status = k1.status
    left join (select distinct status, id from {{ ref('key_order_status') }}) k2 on current_status = k2.status
    left join (select distinct sub_status, id from {{ ref('key_order_substatus') }}) k3 on status.statuses.subStatus = k3.sub_status
    left join (select distinct sub_status, id from {{ ref('key_order_substatus') }}) k4 on current_sub_status = k4.sub_status
    where k1.id <= k2.id and coalesce(k3.id, k1.id*100) <= coalesce(k4.id, k2.id*100)
    group by 
        order_id, 
        TIMESTAMP(millis_to_ts_msk(statuses.updatedTime)), 
        statuses.subStatus, 
        statuses.status,
        current_status,
        current_sub_status
)
) fos
left join {{ ref('key_order_status') }} ks on fos.status = ks.status
left join {{ ref('key_order_substatus') }} kss on fos.subStatus = kss.sub_status
where rn = 1
