{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'bigquery_load': 'false'
    }
) }}



WITH manufactiring AS
(
    select millis_to_ts_msk(min(ts_msk)) as min_manufactured_ts_msk, order_id
    from
    (
    select distinct ts_msk, order_id
    from
    (
        select min(sub_status) over (partition by order_id) as min_sub_status, sub_status, ts_msk, order_id
        from
        (
            SELECT 
                order_id,
                coalesce(col.`subStatus`, col.`status`) AS sub_status,
                col.`updatedTimeMs` AS ts_msk
            FROM 
            (
                SELECT _id AS order_id,
                explode(arrays_zip(state.statusHistory.status, state.statusHistory.subStatus,  state.statusHistory.updatedTimeMs))
                FROM {{ ref('scd2_orders_v2_snapshot') }}
            
            )
            where  col.`status` = 20
        )
    )
    where min_sub_status= sub_status
    )
    group by order_id
)


SELECT _id AS                              order_id,
       millis_to_ts_msk(ctms)           AS created_ts_msk,
       millis_to_ts_msk(utms)           AS update_ts_msk,
       currencies.clientCcy             AS ccy,
       deliveryTimeDays                 AS delivery_time_days,
       friendlyId                       AS friendly_id,
       popupReqId                       AS request_id,
       linehaulChannelID                AS linehaul_channel_id,
       csmr.deviceId                    as device_id,
       csmr.Id                          as user_id,
       state.rejectreason               as reject_reason,
       roleSet.roles.`owner`.moderatorId as owner_id,
       roleSet.roles.`customs`.moderatorId as customs_id,
       roleSet.roles.`logistician`.moderatorId as logistician_id,
       roleSet.roles.`bizDev`.moderatorId as biz_dev_id,
       roleSet.roles.`lawyer`.moderatorId as lawyer_id,
       element_at(state.statusHistory.status,
                  cast((array_position(state.statusHistory.updatedTimeMs,
                                       array_max(state.statusHistory.updatedTimeMs))) as INTEGER)) as last_order_status,
       element_at(state.statusHistory.subStatus,
                  cast((array_position(state.statusHistory.updatedTimeMs,
                                       array_max(state.statusHistory.updatedTimeMs))) as INTEGER)) as last_order_sub_status,
       min_manufactured_ts_msk,
       ARRAY_CONTAINS(tags, "repeated_order") as repeated_order,
       comissionRate as comission_rate,
       dbt_scd_id,
       dbt_updated_at,
       dbt_valid_from,
       dbt_valid_to
FROM {{ ref('scd2_orders_v2_snapshot') }} as s
LEFT JOIN manufactiring AS m ON s._id = order_id
