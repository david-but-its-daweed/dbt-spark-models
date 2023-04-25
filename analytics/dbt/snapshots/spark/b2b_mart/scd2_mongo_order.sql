{% snapshot scd2_mongo_order %}

{{
    config(
      target_schema='b2b_mart',
      unique_key='order_id',

      strategy='timestamp',
      updated_at='update_ts_msk',
      file_format='delta',
      invalidate_hard_deletes=True
    )
}}

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
                coalesce(col.`1`, col.`0`) AS sub_status,
                col.`2` AS ts_msk
            FROM 
            (
                SELECT _id AS order_id,
                explode(arrays_zip(state.statusHistory.status, state.statusHistory.subStatus,  state.statusHistory.updatedTimeMs))
                FROM {{ source('mongo', 'b2b_core_orders_v2_daily_snapshot') }}
            
            )
            where  col.`0` = 20
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
       element_at(state.statusHistory.status,
                  cast((array_position(state.statusHistory.updatedTimeMs,
                                       array_max(state.statusHistory.updatedTimeMs))) as INTEGER)) as last_order_status,
       element_at(state.statusHistory.subStatus,
                  cast((array_position(state.statusHistory.updatedTimeMs,
                                       array_max(state.statusHistory.updatedTimeMs))) as INTEGER)) as last_order_sub_status,
       min_manufactured_ts_msk,
       ARRAY_CONTAINS(tags, "repeated_order") as repeated_order,
       comissionRate as comission_rate
FROM {{ source('mongo', 'b2b_core_orders_v2_daily_snapshot') }} as s
LEFT JOIN manufactiring AS m ON s._id = order_id
{% endsnapshot %}
