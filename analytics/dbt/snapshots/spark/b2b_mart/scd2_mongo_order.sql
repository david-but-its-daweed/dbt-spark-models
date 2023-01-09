{% snapshot scd2_mongo_order %}

{{
    config(
      target_schema='b2b_mart',
      unique_key='order_id',

      strategy='timestamp',
      updated_at='update_ts_msk',
      file_format='delta'
    )
}}

WITH manufactiring AS
(
    SELECT order_id,
            millis_to_ts_msk(MIN(col.`1`)) AS min_manufactured_ts_msk
    FROM (
        SELECT _id AS order_id,
            explode(array_sort(arrays_zip(state.statusHistory.status, state.statusHistory.updatedTimeMs)))
        FROM {{ source('mongo', 'b2b_core_orders_v2_daily_snapshot') }}

        )
    WHERE col.`0` = 20
    GROUP BY 1
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
        min_manufactured_ts_msk
FROM {{ source('mongo', 'b2b_core_orders_v2_daily_snapshot') }} as s
LEFT JOIN manufactiring AS m ON s._id = order_id
{% endsnapshot %}
