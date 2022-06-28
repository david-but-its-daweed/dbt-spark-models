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

SELECT
        _id AS order_id,
        millis_to_ts_msk(ctms) AS created_ts_msk,
        millis_to_ts_msk(utms) AS update_ts_msk,
        currencies.clientCcy AS ccy,
        deliveryTimeDays AS delivery_time_days,
        friendlyId AS friendly_id,
        popupReqId AS request_id,
        linehaulChannelID AS linehaul_channel_id,
        csmr.deviceId as device_id,
        csmr.Id as user_id,
        state.rejectReason as reject_reason,
        state.statusHistory.status[array_position(state.statusHistory.updatedTimeMs,array_max(state.statusHistory.updatedTimeMs))-1] as last_order_status
FROM {{ source('mongo', 'b2b_core_orders_v2_daily_snapshot') }}
{% endsnapshot %}
