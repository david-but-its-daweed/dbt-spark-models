{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'team': 'general_analytics',
      'bigquery_load': 'true'

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

SELECT
       t.order_id,
       TIMESTAMP(created_ts_msk) AS created_ts_msk,
       ccy AS user_ccy,
       delivery_time_days,
       friendly_id,
       request_id,
       linehaul_channel_id,
       device_id,
       user_id,
       reject_reason,
       last_order_status,
       last_order_sub_status,
       m.min_manufactured_ts_msk,
       TIMESTAMP(dbt_valid_from) AS effective_ts_msk,
       TIMESTAMP(dbt_valid_to) AS next_effective_ts_msk
FROM {{ ref('scd2_mongo_order') }} t
left join manufactiring m on t.order_id = m.order_id
