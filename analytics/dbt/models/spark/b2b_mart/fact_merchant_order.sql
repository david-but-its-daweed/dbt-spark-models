{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'team': 'general_analytics',
      'bigquery_load': 'true'

    }
) }}

WITH merchants AS (
    SELECT merchantId, typ as merchant_type,
            min(millis_to_ts_msk(ctms)) as first_order_created
        FROM {{ source('mongo', 'b2b_core_merchant_orders_v2_daily_snapshot') }}
    GROUP BY merchantId, typ
)

SELECT DISTINCT
       id,
       merchant_order_id ,
       TIMESTAMP(created_ts_msk) AS created_ts_msk,
       currency ,
       firstmile_channel_id ,
       friendly_id,
       manufacturing_days,
       merchant_id,
       merchant_type,
       first_order_created,
       order_id,
       afterpayment_done,
       days_after_qc,
       prepay_percent,
       prepayment_done,
       product_id,
       product_type,
       product_vat_rate,
       payment_method_type,
       payment_method_id,
       TIMESTAMP(dbt_valid_from) AS effective_ts_msk,
       TIMESTAMP(dbt_valid_to) AS next_effective_ts_msk
FROM {{ ref('scd2_mongo_merchant_order') }} t
LEFT JOIN merchants m on t.merchant_id = m.merchantId
