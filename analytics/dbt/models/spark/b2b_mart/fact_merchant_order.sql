{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'team': 'general_analytics',
      'bigquery_load': 'true'

    }
) }}
SELECT
       id,
       merchant_order_id ,
       created_ts_msk ,
       currency ,
       firstmile_channel_id ,
       friendly_id,
       manufacturing_days,
       merchant_id,
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
       dbt_valid_from as effective_ts_msk,
       dbt_valid_to as next_effective_ts_msk
from {{ ref('scd2_mongo_merchant_order') }} t