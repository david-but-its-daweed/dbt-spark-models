{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'bigquery_load': 'false'
    }
) }}

WITH products AS (
    SELECT
        merchant_order_id,
        value.id AS product_id,
        value.type AS product_type,
        value.vatRate AS product_vat_rate
    FROM (
        SELECT
            _id AS merchant_order_id,
            explode(products)
        FROM {{ ref('scd2_merchant_orders_v2_snapshot') }}
        WHERE dbt_valid_to IS NULL
    )
)

SELECT
    _id AS merchant_order_id,
    millis_to_ts_msk(ctms)  AS created_ts_msk,
    currency,
    firstmileChannelId AS firstmile_channel_id,
    friendlyId AS friendly_id,
    manDays AS manufacturing_days,
    merchantId AS merchant_id,
    orderId AS order_id,
    paymentSchedule.afterpaymentDone AS afterpayment_done,
    paymentSchedule.daysAfterQC AS days_after_qc,
    paymentSchedule.prepayPercent AS prepay_percent,
    paymentSchedule.prepaymentDone AS prepayment_done,
    product_id,
    product_type,
    product_vat_rate,
    deleted,
    skipBilling AS skip_billing,
    typ as merchant_type,
    millis_to_ts_msk(utms) AS update_ts_msk,
    merchant.paymentMethodType AS payment_method_type,
    merchant.paymentMethod._id AS payment_method_id,
    dbt_valid_from,
    dbt_valid_to
FROM  {{ ref('scd2_merchant_orders_v2_snapshot') }} AS m
LEFT JOIN products AS p ON m._id = p.merchant_order_id
