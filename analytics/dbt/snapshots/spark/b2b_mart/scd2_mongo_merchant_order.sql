{% snapshot scd2_mongo_merchant_order %}

{{
    config(
      target_schema='b2b_mart',
      unique_key='merchant_order_id',

      strategy='timestamp',
      updated_at='update_ts_msk',
      file_format='delta'
    )
}}

WITH products AS
(
    SELECT merchant_order_id,
        value.id AS product_id,
        value.type AS product_type,
        value.vatRate AS product_vat_rate
    FROM (
        SELECT _id AS merchant_order_id,
            explode(products)
        FROM {{ source('mongo', 'b2b_core_merchant_orders_v2_daily_snapshot') }}
        )
)

SELECT
        _id AS merchant_order_id ,
        millis_to_ts_msk(ctms)  AS created_ts_msk ,
        currency ,
        firstmileChannelId AS firstmile_channel_id ,
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
        millis_to_ts_msk(utms)  AS update_ts_msk ,
        merchant.paymentMethodType AS payment_method_type,
        merchant.paymentMethod._id AS payment_method_id
FROM  {{ source('mongo', 'b2b_core_merchant_orders_v2_daily_snapshot') }}  AS m
LEFT JOIN products AS p ON m._id = p.merchant_order_id

{% endsnapshot %}
