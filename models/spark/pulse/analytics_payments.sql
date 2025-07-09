{{ config(
    schema='joompro_analytics_internal_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'bigquery_load': 'true',
      'bigquery_project_id': 'joom-analytics-joompro-public',
      'bigquery_check_counts': 'false',
      'priority_weight': '150'
    }
) }}


WITH currency AS (
    SELECT
        1000000 / rate AS rate,
        EXPLODE(
            SEQUENCE(
                effective_date,
                LEAST(next_effective_date - INTERVAL 1 DAY, CURRENT_DATE()), INTERVAL 1 DAY
            )
        ) AS date
    FROM {{ source('mart','dim_currency_rate') }}
    WHERE currency_code = "BRL"
)

SELECT
    payment._id AS payment_id,
    payment.payhubPaymentIntentId AS payhub_payment_intent_id,
    payment.usedId AS user_id,
    MILLIS_TO_TS(payment.createdTimeMs) AS payment_created_time,
    payment.packageSnapshot._id AS package_id,
    CASE WHEN payment.packageSnapshot._id LIKE "%diamond%" THEN "Diamond" ELSE "Premium" END AS package_type,
    payment.packageSnapshot.duration.unit AS package_duration_unit,
    CASE
        WHEN payment.packageSnapshot.duration.unit = "year" THEN payment.packageSnapshot.duration.value * 12
        ELSE payment.packageSnapshot.duration.value
    END AS package_duration,
    payment.packageSnapshot.price.amount / 1000000 AS package_price,
    payment.packageSnapshot.price.ccy AS package_price_ccy,
    COALESCE(MILLIS_TO_TS(payment.paidTimeMs), MILLIS_TO_TS(payment.createdTimeMs)) AS paid_time,
    TO_DATE(COALESCE(MILLIS_TO_TS(payment.paidTimeMs), MILLIS_TO_TS(payment.createdTimeMs))) AS paid_date,
    payment.price.amount / 1000000 AS price,
    payment.price.ccy AS currency,
    payment.promocodeSnapshot._id AS promocode_id,
    payment.promocodeSnapshot.code AS promocode,
    COALESCE(payment.promocodeSnapshot.discount.fixed.amount, 0) AS discount_fixed,
    COALESCE(payment.promocodeSnapshot.discount.percentage.percentage, 0) AS discount_percentage,
    currency.rate,
    payment.status
FROM {{ source('mongo', 'b2b_core_analytics_payments_daily_snapshot') }} AS payment
INNER JOIN currency ON TO_DATE(MILLIS_TO_TS(payment.createdTimeMs)) = currency.date
