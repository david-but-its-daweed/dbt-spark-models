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
    FROM {{ source('mart', 'dim_currency_rate') }}
    WHERE currency_code = "BRL" AND effective_date <= LEAST(next_effective_date - INTERVAL 1 DAY, CURRENT_DATE())
),

last_subscription_payment AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY payment_id ORDER BY time_payed DESC) AS rn
    FROM {{ ref('analytics_subscriptions') }}
),

payments AS (
    SELECT
        *,
        POSEXPLODE(
            SEQUENCE(
                payment_created_time,
                (CURRENT_DATE()),
                MAKE_INTERVAL(0, package_duration, 0, 0, 0, 0, 0)
            )
        ) AS (payment_number, package_date)
    FROM (
        SELECT
            subscription.payment_id,
            subscription.user_id,
            subscription.payment_created_time,
            subscription.payment_created_date,
            subscription.package_id,
            subscription.package_type,
            subscription.package_duration_unit,
            subscription.package_duration,
            subscription.package_price,
            subscription.package_price_ccy,
            subscription.created_time,
            subscription.created_date,
            subscription.price AS initial_price_paid,
            subscription.price,
            subscription.currency,
            subscription.promocode_id,
            subscription.promocode,
            subscription.discount_fixed,
            subscription.discount_percentage,
            subscription.status,
            subscription.subscribtion_months
        FROM last_subscription_payment AS subscription
        WHERE
            package_duration_unit IN ("year", "month")
            AND rn = 1 AND payment_created_time <= CURRENT_DATE()
    )
)

SELECT
    subscription.payment_id,
    subscription.user_id,
    subscription.package_date,
    subscription.payment_created_time,
    subscription.payment_created_date,
    subscription.package_id,
    subscription.package_type,
    subscription.package_duration_unit,
    subscription.package_duration,
    subscription.package_price,
    subscription.package_price_ccy,
    subscription.created_time,
    subscription.created_date,
    subscription.price AS initial_price_paid,
    subscription.price,
    subscription.currency,
    subscription.promocode_id,
    subscription.promocode,
    subscription.discount_fixed,
    subscription.discount_percentage,
    subscription.subscribtion_months,
    currency.rate,
    subscription.status,
    subscription.payment_number
FROM payments AS subscription
INNER JOIN currency ON TO_DATE(subscription.package_date) = currency.date
WHERE subscription.payment_number != 0
