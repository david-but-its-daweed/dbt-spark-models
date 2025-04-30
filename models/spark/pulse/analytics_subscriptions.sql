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
    subscription.payment_id,
    subscription.payhub_subsciption_id,
    subscription.user_id,
    ADD_MONTHS(subscription.created_time, (subscription.time_payed * subscription.package_duration)) AS payment_created_time,
    ADD_MONTHS(subscription.created_date, (subscription.time_payed * subscription.package_duration)) AS payment_created_date,
    subscription.package_id,
    subscription.package_duration_unit,
    subscription.package_duration,
    subscription.package_price,
    subscription.package_price_ccy,
    subscription.created_time,
    subscription.created_date,
    subscription.price AS initial_price_paid,
    -- CASE WHEN time_payed = 0 THEN package_price ELSE price END AS price,
    subscription.price,
    subscription.currency,
    subscription.promocode_id,
    subscription.promocode,
    subscription.discount_fixed,
    subscription.discount_percentage,
    subscription.status,
    subscription.subscribtion_months,
    currency.rate,
    subscription.time_payed
FROM (
    SELECT
        s.payment_id,
        s.user_id,
        s.payhub_subsciption_id,
        s.payment_created_time,
        s.package_id,
        s.package_duration_unit,
        s.package_duration,
        s.package_price,
        s.package_price_ccy,
        s.created_time,
        s.created_date,
        s.price,
        s.currency,
        s.promocode_id,
        s.promocode,
        s.discount_fixed,
        s.discount_percentage,
        s.status,
        s.subscribtion_months,
        POSEXPLODE(
            ARRAY_REPEAT(
                s.price,
                CAST(CEIL(s.subscribtion_months / s.package_duration) AS INT)
            )
        ) AS (time_payed, price_2)
    FROM (
        SELECT
            subscription._id AS payment_id,
            subscription.payhubSubsciptionId AS payhub_subsciption_id,
            subscription.usedId AS user_id,
            MILLIS_TO_TS(subscription.createdTimeMs) AS payment_created_time,
            subscription.packageSnapshot._id AS package_id,
            subscription.packageSnapshot.duration.unit AS package_duration_unit,
            CASE
                WHEN subscription.packageSnapshot.duration.unit = "year" THEN subscription.packageSnapshot.duration.value * 12
                ELSE subscription.packageSnapshot.duration.value
            END AS package_duration,
            subscription.packageSnapshot.price.amount / 1000000 AS package_price,
            subscription.packageSnapshot.price.ccy AS package_price_ccy,
            MILLIS_TO_TS(subscription.createdTimeMs) AS created_time,
            TO_DATE(MILLIS_TO_TS(subscription.createdTimeMs)) AS created_date,
            subscription.price.amount / 1000000 AS price,
            subscription.price.ccy AS currency,
            subscription.promocodeSnapshot._id AS promocode_id,
            subscription.promocodeSnapshot.code AS promocode,
            COALESCE(subscription.promocodeSnapshot.discount.fixed.amount, 0) AS discount_fixed,
            COALESCE(subscription.promocodeSnapshot.discount.percentage.percentage, 0) AS discount_percentage,
            subscription.status,
            CEIL(
                CASE
                    WHEN subscription.status = "active"
                        THEN MONTHS_BETWEEN(
                            GREATEST(
                                TO_TIMESTAMP(CURRENT_DATE()),
                                COALESCE(
                                    MILLIS_TO_TS(subscription.cancellationTime),
                                    MILLIS_TO_TS(subscription.nextChargeAttemptTime)
                                )
                            ),
                            MILLIS_TO_TS(subscription.createdTimeMs)
                        )
                    ELSE
                        MONTHS_BETWEEN(
                            COALESCE(
                                MILLIS_TO_TS(subscription.cancellationTime),
                                MILLIS_TO_TS(subscription.nextChargeAttemptTime)
                            ),
                            MILLIS_TO_TS(subscription.createdTimeMs)
                        )
                END
            ) AS subscribtion_months
        FROM {{ source('mongo', 'b2b_core_analytics_subscriptions_daily_snapshot') }} AS subscription
    ) AS s
) AS subscription
INNER JOIN currency ON TO_DATE(subscription.created_time) = currency.date
