{{
  config(
    materialized='table',
    alias='referral_orders',
    schema='gold',
    file_format='parquet',
    meta = {
        'model_owner' : '@analytics.duty',
        'bigquery_load': 'true',
        'bigquery_overwrite': 'true',
        'priority_weight': '1000',
    }
  )
}}

WITH referral_old AS (
    SELECT
        id,
        user_id,
        refid,
        date_msk AS order_date_msk
    FROM
        {{ ref('fact_user_points_transactions') }}
    WHERE
        TRUE
        AND type = "referral"
        AND date_msk >= "2022-01-01"

),

finalize_old AS (
    SELECT
        refid,
        effective_usd
    FROM
        {{ ref('fact_user_points_transactions') }}
    WHERE
        TRUE
        AND type = "finalize"
        AND date_msk >= "2022-01-01"

),

points_referral__old AS (
    SELECT
        r.user_id AS referrer_id,
        r.refid AS order_id,
        f.effective_usd,
        r.order_date_msk
    FROM
        referral_old AS r
    INNER JOIN
        finalize_old AS f
        ON
            r.id = f.refid
),

product_purchase_100 AS (
    SELECT
        user_id,
        device_id,
        payload.orderid AS order_id,
        partition_date
    FROM {{ source("mart", "device_events") }}
    WHERE
        1 = 1
        AND type = "productPurchase"
        AND partition_date >= DATE_TRUNC("YEAR", DATE_SUB(CURRENT_DATE(), 2 * 365))
),

orders_old AS (
    SELECT
        t1.order_id,
        t1.order_date_msk,
        t2.user_id,
        t2.device_id,
        t1.referrer_id AS referrer_user_id,
        t1.effective_usd AS points_to_referrer_user,
        "externalLink" AS referral_type
    FROM
        points_referral__old AS t1
    INNER JOIN
        product_purchase_100 AS t2
        ON
            t1.order_id = t2.order_id
    WHERE
        t2.partition_date >= "2022-01-01"
),

orders_new AS (
    SELECT
        order_id,
        TO_DATE(event_ts_msk) AS order_date_msk,
        user_id,
        device_id,
        referrer_id AS referrer_user_id,
        effective_usd AS points_to_referrer_user,
        revenueshare_type AS referral_type
    FROM
        {{ ref('fact_referral_purchase') }}
)

SELECT * FROM orders_old

UNION ALL

SELECT * FROM orders_new
