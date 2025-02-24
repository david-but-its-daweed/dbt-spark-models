{{ config(
    schema='mart',
    materialized='view',
    meta = {
      'model_owner' : '@aleksandrov',
      'team': 'platform'
    }
) }}

WITH payment_orders AS (
    SELECT
        data.target.id AS _id,
        `data`.`method`.`type` AS payment_type,
        `data`.`target`.`type` AS target_type
    FROM {{ source('mongo', 'payment_payment_orders_daily_snapshot') }}
    WHERE `status` = 100
),

payment_types AS (
    SELECT
        CASE WHEN po.target_type = "orderGroup" THEN po._id ELSE pi.data.md.mark.gId END AS order_group_id,
        MAX(po.payment_type) AS payment_type
    FROM payment_orders AS po
    LEFT JOIN {{ source('mongo', 'payment_intents_daily_snapshot') }} AS pi
        ON po._id = pi._id
    GROUP BY 1
)

SELECT
    fo.*,
    pt.payment_type
FROM {{ source('mart', 'fact_order_2020') }} AS fo
LEFT JOIN payment_types AS pt
    ON fo.order_group_id = pt.order_group_id
