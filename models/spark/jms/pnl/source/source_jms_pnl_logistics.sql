{{
  config(
    file_format='delta',
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    on_schema_change='sync_all_columns',
    meta = {
        'model_owner' : '@leonid.enov',
    }
  )
}}

WITH logistics_exploded_items AS (
    SELECT
        EXPLODE(items.externalId) AS friendly_order_id,
        orderNumber AS order_number,
        SIZE(items.externalId) AS order_count_in_jl_order
    FROM {{ source('mongo', 'logistics_orders_daily_snapshot') }}
    WHERE
        finance.payer = 4
        AND replacementOrderNumber IS NULL
),

jloc_data AS (
    SELECT
        t,
        order_number,
        channel_id,
        total_cost
    FROM {{ source('logistics', 'jl_order_cost_base') }}
    WHERE
        legal_entity = 'all'
        AND metric_type = 'expected'
        AND time_unit_type = 'order_created'
        AND counterparty = 'JMS'
),

currency_rate AS (
    SELECT
        currency_code,
        rate,
        effective_date,
        next_effective_date
    FROM {{ source('mart', 'dim_pair_currency_rate') }}
    WHERE currency_code_to = 'USD'
),

add_join AS (
    SELECT
        a.online_order_id IS NOT NULL AS is_online_order,
        jloc.order_number,
        jloc.total_cost / et.order_count_in_jl_order AS total_cost_raw,
        a.friendly_order_id,
        et.order_count_in_jl_order,
        COALESCE(IF(a.order_status IN ('cancelledByJL', 'cancelledByMerchant', 'refunded'), 0, a.logistics_revenue_amount * cr.rate), 0) AS jms_logistics_revenue_final,
        a.logistics_revenue_amount * cr.rate AS jms_logistics_revenue_initial
    FROM {{ ref('jms_orders') }} AS a
    LEFT JOIN logistics_exploded_items AS et
        ON a.friendly_order_id = et.friendly_order_id
    LEFT JOIN jloc_data AS jloc
        ON et.order_number = jloc.order_number
    LEFT JOIN currency_rate AS cr ON
        a.logistics_revenue_currency = cr.currency_code
        AND a.order_date_utc > cr.effective_date
        AND a.order_date_utc <= cr.next_effective_date
),

add_cost AS (
    SELECT
        *,
        COALESCE(
            CASE
                WHEN NOT is_online_order THEN jms_logistics_revenue_final
                ELSE total_cost_raw
            END, 0
        ) AS jl_cost
    FROM add_join
),

logistics_res AS (
    SELECT
        friendly_order_id,
        jms_logistics_revenue_initial,
        jms_logistics_revenue_final,
        jl_cost
    FROM add_cost
)

SELECT *
FROM logistics_res