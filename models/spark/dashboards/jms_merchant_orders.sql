{{
  config(
    materialized='table',
    file_format='parquet',
    clustered_by=['marketplace_name'],
    partition_by=['order_date_utc'],
    fail_on_missing_partitions=False,
    meta = {
        'model_owner' : '@catman-analytics.duty',
        'bigquery_load': 'true',
        'bigquery_overwrite': 'true',        
        'bigquery_partitioning_date_column': 'order_date_utc',
        'bigquery_fail_on_missing_partitions': 'false'
    }
  )
}}

WITH cr_data AS (
    SELECT
        currency_code,
        rate,
        effective_date,
        next_effective_date
    FROM {{ ref('dim_pair_currency_rate') }}
    WHERE
        currency_code_to = 'USD'
),

merchant_orders AS (
    SELECT
        marketplace_created_time,
        TRUNC(TO_DATE(marketplace_created_time), 'MM') AS order_month_utc,
        TO_DATE(marketplace_created_time) AS order_date_utc,

        merchant_id,
        store_id,
        order_id AS merchant_order_id,
        marketplace_id.marketplace AS marketplace_name,
        friendly_id AS friendly_order_id,
        tracking_number,
        online_order_id,

        product_id,
        variant_id AS product_variant_id,

        country AS country_code,
        quantity AS product_quantity,

        money_info.merchant_currency,
        money_info.customer_gmv,
        money_info.customer_vat,
        money_info.merchant_revenue,
        money_info.merchant_unit_price,

        refund.time_utc IS NULL AS is_refunded,
        TO_DATE(refund.time_utc) AS refund_date,
        refund.merchant_reason AS refund_merchant_reason,
        refund.customer_reason AS refund_customer_reason,
        is_fraud,

        cft,
        created_time_utc,
        user_ordered_time_utc,
        fulfilled_online_time_utc,
        shipped_time_utc,
        updated_time_utc,
        cancelled_by_jl_info.time_utc AS cancelled_time_utc
    FROM {{ source('mongo', 'merchant_order') }}
    WHERE
        source.kind = 'jms'
        AND TO_DATE(created_time_utc) >= '2023-02-16' -- date of first JMS order
        AND TO_DATE(created_time_utc) < CURRENT_DATE()
),

jl_orders AS (
    SELECT
        order_number,
        final_revenue_usd AS logistics_total_revenue,
        final_total_cost_usd AS logistics_total_cost
    FROM
        {{ source('logistics_mart', 'jl_fact_order') }}
    WHERE
        counterparty_customer = 'JMS'
        AND TO_DATE(logistics_order_created_time_utc) >= '2023-02-16' -- date of first JMS order
        AND TO_DATE(logistics_order_created_time_utc) < CURRENT_DATE()
)

SELECT
    mo.order_month_utc,
    mo.order_date_utc,

    mo.marketplace_created_time AS order_datetime_utc,

    mo.merchant_id,
    gm.merchant_name,
    mo.store_id,
    mo.merchant_order_id,
    mo.marketplace_name,
    mo.friendly_order_id,
    mo.tracking_number,

    mo.product_id,
    mo.product_variant_id,

    gp.business_line,
    gp.merchant_category_id,
    gmc.l1_merchant_category_id,
    gmc.l1_merchant_category_name,
    gmc.l2_merchant_category_id,
    gmc.l2_merchant_category_name,
    gmc.l3_merchant_category_id,
    gmc.l3_merchant_category_name,
    gmc.l4_merchant_category_id,
    gmc.l4_merchant_category_name,
    gmc.l5_merchant_category_id,
    gmc.l5_merchant_category_name,

    gm.origin_name,
    mo.country_code,
    mo.product_quantity,

    mo.customer_gmv * cr.rate AS gmv_initial_w_vat,
    mo.customer_gmv * cr.rate - mo.customer_vat * cr.rate AS gmv_initial_wo_vat,
    mo.merchant_revenue * cr.rate AS merchant_revenue,
    mo.merchant_unit_price * cr.rate AS merchant_unit_price,

    jlo.logistics_total_revenue,
    jlo.logistics_total_cost,

    mo.is_refunded,
    mo.refund_date,
    mo.refund_merchant_reason,
    mo.refund_customer_reason,
    mo.is_fraud,

    mo.cft,
    mo.user_ordered_time_utc,
    mo.fulfilled_online_time_utc,
    mo.shipped_time_utc,
    mo.updated_time_utc,
    mo.cancelled_time_utc,
    mo.created_time_utc
FROM merchant_orders AS mo
LEFT JOIN cr_data AS cr
    ON
        mo.marketplace_created_time BETWEEN cr.effective_date AND cr.next_effective_date
        AND mo.merchant_currency = cr.currency_code
LEFT JOIN jl_orders AS jlo ON mo.online_order_id = jlo.order_number
LEFT JOIN {{ ref('gold_products') }} AS gp ON mo.product_id = gp.product_id
LEFT JOIN {{ ref('gold_merchants') }} AS gm ON mo.merchant_id = gm.merchant_id
LEFT JOIN {{ ref('gold_merchant_categories') }} AS gmc ON gp.merchant_category_id = gmc.merchant_category_id