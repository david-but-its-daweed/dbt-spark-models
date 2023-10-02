{{
  config(
    materialized='table',
    alias='order_groups',
    schema='gold',
    file_format='delta',
    meta = {
        'model_owner' : '@gusev',
        'bigquery_load': 'true',
        'bigquery_overwrite': 'true',
        'bigquery_override_dataset_id': 'gold_migration',
        'priority_weight': '1000',
    }
  )
}}

WITH ditinct_ids AS (
    SELECT
        order_group_id,
        COALESCE(real_user_id, "__null") AS real_user_id,
        COALESCE(user_id, "__null") AS user_id,
        COALESCE(FIRST_VALUE(device_id), "__null") AS device_id,
        MIN(order_datetime_utc) AS order_datetime_utc
    FROM {{ ref('gold_orders') }}
    GROUP BY 1, 2, 3
),

numbers AS (
    SELECT
        order_group_id,
        device_id,
        real_user_id,
        user_id,
        ROW_NUMBER() OVER (PARTITION BY device_id ORDER BY order_datetime_utc) AS device_order_groups_number,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY order_datetime_utc) AS user_order_groups_number,
        ROW_NUMBER() OVER (PARTITION BY real_user_id ORDER BY order_datetime_utc) AS real_user_order_groups_number
    FROM ditinct_ids
),

order_groups AS (
    SELECT
        order_group_id,
        COALESCE(real_user_id, "__null") AS real_user_id,
        COALESCE(user_id, "__null") AS user_id,
        order_date_msk,
        legal_entity,
        app_entity,
        currency_code,

        FIRST_VALUE(platform) AS platform,
        FIRST_VALUE(country_code) AS country_code,
        FIRST_VALUE(top_country_code) AS top_country_code,
        FIRST_VALUE(region_name) AS region_name,

        COALESCE(FIRST_VALUE(device_id), "__null") AS device_id,
        FIRST_VALUE(real_user_segment) AS real_user_segment,
        MIN(is_new_device) AS is_new_device,
        MIN(device_lifetime) AS device_lifetime,
        MIN(order_datetime_utc) AS order_datetime_utc,

        COUNT(DISTINCT merchant_id) AS mechants_number,
        COUNT(DISTINCT store_id) AS stores_number,
        COUNT(DISTINCT product_id) AS products_number,
        COUNT(DISTINCT order_id) AS orders_number,

        SUM(product_quantity) AS product_quantity_total,
        SUM(gmv_initial) AS gmv_initial,
        SUM(gmv_final) AS gmv_final,
        SUM(gmv_refunded) AS gmv_refunded,
        SUM(gmv_initial_in_local_currency) AS gmv_initial_in_local_currency,
        SUM(psp_initial) AS psp_initial,
        SUM(psp_final) AS psp_final,
        SUM(order_gross_profit_final) AS order_gross_profit_final,
        SUM(order_gross_profit_final_estimated) AS order_gross_profit_final_estimated,
        SUM(ecgp_initial) AS ecgp_initial,
        SUM(ecgp_final) AS ecgp_final,
        SUM(merchant_revenue_initial) AS merchant_revenue_initial,
        SUM(merchant_revenue_final) AS merchant_revenue_final,
        SUM(merchant_sale_price) AS merchant_sale_price,
        SUM(merchant_list_price) AS merchant_list_price,
        SUM(logistics_price_initial) AS logistics_price_initial,
        SUM(vat_markup) AS vat_markup,
        SUM(marketplace_commission_initial) AS marketplace_commission_initial,
        SUM(jl_markup) AS jl_markup,
        SUM(jm_markup) AS jm_markup,
        SUM(coupon_discount) AS coupon_discount,
        SUM(points_initial) AS points_initial
    FROM {{ ref('gold_orders') }}
    GROUP BY 1, 2, 3, 4, 5, 6, 7
)

SELECT
    og.order_group_id,
    IF(og.real_user_id = "__null", NULL, og.real_user_id) AS real_user_id,
    IF(og.user_id = "__null", NULL, og.user_id) AS user_id,
    og.order_date_msk,
    og.legal_entity,
    og.app_entity,
    og.currency_code,

    og.platform,
    og.country_code,
    og.top_country_code,
    og.region_name,

    IF(og.device_id = "__null", NULL, og.device_id) AS device_id,
    og.real_user_segment,
    og.is_new_device,
    og.device_lifetime,
    og.order_datetime_utc,

    og.mechants_number,
    og.stores_number,
    og.products_number,
    og.orders_number,

    og.product_quantity_total,
    og.gmv_initial,
    og.gmv_final,
    og.gmv_refunded,
    og.gmv_initial_in_local_currency,
    og.psp_initial,
    og.psp_final,
    og.order_gross_profit_final,
    og.order_gross_profit_final_estimated,
    og.ecgp_initial,
    og.ecgp_final,
    og.merchant_revenue_initial,
    og.merchant_revenue_final,
    og.merchant_sale_price,
    og.merchant_list_price,
    og.logistics_price_initial,
    og.vat_markup,
    og.marketplace_commission_initial,
    og.jl_markup,
    og.jm_markup,
    og.coupon_discount,
    og.points_initial,

    n.device_order_groups_number,
    n.user_order_groups_number,
    n.real_user_order_groups_number
FROM order_groups AS og
INNER JOIN numbers AS n USING (order_group_id, device_id, real_user_id, user_id)
