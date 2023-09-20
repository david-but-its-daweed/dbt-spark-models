{{
  config(
    materialized='incremental',
    alias='order_groups',
    schema='gold',
    file_format='delta',
    meta = {
        'model_owner' : '@gusev'
    },
    incremental_strategy='merge',
    unique_key=[
        'order_group_id',
        'real_user_id',
        'user_id',
        'order_date_msk',
        'legal_entity',
        'app_entity',
        'currency_code'
    ],
    partition_by=['month'],
    incremental_predicates=["DBT_INTERNAL_DEST.month >= trunc(current_date() - interval 230 days, 'MM')"]
  )
}}

WITH ditinct_ids AS (
    select
        order_group_id,
        device_id,
        real_user_id,
        user_id,
        min(order_datetime_utc) as order_datetime_utc
    from {{ ref('gold_orders') }}
    group by 1, 2, 3, 4
),

numbers as (
    select
        order_group_id,
        device_id,
        real_user_id,
        user_id,
        ROW_NUMBER() OVER (PARTITION BY device_id ORDER BY order_datetime_utc) AS device_order_groups_number,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY order_datetime_utc) AS user_order_groups_number,
        ROW_NUMBER() OVER (PARTITION BY real_user_id ORDER BY order_datetime_utc) AS real_user_order_groups_number
    from ditinct_ids
),

order_groups AS (
    SELECT
        order_group_id,
        real_user_id,
        user_id,
        order_date_msk,
        legal_entity,
        app_entity,
        currency_code,

        FIRST_VALUE(platform) AS platform,
        FIRST_VALUE(country_code) AS country_code,
        FIRST_VALUE(top_country_code) AS top_country_code,
        FIRST_VALUE(region_name) AS region_name,

        FIRST_VALUE(device_id) AS device_id,
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
    {% if is_incremental() %}
        where month >= trunc(date'{{ var("start_date_ymd") }}' - interval 200 days, 'MM')
    {% endif %}
    GROUP BY 1, 2, 3, 4, 5, 6, 7
)

SELECT
    og.*,
    n.device_order_groups_number,
    n.user_order_groups_number,
    n.real_user_order_groups_number,
    trunc(og.order_date_msk, 'MM') as month
FROM order_groups as og
left join numbers as n using (order_group_id, device_id, real_user_id, user_id)
