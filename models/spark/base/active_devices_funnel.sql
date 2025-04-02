{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by=['month_msk'],
        on_schema_change='sync_all_columns',
        meta = {
            'model_owner' : '@general_analytics',
            'bigquery_load': 'true',
            'bigquery_partitioning_date_column': 'date_msk',
            'bigquery_upload_horizon_days': '230',
            'priority_weight': '1000',
            'full_reload_on': '6',
        }
    )
}}

WITH product_funnel AS (
    SELECT
        device_id,
        DATE(partition_date) AS date_msk,
        SUM(IF(type = "productOpen", count, 0)) AS productOpens,
        SUM(IF(type = "productToCart", count, 0)) AS productAddToCarts,
        SUM(IF(type = "productPurchase", count, 0)) AS productPurchases,
        SUM(IF(type = "productToFavorites", count, 0)) AS productToFavourites
    FROM {{ source('recom', 'context_device_counters_v6') }}
    {% if is_incremental() %}
        WHERE partition_date >= TRUNC(DATE '{{ var("start_date_ymd") }}' - INTERVAL 200 DAYS, "MM")
    {% endif %}
    GROUP BY 1, 2
),

checkout_funnel AS (
    SELECT
        device_id,
        cd.date AS date_msk,
        COUNT(cart_open_ts) AS cartOpens,
        COUNT(checkout_start_ts) AS checkoutStarts,
        COUNT(is_checkout_pmt_method_select) AS checkoutPaymentMethodSelects,
        COUNT(is_checkout_delivery_select) AS checkoutDeliverySelects
    FROM {{ source('payments', 'checkout_data') }} AS cd
    {% if is_incremental() %}
        WHERE cd.date >= TRUNC(DATE '{{ var("start_date_ymd") }}' - INTERVAL 200 DAYS, "MM")
    {% endif %}
    GROUP BY 1, 2
)

SELECT
    COALESCE(p.device_id, c.device_id) AS device_id,
    COALESCE(p.date_msk, c.date_msk) AS date_msk,
    TRUNC(COALESCE(p.date_msk, c.date_msk), "MM") AS month_msk,
    p.productOpens,
    p.productAddToCarts,
    p.productPurchases,
    p.productToFavourites,
    c.cartOpens,
    c.checkoutStarts,
    c.checkoutPaymentMethodSelects,
    c.checkoutDeliverySelects
FROM product_funnel AS p
FULL OUTER JOIN checkout_funnel AS c USING (device_id, date_msk)
DISTRIBUTE BY month_msk, ABS(HASH(device_id)) % 10
