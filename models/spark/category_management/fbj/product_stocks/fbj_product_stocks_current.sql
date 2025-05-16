{{
  config(
    file_format='delta',
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    meta = {
        'model_owner' : '@catman-analytics.duty',
        'bigquery_load': 'true'
    }
  )
}}

WITH freezed_stock AS (
    SELECT DISTINCT
        wws_ds.pvId AS variant_id,
        wws_ds.pendingStock
    FROM {{ source('mongo', 'warehouse_warehouse_stocks_daily_snapshot') }} AS wws_ds --mongo.warehouse_warehouse_stocks_daily_snapshot as wws_ds
    INNER JOIN {{ source('mongo', 'core_warehouses_daily_snapshot') }} AS cw_ds -- mongo.core_warehouses_daily_snapshot as cw_ds
        ON
            1 = 1
            AND wws_ds.whid = cw_ds._id
            AND cw_ds.enabled
            AND cw_ds.state = 1
            AND cw_ds.type = 3
    WHERE
        1 = 1
        AND wws_ds.pendingStock IS NOT NULL
        AND wws_ds.stock > 0
)

SELECT
    CURRENT_DATE() AS partition_date,
    published_variant.product_id,
    gp.business_line,
    info.externalId AS product_variant_id,
    info._id AS logistics_product_id,
    stocks.reserved AS number_of_reserved,
    stocks.stock AS number_of_products_in_stock,
    freezed_stock.pendingStock AS number_of_products_in_pending_stock,
    stocks.uTm AS stock_update_ts,
    info.brand AS brand_name,
    info.nameEn AS product_name,
    info.url AS product_url,
    info.uDimensions AS product_dimensions,
    info.merchantId AS merchant_id,
    info.uPrice.amount * currency_rate.rate / 1000000 AS original_merchant_price,
    info.uWeight AS product_weight
FROM {{ source('mongo', 'logistics_product_stocks_daily_snapshot') }} AS stocks
LEFT JOIN {{ source('mongo', 'logistics_products_v2_daily_snapshot') }} AS info
    ON stocks._id.pid = info._id
LEFT JOIN mart.dim_published_variant AS published_variant
    ON
        info.externalid = published_variant.variant_id
        AND CURRENT_DATE() BETWEEN published_variant.effective_ts AND published_variant.next_effective_ts
LEFT JOIN {{ ref('dim_pair_currency_rate') }} AS currency_rate
    ON
        info.uPrice.ccy = currency_rate.currency_code
        AND currency_rate.effective_date = CURRENT_DATE()
        AND currency_rate.currency_code_to = 'USD'
LEFT JOIN {{ ref('gold_products') }} AS gp ON published_variant.product_id = gp.product_id
LEFT JOIN freezed_stock
    ON
        1 = 1
        AND info.externalId = freezed_stock.variant_id
WHERE
    stocks._id.wid = '66719d184d731a97c6a58b28'
    AND NOT (stocks.reserved = 0 AND stocks.stock = 0)