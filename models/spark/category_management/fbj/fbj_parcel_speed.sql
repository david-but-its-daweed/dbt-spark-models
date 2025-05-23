{{
  config(
    file_format='delta',
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    meta = {
        'model_owner' : '@catman-analytics.duty',
        'bigquery_load': 'true'
    },
    schema='category_management',
    alias='fbj_parcel_speed',
    on_schema_change='append_new_columns'
  )
}}

WITH data AS (
    SELECT
        DATE(outbound_time_utc) AS dt,
        parcel_id,
        IF(country = 'RU', 'RU', 'nonRU') AS country_type,
        SUM(quantity) AS quantity_in_parcel, -- сколько в посылке всего штучек 
        COUNT(order_id) AS orders_in_parcel, -- сколько в посылке всего заказов
        SUM(IF(is_fbj_order, quantity, null)) AS fbj_quantity_in_parcel, -- сколько в посылке fbj штучек
        COUNT(IF(is_fbj_order, order_id, null)) AS fbj_orders_in_parcel, -- сколько в посылке fbj заказов

        MIN(order_created_time_utc) AS min_order_created_at_in_parcel,
        MAX(outbound_time_utc) AS max_outbound_time_in_parcel,
        DATE_DIFF(HOUR, MIN(order_created_time_utc), MAX(outbound_time_utc)) / 24.0 AS dif_days
    FROM {{ source('logistics_mart', 'fact_order') }} -- logistics_mart.fact_order
    WHERE
        1 = 1
        AND DATE(outbound_time_utc) >= DATE('2025-01-01')
        AND origin_country = 'CN'
    GROUP BY
        DATE(outbound_time_utc),
        parcel_id,
        IF(country = 'RU', 'RU', 'nonRU')

    UNION ALL

    SELECT
        DATE(outbound_time_utc) AS dt,
        parcel_id,
        'total' AS country_type,
        SUM(quantity) AS quantity_in_parcel,
        COUNT(order_id) AS orders_in_parcel,
        SUM(IF(is_fbj_order, quantity, null)) AS fbj_quantity_in_parcel,
        COUNT(IF(is_fbj_order, order_id, null)) AS fbj_orders_in_parcel,
        MIN(order_created_time_utc) AS min_order_created_at_in_parcel,
        MAX(outbound_time_utc) AS max_outbound_time_in_parcel,
        DATE_DIFF(HOUR, MIN(order_created_time_utc), MAX(outbound_time_utc)) / 24.0 AS dif_days
    FROM {{ source('logistics_mart', 'fact_order') }} -- logistics_mart.fact_order
    WHERE
        1 = 1
        AND DATE(outbound_time_utc) >= DATE('2025-01-01')
        AND origin_country = 'CN'
    GROUP BY
        DATE(outbound_time_utc),
        parcel_id
)

SELECT
    dt,
    country_type,
    COUNT(parcel_id) AS parcel_cnt, -- количество посылок всего
    AVG(dif_days) AS avg_dif_days, -- скорость посылок средняя
    AVG(quantity_in_parcel) AS avg_quantity_in_parcel, -- в среднем штучек в посылке
    AVG(orders_in_parcel) AS avg_orders_in_parcel, -- в среднем заказов в посылке
    --AVG(fbj_quantity_in_parcel) AS avg_fbj_quantity_in_parcel,
    --AVG(fbj_orders_in_parcel) AS avg_fbj_orders_in_parcel,
    --
    COUNT(IF(orders_in_parcel = fbj_orders_in_parcel, parcel_id, null)) AS fbj_parcel_cnt,
    AVG(IF(orders_in_parcel = fbj_orders_in_parcel, dif_days, null)) AS avg_fbj_dif_days,
    AVG(IF(orders_in_parcel = fbj_orders_in_parcel, quantity_in_parcel, null)) AS avg_quantity_in_fbj_parcel, -- в среднем штучек в fbj посылке
    AVG(IF(orders_in_parcel = fbj_orders_in_parcel, orders_in_parcel, null)) AS avg_orders_in_fbj_parcel, -- в среднем заказов в fbj посылке
    AVG(IF(orders_in_parcel = fbj_orders_in_parcel, fbj_quantity_in_parcel, null)) AS avg_fbj_quantity_in_fbj_parcel, -- в среднем штучек в fbj посылке
    AVG(IF(orders_in_parcel = fbj_orders_in_parcel, fbj_orders_in_parcel, null)) AS avg_fbj_orders_in_fbj_parcel, -- в среднем заказов в fbj посылке
    --
    COUNT(IF(orders_in_parcel <> fbj_orders_in_parcel AND fbj_orders_in_parcel > 0, parcel_id, null)) AS not_only_fbj_parcel_cnt,
    AVG(IF(orders_in_parcel <> fbj_orders_in_parcel AND fbj_orders_in_parcel > 0, dif_days, null)) AS avg_not_only_fbj_dif_days,
    AVG(IF(orders_in_parcel <> fbj_orders_in_parcel AND fbj_orders_in_parcel > 0, quantity_in_parcel, null)) AS avg_quantity_in_not_only_fbj_parcel, -- в среднем штучек в посылке
    AVG(IF(orders_in_parcel <> fbj_orders_in_parcel AND fbj_orders_in_parcel > 0, orders_in_parcel, null)) AS avg_orders_in_not_only_fbj_parcel, -- в среднем заказов в осылке
    AVG(IF(orders_in_parcel <> fbj_orders_in_parcel AND fbj_orders_in_parcel > 0, fbj_quantity_in_parcel, null)) AS avg_fbj_quantity_in_not_only_fbj_parcel, -- в среднем fbj штучек в посылке
    AVG(IF(orders_in_parcel <> fbj_orders_in_parcel AND fbj_orders_in_parcel > 0, fbj_orders_in_parcel, null)) AS avg_fbj_orders_in_not_only_fbj_parcel, -- в среднем заказов в fbj посылке
    AVG(IF(orders_in_parcel <> fbj_orders_in_parcel AND fbj_orders_in_parcel > 0, fbj_quantity_in_parcel / quantity_in_parcel, null)) AS avg_fbj_quantity_share_in_not_only_fbj_parcel,
    AVG(IF(orders_in_parcel <> fbj_orders_in_parcel AND fbj_orders_in_parcel > 0, fbj_orders_in_parcel / orders_in_parcel, null)) AS avg_fbj_order_share_in_not_only_fbj_parcel,
    --
    COUNT(IF(fbj_orders_in_parcel = 0, parcel_id, null)) AS not_fbj_parcel_cnt,
    AVG(IF(fbj_orders_in_parcel = 0, dif_days, null)) AS avg_not_fbj_dif_days,
    AVG(IF(fbj_orders_in_parcel = 0, quantity_in_parcel, null)) AS avg_quantity_in_not_fbj_parcel, -- в среднем штучек в fbj посылке
    AVG(IF(fbj_orders_in_parcel = 0, orders_in_parcel, null)) AS avg_orders_in_not_fbj_parcel--, -- в среднем заказов в fbj посылке
    --AVG(IF(fbj_orders_in_parcel = 0, fbj_quantity_in_parcel, null)) AS avg_fbj_quantity_in_not_fbj_parcel, -- в среднем штучек в fbj посылке
    --AVG(IF(fbj_orders_in_parcel = 0, fbj_orders_in_parcel, null)) AS avg_fbj_orders_in_not_fbj_parcel -- в среднем заказов в fbj посылке
FROM data
GROUP BY
    dt,
    country_type