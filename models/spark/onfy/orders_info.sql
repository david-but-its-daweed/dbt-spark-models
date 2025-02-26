{{ config(
    schema='onfy',
    materialized='table',
    file_format='parquet',
    partition_by=['partition_date'],
    meta = {
      'model_owner' : '@annzaychik',
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': '#onfy-etl-monitoring',
      'bigquery_partitioning_date_column': 'partition_date',
      'bigquery_overwrite': 'true',
      'bigquery_fail_on_missing_partitions': 'false'
    }
) }}

WITH product_names_cte AS (
    SELECT DISTINCT
        product_id,
        product_name
    FROM {{ source('onfy_mart', 'dim_product') }}
),

devices_mart_cte AS (
    SELECT
        orders.device_id,
        CASE
            WHEN device.app_type = 'WEB' AND device.device_type = 'DESKTOP' THEN 'web_desktop'
            WHEN device.app_type = 'WEB' AND device.device_type IN ('PHONE', 'TABLET') THEN 'web_mobile'
            WHEN device.app_type = 'ANDROID' THEN 'android'
            WHEN device.app_type = 'IOS' THEN 'ios'
            ELSE device.app_type || '_' || device.device_type
        END AS app_device_type,
        MIN(orders.created) AS min_purchase_ts
    FROM {{ source('pharmacy_landing', 'order') }} AS orders
    INNER JOIN {{ source('pharmacy_landing', 'device') }} AS device
        ON device.id = orders.device_id
    GROUP BY 1, 2
)

SELECT
    ord.user_id,
    ord.device_id,
    ord.user_email_hash,
    devices_mart_cte.app_device_type,
    CASE
        WHEN FROM_UTC_TIMESTAMP(ord.created, 'Europe/Berlin') > devices_mart_cte.min_purchase_ts THEN 1
        ELSE 0
    END AS is_buyer,
    DENSE_RANK() OVER (PARTITION BY ord.user_email_hash ORDER BY FROM_UTC_TIMESTAMP(ord.created, 'Europe/Berlin') ASC) AS purchase_num,
    ord.id AS order_id,
    FROM_UTC_TIMESTAMP(ord.created, 'Europe/Berlin') AS order_created_time_cet,
    CAST(FROM_UTC_TIMESTAMP(ord.created, 'Europe/Berlin') AS DATE) AS partition_date,
    ord.city,
    ord.payment_method,
    order_parcel.id AS parcel_id,
    order_parcel.status AS parcel_status,
    order_parcel_delivery.status AS parcel_delivery_status,
    order_parcel.store_id,
    store.name AS store_name,
    store_delivery.express,
    manufacturer.name AS manufacturer_name,
    manufacturer.short_name AS manufacturer_short_name,
    COALESCE(order_paketshop.order_id IS NOT NULL, FALSE) AS is_packetshop,
    order_parcel.delivery_price AS parcel_delivery_price,
    order_parcel_item.product_id,
    medicine.country_local_id AS pzn,
    product_names_cte.product_name,
    order_parcel_item.quantity,
    CAST(order_parcel_item.price AS DOUBLE) AS item_price,
    CAST(order_parcel_item.price * order_parcel_item.quantity AS DOUBLE) AS products_price,
    COALESCE(price_applier_applied_for_object.before_price, order_parcel_item.price) AS before_item_price,
    COALESCE(price_applier_applied_for_object.before_price, order_parcel_item.price) * order_parcel_item.quantity AS before_products_price,
    COALESCE(price_applier_applied_for_object.before_price - price_applier_applied_for_object.after_price, 0) AS item_discount,
    COALESCE((price_applier_applied_for_object.before_price - price_applier_applied_for_object.after_price), 0) * order_parcel_item.quantity AS products_discount
FROM {{ source('pharmacy_landing', 'order') }} AS ord
INNER JOIN {{ source('pharmacy_landing', 'order_parcel') }} AS order_parcel
    ON ord.id = order_parcel.order_id
LEFT JOIN {{ source('pharmacy_landing', 'order_parcel_delivery') }} AS order_parcel_delivery
    ON order_parcel.id = order_parcel_delivery.id
LEFT JOIN {{ source('pharmacy_landing', 'order_parcel_item') }} AS order_parcel_item
    ON
        order_parcel.id = order_parcel_item.order_parcel_id
        AND order_parcel_item.type = 'PRODUCT'
LEFT JOIN {{ source('pharmacy_landing', 'product') }} AS product
    ON order_parcel_item.product_id = product.id
LEFT JOIN {{ source('pharmacy_landing', 'manufacturer') }} AS manufacturer
    ON product.manufacturer_id = manufacturer.id
LEFT JOIN {{ source('pharmacy_landing', 'store') }} AS store
    ON
        store.id = order_parcel.store_id
INNER JOIN {{ source('pharmacy_landing', 'store_delivery') }} AS store_delivery
    ON
        store.id = store_delivery.store_id
LEFT JOIN {{ source('pharmacy_landing', 'price_applier_applied_for_object') }} AS price_applier_applied_for_object
    ON
        price_applier_applied_for_object.object_id = order_parcel_item.id
        AND price_applier_applied_for_object.object_type = 'ORDER_PARCEL_ITEM'
        AND price_applier_applied_for_object.applier_type = 'DISCOUNT_BY_TOKEN'
LEFT JOIN {{ source('pharmacy_landing', 'medicine') }} AS medicine
    ON
        order_parcel_item.product_id = medicine.id
INNER JOIN devices_mart_cte
    ON
        ord.device_id = devices_mart_cte.device_id
INNER JOIN product_names_cte
    ON
        product_names_cte.product_id = order_parcel_item.product_id
LEFT JOIN {{ source('pharmacy_landing', 'order_paketshop') }} AS order_paketshop
    ON
        ord.id = order_paketshop.order_id
DISTRIBUTE BY partition_date
