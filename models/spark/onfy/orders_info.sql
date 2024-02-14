{{ config(
    schema='onfy',
    materialized='table',
    meta = {
      'model_owner' : '@easaltykova',
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': 'onfy-etl-monitoring'
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
        order.device_id,
        order.user_id,
        order.user_email_hash,
        CASE
            WHEN device.app_type = 'WEB' AND device.device_type = 'DESKTOP' THEN 'web_desktop'
            WHEN device.app_type = 'WEB' AND device.device_type IN ('PHONE', 'TABLET') THEN 'web_mobile'
            WHEN device.app_type = 'ANDROID' THEN 'android'
            WHEN device.app_type = 'IOS' THEN 'ios'
            ELSE device.app_type || '_' || device.device_type
        END AS app_device_type,
        order.created AS order_dt,
        MIN(order.created) OVER (PARTITION BY order.user_email_hash) AS min_purchase_ts
    FROM {{ source('pharmacy_landing', 'order') }} AS order
    INNER JOIN {{ source('pharmacy_landing', 'device') }} AS device
        ON device.id = order.device_id
)
SELECT
    ord.user_id,
    ord.device_id,
    devices_mart_cte.user_email_hash,
    devices_mart_cte.app_device_type,
    CASE
        WHEN FROM_UTC_TIMESTAMP(ord.created, 'Europe/Berlin') > devices_mart_cte.min_purchase_ts THEN 1
        ELSE 0
    END AS is_buyer,
    DENSE_RANK() OVER (PARTITION BY devices_mart_cte.user_email_hash ORDER BY FROM_UTC_TIMESTAMP(ord.created, 'Europe/Berlin') ASC) AS purchase_num,
    ord.id AS order_id,
    FROM_UTC_TIMESTAMP(ord.created, 'Europe/Berlin') AS order_created_time_cet,
    ord.city,
    ord.payment_method,
    order_parcel.id AS parcel_id,
    order_parcel.store_id,
    store.name AS store_name,
    store_delivery.express,
    COALESCE(order_paketshop.order_id IS NOT NULL, FALSE) AS is_packetshop,
    order_parcel.delivery_price AS parcel_delivery_price,
    order_parcel_item.product_id,
    medicine.country_local_id AS pzn,
    product_names_cte.product_name,
    order_parcel_item.quantity,
    CAST(order_parcel_item.price AS DOUBLE) AS item_price,
    CAST(order_parcel_item.price * order_parcel_item.quantity AS DOUBLE) AS products_price
FROM {{ source('pharmacy_landing', 'order') }} AS ord
INNER JOIN {{ source('pharmacy_landing', 'order_parcel') }} AS order_parcel
    ON ord.id = order_parcel.order_id
LEFT JOIN {{ source('pharmacy_landing', 'order_parcel_item') }} AS order_parcel_item
    ON
        order_parcel.id = order_parcel_item.order_parcel_id
        AND order_parcel_item.type = 'PRODUCT'
LEFT JOIN {{ source('pharmacy_landing', 'store') }} AS store
    ON
        store.id = order_parcel.store_id
INNER JOIN {{ source('pharmacy_landing', 'store_delivery') }} AS store_delivery
    ON
        store.id = store_delivery.store_id
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