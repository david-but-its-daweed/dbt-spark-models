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

SELECT 
    order.user_id,
    order.device_id,
    devices_mart.app_device_type,
    CASE
        WHEN from_utc_timestamp(order.created, 'Europe/Berlin') > devices_mart.min_purchase_ts THEN True
        ELSE False
        END AS is_buyer,
             
    order.id AS order_id,
    from_utc_timestamp(order.created, 'Europe/Berlin') AS order_created_time_cet,
    order.city,
    order.payment_method,
             
    order_parcel.id AS parcel_id,
    order_parcel.store_id,
    store.name AS store_name,
    store_delivery.express,
    order_parcel.delivery_price AS parcel_delivery_price,
             
    order_parcel_item.product_id,
    medicine.country_local_id AS pzn,
    order_parcel_item.quantity,
    cast(order_parcel_item.price AS double) AS item_price,
    cast(order_parcel_item.price * order_parcel_item.quantity as double) AS products_price
FROM {{ source('pharmacy_landing', 'order') }} AS order
JOIN {{ source('pharmacy_landing', 'order_parcel') }} AS order_parcel
    ON order.id = order_parcel.order_id
LEFT JOIN {{ source('pharmacy_landing', 'order_parcel_item') }} AS order_parcel_item
    ON order_parcel.id = order_parcel_item.order_parcel_id
    AND order_parcel_item.type = 'PRODUCT'
             
LEFT JOIN {{ source('pharmacy_landing', 'store') }} AS store
    ON store.id = order_parcel.store_id
JOIN {{ source('pharmacy_landing', 'store_delivery') }} AS store_delivery
    ON store.id = store_delivery.store_id
             
LEFT JOIN {{ source('pharmacy_landing', 'medicine') }} medicine
    ON order_parcel_item.product_id = medicine.id
             
JOIN {{ source('onfy_mart', 'devices_mart') }} AS devices_mart
    ON order.device_id = devices_mart.device_id
