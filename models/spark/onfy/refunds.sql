{{ config(
    schema='onfy',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@annzaychik',
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': '#onfy-etl-monitoring'
    }
) }}

WITH product_names AS (
    SELECT DISTINCT
        product_id,
        product_name
    FROM 
        {{ source('onfy_mart', 'dim_product') }}
)

SELECT
    refund_order.id AS refund_order_id,
    refund_order.order_id AS order_id,
    FROM_UTC_TIMESTAMP(refund_order.created, 'Europe/Berlin') AS refund_time_cet,
    refund_order.status,
    refund_order.reason,
    refund_order.comment,
    refund_parcel.order_parcel_id AS parcel_id,
    COALESCE(refund_parcel.delivery_refund_price, 0) AS delivery_refund_price,
    order_parcel_item.product_id AS product_id,
    medicine.country_local_id AS pzn,
    product_names.product_name,
    COALESCE(refund_item.quantity, 0) AS refunded_items_quantity,
    COALESCE(refund_item.initial_item_price, 0) AS refunded_item_price,
    COALESCE(refund_item.item_refund_price, 0) AS refunded_item_amount,
    store.name as store_name,
    CASE 
        WHEN checkout.type = 'PAY_PAL' THEN 'PAY_PAL_EXPRESS'
        ELSE order.payment_method
    END AS payment_method
FROM 
    {{ source('pharmacy_landing', 'refund') }} AS refund_order
    LEFT JOIN {{ source('pharmacy_landing', 'refund_order_parcel') }} AS refund_parcel
        ON refund_order.id = refund_parcel.refund_id
    LEFT JOIN {{ source('pharmacy_landing', 'refund_order_parcel_item') }} AS refund_item
        ON refund_item.refund_id = refund_order.id
        AND refund_item.type = 'PRODUCT'
    LEFT JOIN {{ source('pharmacy_landing', 'order_parcel_item') }} AS order_parcel_item
        ON order_parcel_item.id = refund_item.order_parcel_item_id
    LEFT JOIN {{ source('pharmacy_landing', 'medicine') }}
        ON order_parcel_item.product_id = medicine.id
    LEFT JOIN product_names
        ON product_names.product_id = order_parcel_item.product_id
    LEFT JOIN {{ source('pharmacy_landing', 'order_parcel') }} as order_parcel
        ON order_parcel.id = refund_parcel.order_parcel_id
    LEFT JOIN {{ source('pharmacy_landing', 'store') }} as store
        ON store.id = order_parcel.store_id
    LEFT JOIN {{ source('pharmacy_landing', 'order') }} as order
        ON order.id = order_parcel.order_id
    LEFT JOIN {{ source('pharmacy_landing', 'checkout') }} as checkout
        ON checkout.id = order.checkout_id
WHERE refund_order.final_refunded_amount_price > 0
