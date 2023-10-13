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

select
    order.user_id,
    order.device_id,
    devices_mart.app_device_type,
    case
        when from_utc_timestamp(order.created, 'Europe/Berlin') > devices_mart.min_purchase_ts then True
        else False
        end as is_buyer,
             
    order.id as order_id,
    from_utc_timestamp(order.created, 'Europe/Berlin') as order_created_time_cet,
    order.city,
    order.payment_method,
             
    order_parcel.id as parcel_id,
    order_parcel.store_id,
    store.name as store_name,
    store_delivery.express,
    order_parcel.delivery_price as parcel_delivery_price,
             
    order_parcel_item.product_id,
    medicine.country_local_id as pzn,
    order_parcel_item.quantity,
    cast(order_parcel_item.price as double) as item_price,
    cast(order_parcel_item.price * order_parcel_item.quantity as double) as products_price
from {{ source('pharmacy_landing', 'order') }} as order
join {{ source('pharmacy_landing', 'order_parcel') }} as order_parcel
    on order.id = order_parcel.order_id
left join {{ source('pharmacy_landing', 'order_parcel_item') }} as order_parcel_item
    on order_parcel.id = order_parcel_item.order_parcel_id
    and order_parcel_item.type = 'PRODUCT'
             
left join {{ source('pharmacy_landing', 'store') }} as store
    on store.id = order_parcel.store_id
join {{ source('pharmacy_landing', 'store_delivery') }} as store_delivery
    on store.id = store_delivery.store_id
             
left join {{ source('pharmacy_landing', 'medicine') }} medicine
    on order_parcel_item.product_id = medicine.id
             
join {{ source('onfy_mart', 'devices_mart') }} as devices_mart
    on order.device_id = devices_mart.device_id
