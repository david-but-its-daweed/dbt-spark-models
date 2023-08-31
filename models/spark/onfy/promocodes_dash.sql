{{ config(
    schema='onfy',
    materialized='table',
    meta = {
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': 'onfy-etl-monitoring'
    }
) }}

with promo_applications as 
(
    select distinct
        order.id as order_id,
        order.checkout_id,
        from_utc_timestamp(order.created, 'Europe/Berlin') as order_created_cet,
        promocode.id as promocode_id,
        promocode.type as promocode_type,
        promocode.name as promocode_name,
        promocode.discount_value_price,
        promocode.minimal_order_sum_price,
        from_utc_timestamp(promocode.valid_until, 'Europe/Berlin') as promocode_valid_until_date_cet,
        from_utc_timestamp(promocode.created, 'Europe/Berlin') as promocode_created_date_cet, 
        from_utc_timestamp(promocode.updated, 'Europe/Berlin') as promocode_updated_date_cet,
        promocode.is_referral,
        promocode.discount_type,
        promocode.percent_discount_value,
        admin_user.email,
        order.user_email_hash,
        order.device_id,
        count(order_parcel.store_id) as parcels_in_order,
        sum(cast(order_parcel_item.price * order_parcel_item.quantity - 
            coalesce(order_parcel_item.total_price_after_discount_price, order_parcel_item.price * order_parcel_item.quantity) as float)) as total_order_discount,
        sum(order_parcel_item.price * order_parcel_item.quantity) as products_price_sum
    from {{ source('pharmacy_landing', 'order') }} as order
    join {{ source('pharmacy_landing', 'order_parcel') }} as order_parcel
        on order_parcel.order_id = order.id
    join {{ source('pharmacy_landing', 'order_parcel_item') }} as order_parcel_item
        on order_parcel_item.order_parcel_id = order_parcel.id
    join pharmacy.checkout_promocode as checkout_promocode
        on order.checkout_id = checkout_promocode.checkout_id
        and order.checkout_id is not null
    join {{ source('pharmacy_landing', 'promocode') }} as promocode
        on checkout_promocode.promocode_id = promocode.id
    join {{ source('pharmacy_landing', 'admin_user') }} as admin_user
        on promocode.admin_user_id = admin_user.id
    where 1=1
    group by 
        order.id,
        order.checkout_id,
        promocode.id,
        promocode.type,
        promocode.name,
        promocode.discount_value_price,
        promocode.minimal_order_sum_price,
        from_utc_timestamp(promocode.valid_until, 'Europe/Berlin'),
        from_utc_timestamp(promocode.created, 'Europe/Berlin'), 
        from_utc_timestamp(promocode.updated, 'Europe/Berlin'),
        promocode.is_referral,
        promocode.discount_type,
        promocode.percent_discount_value,
        admin_user.email,
        order.user_email_hash,
        order.device_id,
        from_utc_timestamp(order.created, 'Europe/Berlin')
)


select 
    *
from promo_applications
