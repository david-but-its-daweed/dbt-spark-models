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

with vats as 
(
    select 
        order_parcel_id,
        sum(total_price_after_discount_price / (1+vat)) as products_price_before_vat,
        sum(refunded_item_amount / (1+vat)) as products_price_refund_before_vat
    from {{source('pharmacy_landing', 'order_parcel_item')}}
    left join {{source('onfy', 'refunds')}}
        on order_parcel_item.order_parcel_id = refunds.parcel_id
        and order_parcel_item.product_id = refunds.product_id
    join {{source('pharmacy_landing', 'medicine')}}
        on medicine.id = order_parcel_item.product_id
    group by 
        order_parcel_id
), 

transactions as
(
    select 
        transactions.order_id,
        transactions.order_parcel_id,
        products_price_refund_before_vat,
        order_num,
        (sum(if(type = 'PAYMENT', price, 0)) - sum(if(type='DISCOUNT', price, 0))) as payment,
        (sum(if(type = 'ORDER_REVERSAL', price, 0)) - sum(if(type='DISCOUNT_REVERSAL', price, 0))) as reversal,
        sum(if(type = 'ORDER_REVERSAL', 1, 0)) as parcels_reversed,
        sum(if(type = 'DISCOUNT', price, 0)) as discount,
        products_price_before_vat
    from {{source('onfy', 'transactions')}}
    join vats
        on transactions.order_parcel_id = vats.order_parcel_id
    join {{source('pharmacy_landing', 'order')}}
        on order.id = transactions.order_id
    where 1=1
        and currency = 'EUR'
        and transactions.order_parcel_id is not null
    group by 
        transactions.order_id,
        purchase_num,
        products_price_refund_before_vat,
        transactions.order_parcel_id,
        products_price_before_vat,
        order_num
)

select 
    source,
    order_num,
    first_user_source,
    attributed_session_dt,
    purchase_num,
    ad.order_id,
    sum(gmv_initial) as gmv_initial,
    sum(gross_profit_initial) as gross_profit_initial,
    count(distinct order_parcel_id) as parcels,
    sum(payment) as payment,
    sum(reversal) as reversal,
    sum(parcels_reversed) as parcels_reversed,
    max(parcels_reversed) as had_reversal,
    sum(products_price_refund_before_vat) as products_price_refund_before_vat,
    sum(products_price_before_vat - coalesce(products_price_refund_before_vat, 0)) as products_price_before_vat_final,
    sum(products_price_before_vat) as products_price_before_vat,
    case
        when sum(discount) = 0 and purchase_num = 1 then 0.15
        when sum(discount) > 0 and purchase_num = 1 then 0.1
        when sum(discount) = 0 and purchase_num > 1 then 0.045
        when sum(discount) > 0 and purchase_num > 1 then 0.03
    end as comission,
    case
        when sum(discount) = 0 and purchase_num = 1 then 0.15
        when sum(discount) > 0 and purchase_num = 1 then 0.1
        when sum(discount) = 0 and purchase_num > 1 then 0.045
        when sum(discount) > 0 and purchase_num > 1 then 0.03
    end * sum(products_price_before_vat) as comission_sum
from {{source('onfy', 'ads_dashboard')}} as ad
join transactions
    on ad.order_id = transactions.order_id
where 1=1
    and source = 'awin'
group by 
    source,
    order_num,
    first_user_source,
    attributed_session_dt,
    purchase_num,
    ad.order_id
order by 
    attributed_session_dt desc
