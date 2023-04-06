{{ config(
    schema='pharmacy',
    materialized='table',
    meta = {
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': 'onfy-etl-monitoring'
    }
) }}

with sessions as 
(
    select distinct
        device_events.payload.params.utm_source as utm_source,
        coalesce(if(split(device_events.payload.link, '/')[2] like '%?utm%', left(split(device_events.payload.link, '/')[2], 8), split(device_events.payload.link, '/')[2]), 
        device_events.payload.link) as pzn,
        device_id,
        event_ts_cet as session_ts_cet,
        event_id as event_id,
        lead(event_ts_cet) over (partition by device_id order by event_ts_cet) as next_session_ts_cet
    from 
        {{ source('onfy_mart', 'device_events') }} as device_events
     where 1=1
        and device_events.partition_date_cet >= '2023-02-22'
        and device_events.payload.params.utm_source like '%idealo%'
        and device_events.payload.link is not null
),

-- Mark's calculations, should change to single table once it's in production

order_data as 
(
    select
        pharmacy_landing.order.user_email_hash,
        pharmacy_landing.order.device_id,
        pharmacy_landing.order.id as order_id,
        pharmacy_landing.order.payment_method,
        from_utc_timestamp(pharmacy_landing.order.created, 'Europe/Berlin') as order_created_time_cet,
        date_trunc('DAY', from_utc_timestamp(pharmacy_landing.order.created, 'Europe/Berlin')) as order_created_date_cet,
        pharmacy_landing.order.service_fee_price,
        coalesce(pharmacy_landing.refund.service_fee_refund_price, 0) as service_fee_refund_price,
        sum(
            cast(
                pharmacy_landing.order_parcel_item.price * pharmacy_landing.order_parcel_item.quantity 
                - coalesce(pharmacy_landing.order_parcel_item.total_price_after_discount_price, pharmacy_landing.order_parcel_item.price * pharmacy_landing.order_parcel_item.quantity) 
            as double) 
        ) as promocode_discount
    from 
        {{ source('pharmacy_landing', 'order') }}
        join {{ source('pharmacy_landing', 'order_parcel') }}
            on pharmacy_landing.order.id = pharmacy_landing.order_parcel.order_id    
        join {{ source('pharmacy_landing', 'order_parcel_item') }}
            on pharmacy_landing.order_parcel_item.order_parcel_id = pharmacy_landing.order_parcel.id
            and pharmacy_landing.order_parcel_item.product_id is not null
        left join {{ source('pharmacy_landing', 'refund') }}
            on pharmacy_landing.refund.order_id = pharmacy_landing.order.id
    where
        from_utc_timestamp(pharmacy_landing.order.created, 'Europe/Berlin') >= '2023-02-22'
    group by 
        pharmacy_landing.order.user_email_hash,
        pharmacy_landing.order.device_id,
        pharmacy_landing.order.id,
        pharmacy_landing.order.payment_method,
        order_created_time_cet,
        order_created_date_cet,
        pharmacy_landing.order.service_fee_price,
        service_fee_refund_price
),

billing_data as (
    select
        pharmacy_landing.order_parcel.order_id,
        coalesce(sum(case when pharmacy_landing.billing_operation.type = 'PAYMENT' then pharmacy_landing.billing_operation.price else 0 end), 0) as products_price,
        coalesce(sum(case when pharmacy_landing.billing_operation.type = 'PAYMENT_VAT' then pharmacy_landing.billing_operation.price else 0 end), 0) as products_price_vat,
        coalesce(sum(case when pharmacy_landing.billing_operation.type = 'ORDER_REVERSAL' then pharmacy_landing.billing_operation.price else 0 end), 0) as products_price_refund,
        coalesce(sum(case when pharmacy_landing.billing_operation.type = 'ORDER_REVERSAL_VAT' then pharmacy_landing.billing_operation.price else 0 end), 0) as products_price_refund_vat,
        coalesce(sum(case when pharmacy_landing.billing_operation.type = 'ORDER_SHIPMENT' then pharmacy_landing.billing_operation.price else 0 end), 0) as delivery,
        coalesce(sum(case when pharmacy_landing.billing_operation.type = 'ORDER_SHIPMENT_REV' then pharmacy_landing.billing_operation.price else 0 end), 0) as delivery_refund,
        coalesce(sum(case when pharmacy_landing.billing_operation.type = 'COMMISSION' then pharmacy_landing.billing_operation.price else 0 end), 0) as commission,
        coalesce(sum(case when pharmacy_landing.billing_operation.type = 'COMMISSION_VAT' then pharmacy_landing.billing_operation.price else 0 end), 0) as commission_vat,
        coalesce(sum(case when pharmacy_landing.billing_operation.type = 'COMMISSION_REVERSAL' then pharmacy_landing.billing_operation.price else 0 end), 0) as commission_refund,
        coalesce(sum(case when pharmacy_landing.billing_operation.type = 'COMMISSION_REVERSAL_VAT' then pharmacy_landing.billing_operation.price else 0 end), 0) as commission_refund_vat
    from
        {{ source('pharmacy_landing', 'billing_operation') }}
        join {{ source('pharmacy_landing', 'order_parcel') }}
            on pharmacy_landing.billing_operation.order_parcel_id = pharmacy_landing.order_parcel.id 
    group by 
        pharmacy_landing.order_parcel.order_id
),

order_billing as (
    select
        order_data.user_email_hash,
        order_data.device_id,
        order_data.order_id,
        order_data.payment_method, 
        order_data.order_created_time_cet,
        order_data.order_created_date_cet,
        order_data.service_fee_price,
        order_data.service_fee_refund_price,
        order_data.promocode_discount,
        billing_data.products_price,
        billing_data.products_price_vat,
        billing_data.products_price_refund,
        billing_data.products_price_refund_vat,
        billing_data.delivery,
        billing_data.delivery_refund,
        billing_data.commission,
        billing_data.commission_vat,
        billing_data.commission_refund,
        billing_data.commission_refund_vat
    from
        billing_data
        join order_data
            on order_data.order_id = billing_data.order_id
),

gmv as 
(
    select
        user_email_hash,
        device_id,
        order_id,
        payment_method,
        order_created_time_cet,
        order_created_date_cet,
        service_fee_price,
        service_fee_price / 1.19 * 0.19 as service_fee_vat,
        service_fee_refund_price,
        service_fee_refund_price / 1.19 * 0.19 as service_fee_refund_vat,
        products_price,
        products_price_vat,
        products_price_refund,
        products_price_refund_vat,
        delivery,
        delivery_refund,
        commission,
        commission_vat,
        commission_refund,
        commission_refund_vat,
        promocode_discount,
        products_price + delivery - promocode_discount + service_fee_price as gmv_initial,
        products_price_refund + delivery_refund + service_fee_refund_price as refund,
        (products_price + delivery - promocode_discount + service_fee_price)
            - (products_price_refund + delivery_refund + service_fee_refund_price) as gmv_final,
        case
            when payment_method = 'PAY_PAL' then 0.35
            when payment_method = 'CARD'    then 0.05
            when payment_method = 'GIROPAY' then 0.22
            when payment_method = 'SOFORT'  then 0.22
            else 0
        end as psp_commission_fix,
        case
            when payment_method = 'PAY_PAL' then 0.023 
            when payment_method = 'CARD'    then 0.01
            when payment_method = 'GIROPAY' then 0.0205
            when payment_method = 'SOFORT'  then 0.0205
            else cast(null as double)
        end as psp_commission_perc
    from 
        order_billing
),

psp_commission as
(
    select 
        user_email_hash,
        device_id,
        order_id,
        payment_method,
        order_created_time_cet,
        order_created_date_cet,
        service_fee_price,
        service_fee_vat,
        service_fee_refund_price,
        service_fee_refund_vat,
        products_price,
        products_price_vat,
        products_price_refund,
        products_price_refund_vat,
        delivery,
        delivery_refund,
        commission,
        commission_vat,
        commission_refund,
        commission_refund_vat,
        promocode_discount,
        gmv_initial,
        refund,
        gmv_final,
        psp_commission_fix,
        psp_commission_perc,
        psp_commission_fix + psp_commission_perc * gmv_initial as psp_commission_initial,
        case 
            when refund > 0 then 2 * psp_commission_fix + psp_commission_perc * (gmv_initial + refund)
            else psp_commission_fix + psp_commission_perc * gmv_initial
        end as psp_commission_final
    from 
        gmv
),
   
gross_profit as 
(
    select
        psp_commission.user_email_hash,
        device_id,
        order_id,
        payment_method,
        order_created_time_cet,
        order_created_date_cet,
        service_fee_price,
        service_fee_vat,
        service_fee_refund_price,
        service_fee_refund_vat,
        products_price,
        products_price_vat,
        products_price_refund,
        products_price_refund_vat,
        delivery,
        delivery_refund,
        commission,
        commission_vat,
        commission_refund,
        commission_refund_vat,
        promocode_discount,
        gmv_initial,
        refund,
        gmv_final,
        psp_commission_fix,
        psp_commission_perc,
        psp_commission_initial,
        psp_commission_final,

        commission - commission_vat + service_fee_price - service_fee_vat - promocode_discount
        - psp_commission_initial as gross_profit_orders_initial, 

        commission - commission_vat + service_fee_price - service_fee_vat - promocode_discount
        - (commission_refund - commission_refund_vat + service_fee_refund_price - service_fee_refund_vat)
        - psp_commission_final as gross_profit_orders_final
    from 
        psp_commission
)

-- end of Mark's calculations. will be replaced when the financial dataset comes into prod

select
    utm_source,
    pzn,
    count(distinct sessions.event_id) as clicks,
    count(distinct gross_profit.order_id) as payments,
    count(distinct gross_profit.order_id) / count(distinct sessions.event_id) as cr,
    count(distinct sessions.event_id) * 0.44 as cost,
    sum(products_price) as products_price_sum,
    sum(cast(promocode_discount as float)) as promocode_discount,
    sum(gross_profit_orders_final) as gross_profit_final
from 
    sessions
left join gross_profit
    on sessions.device_id = gross_profit.device_id
    and gross_profit.order_created_time_cet between sessions.session_ts_cet and coalesce(next_session_ts_cet, current_timestamp())
    and to_unix_timestamp(gross_profit.order_created_time_cet) - to_unix_timestamp(sessions.session_ts_cet) <= 86400
group by 
    utm_source,
    pzn
