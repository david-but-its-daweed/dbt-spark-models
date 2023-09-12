{{ config(
    schema='onfy',
    materialized='table',
    meta = {
      'model_owner' : '@annzaychik',
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': 'onfy-etl-monitoring'
    }
) }}

with products as 
(
    select distinct
        product.id as product_id,
        manufacturer.name as manufacturer_name,
        product.name as product_name,
        medicine.quantity,
        medicine.unit
    from {{ source('pharmacy_landing', 'product') }} as product
    join {{ source('pharmacy_landing', 'manufacturer') }} as manufacturer
        on product.manufacturer_id = manufacturer.id
    left join {{ source('pharmacy_landing', 'medicine') }} as medicine
        on translate(product.id, "-", '') = medicine.id

)

select *,
    row_number() over (partition by manufacturer_name order by visits desc) as rn
from 
(
    select 
        date_trunc('month', event_ts_cet) as visit_month,
        payload.pzn as pzn,
        products.manufacturer_name,
        products.product_name,
        concat(products.quantity, " ", products.unit) as package_size,
        count(device_id) as visits,
        count(distinct device_id) as users
    from onfy_mart.device_events as events
    join products
        on events.payload.productId = products.product_id
    where 1=1
        and type = 'productOpen'
    group by 
        date_trunc('month', event_ts_cet),
        payload.pzn,
        products.manufacturer_name,
        products.product_name,
        concat(products.quantity, " ", products.unit)
    order by
        products.manufacturer_name,
        count(device_id) desc
) as t
