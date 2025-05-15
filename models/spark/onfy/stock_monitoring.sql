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

with pzns_list as 
(
    select distinct 
        explode(sequence(current_date(), date_add(current_date(), -366))) as report_date,
        product_id,
        pzn,
        product_name
    from {{ source('onfy', 'orders_info') }}
),

stocks_raw as (
    select
        dates.report_date,
        product.product_id,
        product.pzn,
        product.manufacturer_name,
        product.store_name,
        sum(coalesce(product.stock_quantity, 0)) as stock_quantity,
        min(product.price) as price
    from {{ source('onfy_mart', 'dim_product') }} as product
    inner join (select explode(sequence(current_date(), date_add(current_date(), -366))) as report_date) as dates
        on dates.report_date >= date(date_format(product.effective_ts, 'yyyy-mm-dd hh:mm:ss'))
        and dates.report_date < date(date_format(product.next_effective_ts, 'yyyy-mm-dd hh:mm:ss'))
    where
        date(product.effective_ts) >= current_date() - interval 366 days
        and product.store_state = 'default'
        and product.product_state = 'default'
    group by 
        dates.report_date,
        product.product_id,
        product.pzn,
        product.manufacturer_name,
        product.store_name
),

orders_daily as (
    select
        pzns_list.report_date as event_date,
        pzns_list.pzn,
        pzns_list.product_name,
        pzns_list.product_id,
        pharmacy_landing_product.ean,
        coalesce(sum(orders_info.quantity), 0) as quantity
    from pzns_list
    join {{ source('pharmacy_landing', 'product') }} as pharmacy_landing_product
        on pharmacy_landing_product.id = pzns_list.product_id
    left join {{ source('onfy', 'orders_info') }}
        on pzns_list.product_id = orders_info.product_id
        and pzns_list.report_date = date(orders_info.order_created_time_cet)
        and orders_info.order_created_time_cet >= current_date() - interval 366 days
    group by 
        pzns_list.report_date,
        pzns_list.pzn,
        pzns_list.product_name,
        pzns_list.product_id,
        pharmacy_landing_product.ean
),

rolling_sum as (
    select
        orders_daily.*,
        sum(quantity) over (partition by pzn order by event_date rows between 29 preceding and current row) as rolling_sum_quantity
    from orders_daily
),

popularity_ranks_orders as (
    select
        *,
        rank() over (partition by event_date order by rolling_sum_quantity desc) as popularity_rank
    from rolling_sum
)

select
    orders.event_date,
    orders.product_id,
    orders.pzn,
    orders.product_name,
    orders.quantity as items_quantity,
    orders.rolling_sum_quantity as 30d_items_quantity,
    orders.popularity_rank,
    orders.ean,
    stocks.manufacturer_name,
    stocks.store_name,
    coalesce(stocks.stock_quantity, 0) as stock_quantity,
    stocks.price as item_price,
    if(orders.pzn is null, '1', '0') as is_non_pzn
from popularity_ranks_orders as orders
left join stocks_raw as stocks
    on orders.event_date = stocks.report_date
    and orders.product_id = stocks.product_id
