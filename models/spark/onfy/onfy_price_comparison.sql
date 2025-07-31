{{ config(
    schema='onfy',
    file_format='delta',
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    meta = {
      'model_owner' : '@annzaychik',
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': '#onfy-etl-monitoring'
    }
) }}

-- calculating orders per day
with products_day as
(    
    select 
        date_trunc('day', order_created_time_cet) as order_date_cet,
        orders_info.product_id,
        orders_info.pzn,
        orders_info.store_name,
        orders_info.store_id,
        avg(orders_info.item_price) as avg_product_price,
        sum(orders_info.quantity) as sum_quantity,
        avg(dim_product.price) as avg_price
    FROM {{ ref('orders_info') }}
    INNER JOIN {{ source('onfy_mart', 'dim_product') }}
        on orders_info.product_id = dim_product.product_id
        and orders_info.store_id = dim_product.store_id
        and orders_info.order_created_time_cet between from_utc_timestamp(dim_product.effective_ts, 'Europe/Berlin') 
            and from_utc_timestamp(dim_product.next_effective_ts, 'Europe/Berlin')
    group by 
        date_trunc('day', order_created_time_cet),
        orders_info.product_id,
        orders_info.pzn,
        orders_info.store_name,
        orders_info.store_id
),

-- calculating order per month (I take only 7 days of the month to make the comparison)
products_month as 
(
    select 
        date_trunc('month', order_date_cet) as order_month_cet,
        product_id,
        pzn,
        store_name,
        store_id,
        avg(avg_product_price) as avg_product_price,
        avg(sum_quantity) as sum_quantity,
        avg(avg_price) as avg_price
    from products_day
        where order_date_cet <= date_add(date_trunc('month', order_date_cet), 7)
    group by 
        date_trunc('month', order_date_cet),
        product_id,
        pzn,
        store_name,
        store_id
),

-- calculating order per quarter (I take first 10 days of the quarter to make the comparison)
products_quarter as 
(
    select 
        date_trunc('quarter', order_date_cet) as order_quarter_cet,
        product_id,
        pzn,
        store_name,
        store_id,
        avg(avg_product_price) as avg_product_price,
        avg(sum_quantity) as sum_quantity,
        avg(avg_price) as avg_price
    from products_day
        where order_date_cet <= date_add(date_trunc('quarter', order_date_cet), 10)
    group by 
        date_trunc('quarter', order_date_cet),
        product_id,
        pzn,
        store_name,
        store_id
),

-- calculating order per quarter (I take first 15 days of the year to make the comparison and to make sure I have data points for more PZNs)
products_year as 
(
    select 
        date_trunc('year', order_date_cet) as order_year_cet,
        product_id,
        pzn,
        store_name,
        store_id,
        avg(avg_product_price) as avg_product_price,
        avg(sum_quantity) as sum_quantity,
        avg(avg_price) as avg_price
    from products_day
        where order_date_cet <= date_add(date_trunc('year', order_date_cet), 15)
    group by 
        date_trunc('year', order_date_cet),
        product_id,
        pzn,
        store_name,
        store_id
),

-- here I'm making sure that the early days of periods to compare are compared with previous, and not current, period.
-- otherwise we would get no sense from the data in the early days of the periods.
table_prep as 
(
    select 
        'previous_day' as comparison_type,
        date_add(order_date_cet, -1) as order_comparison_date,
        *
    from products_day
    union all
    select 
        'month' as comparison_type,
         date_trunc('month', if(order_date_cet <= date_add(date_trunc('month', order_date_cet), 7), date_add(order_date_cet, -8), order_date_cet)) as order_comparison_date,
        *
    from products_day
    union all
    select 
        'quarter' as comparison_type,
        date_trunc('quarter', if(order_date_cet <= date_add(date_trunc('quarter', order_date_cet), 10), date_add(order_date_cet, -11), order_date_cet)) as order_comparison_date,
        *
    from products_day
    union all
    select 
        'year' as comparison_type,
        date_trunc('year', if(order_date_cet <= date_add(date_trunc('year', order_date_cet), 15), date_add(order_date_cet, -16), order_date_cet)) as order_comparison_date, 
        *
    from products_day
),

-- calculating the ratio for the prices compared to previous periods
price_dynamics as 
(
    select 
        table_prep.*,
        
        round(coalesce(previous_day.avg_price, month.avg_price, quarter.avg_price, year.avg_price), 2) as price_to_compare,
        coalesce(round(table_prep.avg_price / coalesce(previous_day.avg_price, month.avg_price, quarter.avg_price, year.avg_price) - 1, 4), 0) as price_comparison,
        round(coalesce(previous_day.sum_quantity, month.sum_quantity, quarter.sum_quantity, year.sum_quantity), 2) as quantity_comparison,
        sum(table_prep.sum_quantity) over (partition by comparison_type, table_prep.order_date_cet) as total_period_quantity
    from 
        table_prep 
        left join products_day as previous_day
            on table_prep.comparison_type = 'previous_day'
            and table_prep.order_comparison_date = previous_day.order_date_cet
            and table_prep.pzn = previous_day.pzn
            and table_prep.store_name = previous_day.store_name
        left join products_month as month
            on table_prep.comparison_type = 'month'
            and table_prep.order_comparison_date = month.order_month_cet
            and table_prep.pzn = month.pzn
            and table_prep.store_name = month.store_name
        left join products_quarter as quarter
            on table_prep.comparison_type = 'quarter'
            and table_prep.order_comparison_date = quarter.order_quarter_cet
            and table_prep.pzn = quarter.pzn
            and table_prep.store_name = quarter.store_name
        left join products_year as year
            on table_prep.comparison_type = 'year'
            and table_prep.order_comparison_date = year.order_year_cet
            and table_prep.pzn = year.pzn
            and table_prep.store_name = year.store_name
)

select 
    *
from price_dynamics
where 1=1 
