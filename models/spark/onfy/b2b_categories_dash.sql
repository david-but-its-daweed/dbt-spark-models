{{ config(
    schema='onfy',
    materialized='table',
    file_format='parquet',
    partition_by=['order_day'],
    meta = {
      'model_owner' : '@annzaychik',
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': '#onfy-etl-monitoring',
      'bigquery_partitioning_date_column': 'order_day'
    }
) }}


--------------------------------------------------------------
-- определяем категорийное дерево для продуктов
-- если у продукта 2 или более катеогорий, остаются все категории
--------------------------------------------------------------
with product_categories as 
(
    select 
        product_id,
        category_id as product_category,
        category_1.slug as product_category_slug,
        case
            when category_1.parent_category_id is null and category_1.id is not null then category_1.id
            when category_2.parent_category_id is null and category_2.id is not null then category_2.id
            when category_3.parent_category_id is null and category_3.id is not null then category_3.id
            when category_4.parent_category_id is null and category_4.id is not null then category_4.id
            when category_5.parent_category_id is null and category_5.id is not null then category_5.id
            else null
        end as category_id_level_1,
        case
            when category_2.parent_category_id is null and category_2.id is not null then category_1.id
            when category_3.parent_category_id is null and category_3.id is not null then category_2.id
            when category_4.parent_category_id is null and category_4.id is not null then category_3.id
            when category_5.parent_category_id is null and category_5.id is not null then category_4.id
            else null
        end as category_id_level_2,
        case
            when category_3.parent_category_id is null and category_3.id is not null then category_1.id
            when category_4.parent_category_id is null and category_4.id is not null then category_2.id
            when category_5.parent_category_id is null and category_5.id is not null then category_3.id
            else null
        end as category_id_level_3, 
        case
            when category_4.parent_category_id is null and category_4.id is not null then category_1.id
            when category_5.parent_category_id is null and category_5.id is not null then category_2.id
            else null
        end as category_id_level_4, 
        case
            when category_5.parent_category_id is null and category_5.id is not null then category_1.id
            else null
        end as category_id_level_5, 
        
        case
            when category_1.parent_category_id is null and category_1.id is not null then category_1.name
            when category_2.parent_category_id is null and category_2.id is not null then category_2.name
            when category_3.parent_category_id is null and category_3.id is not null then category_3.name
            when category_4.parent_category_id is null and category_4.id is not null then category_4.name
            when category_5.parent_category_id is null and category_5.id is not null then category_5.name
            else null
        end as category_name_level_1,
        case
            when category_2.parent_category_id is null and category_2.id is not null then category_1.name
            when category_3.parent_category_id is null and category_3.id is not null then category_2.name
            when category_4.parent_category_id is null and category_4.id is not null then category_3.name
            when category_5.parent_category_id is null and category_5.id is not null then category_4.name
            else null
        end as category_name_level_2,
        case
            when category_3.parent_category_id is null and category_3.id is not null then category_1.name
            when category_4.parent_category_id is null and category_4.id is not null then category_2.name
            when category_5.parent_category_id is null and category_5.id is not null then category_3.name
            else null
        end as category_name_level_3, 
        case
            when category_4.parent_category_id is null and category_4.id is not null then category_1.name
            when category_5.parent_category_id is null and category_5.id is not null then category_2.name
            else null
        end as category_name_level_4, 
        case
            when category_5.parent_category_id is null and category_5.id is not null then category_1.name
            else null
        end as category_name_level_5, 
        
        case
            when category_1.parent_category_id is null and category_1.id is not null then category_1.slug
            when category_2.parent_category_id is null and category_2.id is not null then category_2.slug
            when category_3.parent_category_id is null and category_3.id is not null then category_3.slug
            when category_4.parent_category_id is null and category_4.id is not null then category_4.slug
            when category_5.parent_category_id is null and category_5.id is not null then category_5.slug
            else null
        end as category_slug_level_1,
        case
            when category_2.parent_category_id is null and category_2.id is not null then category_1.slug
            when category_3.parent_category_id is null and category_3.id is not null then category_2.slug
            when category_4.parent_category_id is null and category_4.id is not null then category_3.slug
            when category_5.parent_category_id is null and category_5.id is not null then category_4.slug
            else null
        end as category_slug_level_2,
        case
            when category_3.parent_category_id is null and category_3.id is not null then category_1.slug
            when category_4.parent_category_id is null and category_4.id is not null then category_2.slug
            when category_5.parent_category_id is null and category_5.id is not null then category_3.slug
            else null
        end as category_slug_level_3, 
        case
            when category_4.parent_category_id is null and category_4.id is not null then category_1.slug
            when category_5.parent_category_id is null and category_5.id is not null then category_2.slug
            else null
        end as category_slug_level_4, 
        case
            when category_5.parent_category_id is null and category_5.id is not null then category_1.slug
            else null
        end as category_slug_level_5 
            
    from {{source('pharmacy_landing', 'product_category_flat')}} as product_category_flat 
    left join {{source('pharmacy_landing', 'category')}} as category_1
        on product_category_flat.category_id = category_1.id
    left join {{source('pharmacy_landing', 'category')}} as category_2
        on category_1.parent_category_id = category_2.id
    left join {{source('pharmacy_landing', 'category')}} as category_3
        on category_2.parent_category_id = category_3.id
    left join {{source('pharmacy_landing', 'category')}} as category_4
        on category_3.parent_category_id = category_4.id
    left join {{source('pharmacy_landing', 'category')}} as category_5
        on category_4.parent_category_id = category_5.id
),

--------------------------------------------------------------
-- забираем только те продукты, которые были проданы
--------------------------------------------------------------
sold_products as 
(
    select 
        cast(date_trunc('day', order_created_time_cet) as date) as order_day,
        store_name,
        pzn,
        product_id,
        product_name,
        sum(before_products_price) as before_products_price,
        sum(products_price) as products_price,
        sum(quantity) as quantity,
        count(distinct order_id) as orders
    from {{source('onfy', 'orders_info')}}
    group by 
        date_trunc('day', order_created_time_cet),
        store_name,
        pzn,
        product_id,
        product_name
),

--------------------------------------------------------------
-- добираем информацию о продукте
--------------------------------------------------------------
products_info as
(
    select distinct
        product.id as product_id,
        manufacturer_id,
        manufacturer.name as manufacturer_name,
        medicine.quantity as medicine_quantity,
        medicine.unit as medicine_unit,
        product.slug as product_slug,
        onfy_medicine_analogs.actives as actives_list,
        onfy_medicine_analogs.analog_pzns as analog_pzns,
        coalesce(onfy_medicine_analogs.analogs_count, 0) as analogs_count
    from {{source('pharmacy_landing', 'product')}} as product
    left join {{source('pharmacy_landing', 'manufacturer')}} as manufacturer
        on product.manufacturer_id = manufacturer.id
    left join {{source('pharmacy_landing', 'medicine')}} as medicine
        on product.id = medicine.id
    left join {{source('onfy', 'onfy_medicine_analogs')}} as onfy_medicine_analogs
        on medicine.country_local_id = onfy_medicine_analogs.pzn
)

select 
    sold_products.*,
    manufacturer_id,
    manufacturer_name,
    medicine_quantity,
    medicine_unit,
    product_slug,
    product_category,
    
    category_id_level_1,
    category_id_level_2,
    category_id_level_3,
    category_id_level_4,
    category_id_level_5,
    
    coalesce(category_name_level_1, 'NO CATEGORY') as category_name_level_1,
    category_name_level_2,
    category_name_level_3,
    category_name_level_4,
    category_name_level_5,
    
    coalesce(category_slug_level_1, 'NO CATEGORY') as category_slug_level_1,
    category_slug_level_2,
    category_slug_level_3,
    category_slug_level_4,
    category_slug_level_5,
    
    concat('https://onfy.de/katalog/', product_category, "/", product_category_slug) as catalog_url,
    concat('https://onfy.de/artikel/', pzn, '/', product_slug) as product_url,
    
    count(product_category) over (partition by order_day, pzn, manufacturer_id) as categories_number,

    actives_list,
    analog_pzns,
    analogs_count
from sold_products
join products_info
    using(product_id)
left join product_categories
    using(product_id)
