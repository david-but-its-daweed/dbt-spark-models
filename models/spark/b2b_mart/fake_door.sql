{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}


with 
users as (
select user_id, interaction_create_time,
row_number() over (partition by user_id order by interaction_create_time) as rn
from {{ ref('fact_attribution_interaction') }}
where interaction_type = 10
and country = 'BR'
and interaction_create_date >= '2024-01-19'
),

requests as (
select
    created_time,
    customer_request_id,
    deal_id,
    user_id,
    link,
    shop,
    expected_quantity,
    case when shop = 'mercadolivre' and 
            link like '%produto.mercadolivre.com.br%' then concat("MLB-", split_part(split_part(link, "/", 4), "-", 2))
        when shop = 'mercadolivre' and link like '%www.mercadolivre.com.br%'
            then split_part(split_part(link, "/", 6), "?", 1)
    end as product_id
from
(
select distinct
created_time,
customer_request_id,
deal_id,
user_id,
link,
case when link not like '%http%' then 'No Link'
    when split_part(split_part(link, "//", 2), '/', 1) = '' then 'No Link'
    when link like '%mercadolivre%' then 'mercadolivre'
    else split_part(split_part(link, "//", 2), '/', 1)
end as shop,
expected_quantity
from {{ ref('fact_customer_requests') }}
where fake_door
  and link not like '%test%'
)
),

categories_merchado as (
    select
        category_id,
        breadcrumb_names[0] as cate_lv1,
        breadcrumb_names[1] as cate_lv2,
        breadcrumb_names[2] as cate_lv3,
        breadcrumb_names[3] as cate_lv4,
        breadcrumb_names[4] as cate_lv5,
        breadcrumb_names[5] as cate_lv6
    from {{ source('joompro_mart', 'mercadolibre_categories') }}
),

mercado_livre as (
select product_id, price_amount, cate_lv1, cate_lv2, cate_lv3, cate_lv4, cate_lv5, cate_lv6
from
(
select product_id, min(category_id) as category_id, min(price_amount) as price_amount
from
{{ source('joompro_analytics_mart', 'mercadolibre_products_snapshot') }}
where product_id in (select distinct product_id from requests)
group by product_id
)
left join categories_merchado using (category_id)
)

select distinct
user_id, interaction_create_time, 
customer_request_id, requests.created_time as customer_request_create_time,
deal_id, link, shop, product_id, expected_quantity, 
price_amount, cate_lv1, cate_lv2, cate_lv3, cate_lv4, cate_lv5, cate_lv6,
row_number() over (partition by user_id order by requests.created_time) as request_number
from 
users
left join requests using (user_id)
left join mercado_livre using (product_id)
where rn = 1
