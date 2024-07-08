{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'model_owner' : '@amitiushkina',
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}

WITH 
deals as (
     SELECT
        user_id,
        deal_id, product_id,
        created_ts_msk - INTERVAL 3 hours AS deal_created_ts,
        CAST(created_ts_msk - INTERVAL 3 hours AS DATE) AS deal_created_date,
        CASE WHEN d.owner_email is not null then owner_ts - INTERVAL 3 hours end AS owner_ts,
        CAST(CASE WHEN d.owner_email is not null then owner_ts - INTERVAL 3 hours end AS DATE) AS owner_date
    FROM {{ ref('fact_deals') }} d
    left join {{ ref('dim_deal_products') }} using (deal_id, user_id)
    WHERE next_effective_ts_msk is null
        AND (self_service or ss_customer)
),

utm_labels as (
    SELECT DISTINCT
        first_value(labels.utm_source) over (partition by user_id order by event_ts_msk) as utm_source,
        first_value(labels.utm_medium) over (partition by user_id order by event_ts_msk) as utm_medium,
        first_value(labels.utm_campaign) over (partition by user_id order by event_ts_msk) as utm_campaign,
        user_id
    from
    (
    select
    map_from_arrays(transform(
            split(split_part(payload.pageUrl, "?", -1), '&'), 
            x -> case when split_part(x, '=', 1) != "gclid" then split_part(x, '=', 1) else "gclid" || uuid() end
        ),
        
        transform(
            split(split_part(payload.pageUrl, "?", -1), '&'), 
            x -> split_part(x, '=', 2)
        )
        ) as labels,
        user.userId as user_id, event_ts_msk
    from {{ source('b2b_mart', 'device_events') }}
    where type = 'sessionStart' and payload.pageUrl like "%utm%"
    )
)
,

products AS (
    SELECT
        l1_merchant_category_name,
        l2_merchant_category_name,
        category_id,
        product_id,
        user_id,
        event_ts,
        event_date,
        CASE WHEN lead_type = 'orderCreateClick' THEN 1 ELSE 0 END AS order_create,
        deal_id,
        deal_created_ts,
        deal_created_date,
        case when deal_created_ts is not null then 1 else 0 end as deal_created,
        owner_ts,
        owner_date,
        case when owner_ts is not null then 1 else 0 end as deal_assigned
    FROM
    (
    SELECT
        payload['productId'] AS product_id,
        user['userId'] AS user_id,
        category_id,
        event_ts_msk - interval 3 hours AS event_ts,
        CAST(event_ts_msk - interval 3 hours AS DATE) AS event_date,
        LEAD(type) OVER (
            PARTITION BY CASE WHEN type = 'productSelfServiceOpen' THEN payload['productId'] ELSE split_part(payload.pageUrl, "/", -1) END
            ORDER BY event_ts_msk
            ) as lead_type,
        l1_merchant_category_name,
        l2_merchant_category_name,
        type
    FROM {{ source('b2b_mart', 'device_events') }} main
    LEFT JOIN (select distinct product_id, category_id from {{ ref('sat_published_product') }}) on product_id = payload['productId']
    LEFT JOIN {{ ref('gold_merchant_categories') }} AS cat
        ON category_id = cat.merchant_category_id
    WHERE type in ('productSelfServiceOpen', 'orderCreateClick')
       -- AND payload.pageUrl like '%https://joom.pro/pt-br%'
        AND CAST(event_ts_msk AS DATE) >= '2024-04-01'
    )
    LEFT JOIN deals USING (user_id, product_id)
    WHERE type = 'productSelfServiceOpen'
),

categories as (
    SELECT
        l1_merchant_category_name,
        l2_merchant_category_name,
        payload['categoryId'] AS category_id,
        '' as product_id,
        user['userId'] AS user_id,
        event_ts_msk - interval 3 hours AS event_ts,
        CAST(event_ts_msk - interval 3 hours AS DATE) AS event_date
    FROM {{source('b2b_mart', 'device_events') }} AS main
    LEFT JOIN {{ ref('gold_merchant_categories') }} AS cat
        ON main.payload['categoryId'] = cat.merchant_category_id
    WHERE type = 'categoryOpen'
        AND payload.pageUrl like '%https://joom.pro/pt-br%'
        AND CAST(event_ts_msk AS DATE) >= '2024-01-01'
)


SELECT * FROM
(
SELECT
        l1_merchant_category_name,
        l2_merchant_category_name,
        category_id,
        product_id,
        user_id,
        event_ts,
        event_date,
        order_create,
        deal_id,
        deal_created_ts,
        deal_created_date,
        deal_created,
        owner_ts,
        owner_date,
        deal_assigned,
        'products' as page_type
FROM products

UNION ALL 

SELECT
        l1_merchant_category_name,
        l2_merchant_category_name,
        category_id,
        product_id,
        user_id,
        event_ts,
        event_date,
        0 as order_create,
        '' deal_id,
        null as deal_created_ts,
        null as deal_created_date,
        0 as deal_created,
        null as owner_ts,
        null as owner_date,
        0 as deal_assigned,
        'categories' as page_type
FROM categories
) LEFT JOIN utm_labels USING (user_id)
