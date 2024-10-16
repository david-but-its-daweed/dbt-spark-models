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


with interactions as (
    select user_id, min(interaction_create_time) as registration_start
    from {{ ref('fact_attribution_interaction') }}
    where type = 'Web' AND source = 'selfService' AND interaction_type = 10
    group by user_id
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
    where type = 'sessionStart' and payload.pageUrl like "%utm%" and and payload.pageUrl not like  '%https://joompro.ru/ru%'
    )
)
,
    
phone_numbers as (
        select distinct uid as user_id, _id as phone_number
        from {{ source('mongo', 'b2b_core_phone_credentials_daily_snapshot') }}
    ),

users_info as (
        select distinct 
        _id as user_id,
        fn as user_name,
        email as user_email,
        roleOtherValue as user_company_role_other_value,
        landingId as landing_id, 
        contactId as contact_id 
        from {{ source('mongo', 'b2b_core_users_daily_snapshot') }}
        
    ),
    
company_info as (
        select distinct 
        _id as user_id,
        companyName as company_name,
        companyWebsite as company_website,
        companyOpFields as company_op_fields,
        companyEmployeesNumber as company_employees_number,
        hasProductImport as has_product_import,
        catIds as product_categories,
        categoryOtherValues as category_other_values,
        companyAnnualTurnoverRange as company_annual_turnover_range, 
        cnpj as cnpj,
        gradeInfo.grade as grade
        from {{ source('mongo', 'b2b_core_customers_daily_snapshot') }}
    ),


joompro_users_table as (
select * from interactions
left join phone_numbers using (user_id)
left join users_info using (user_id)
left join company_info using (user_id)
left join utm_labels using (user_id)
),

users_info_1 AS (
    SELECT
        user_id,
        registration_start AS registration_start,
        phone_number,
        user_name,
        user_email,
        ARRAY_JOIN(user_company_role_other_value, ', ') AS user_company_role_other_value,
        company_name,
        company_website,
        CASE
            WHEN company_employees_number = 10
                THEN '0-2'
            WHEN company_employees_number = 20
                THEN '3-10'
            WHEN company_employees_number = 30
                THEN '11-50'
            WHEN company_employees_number = 40
                THEN '51-200'
            WHEN company_employees_number = 50
                THEN '>= 201'
        END AS company_employees_number,
        company_op_fields AS company_op_fields,
        has_product_import,
        product_categories,
        ARRAY_JOIN(category_other_values, ', ') AS category_other_values,
        company_annual_turnover_range,
        grade,
        cnpj,
        utm_source,
        utm_medium,
        utm_campaign,
        landing_id, 
        contact_id 
    FROM joompro_users_table
    WHERE user_id NOT IN (
        '65f8a3b040640e6f0b103c62',
        '62a9feec98d5f1bcd5f8f651',
        '64dfe85752e94057726ce7e3',
        '654a36ca194414a2aa015942',
        '6571e7a767653caa48078a8b',
        '6050ddece1fffe0006ee7d80',
        '625441434c41263737ad2ca4',
        '65e8783880017584d8a361f6',
        '65ba98813b6d7111865f2f91',
        '661425270c35c69b50009cb0'
    )
),

users_info_2 AS (
-- добавляю название категорий
    SELECT
        user_id,
        registration_start,
        phone_number,
        user_name,
        user_email,
        user_company_role_other_value,
        company_name,
        company_website,
        company_employees_number,
        company_op_fields,
        has_product_import,
        product_categories,
        category_other_values,
        company_annual_turnover_range,
        grade,
        cnpj,
        utm_source,
        utm_medium,
        utm_campaign,
        landing_id, 
        contact_id,
        ARRAY_JOIN(COLLECT_LIST(merchant_category_name), '; ') AS categories
    FROM (SELECT *, EXPLODE_OUTER(product_categories) AS product_category FROM users_info_1) AS main
    LEFT JOIN {{ ref('gold_merchant_categories') }} AS cat
        ON cat.merchant_category_id = main.product_category
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
), 

categories AS (
    SELECT
        user_id,
        ARRAY_JOIN(COLLECT_LIST(DISTINCT merchant_category_name), '; ') AS categories_opened
    FROM (
        SELECT
            merchant_category_name,
            user['userId'] AS user_id
        FROM {{ source('b2b_mart', 'device_events') }}        AS main
        LEFT JOIN {{ ref('gold_merchant_categories') }} AS cat
            ON main.payload['categoryId'] = cat.merchant_category_id
        WHERE type = 'categoryOpen'
            AND payload.pageUrl like '%https://joom.pro/pt-br%'
            AND CAST(event_ts_msk AS DATE) >= '2024-01-01'
    )
    GROUP BY 1
),

products AS (
    SELECT
        user_id,
        ARRAY_JOIN(COLLECT_LIST( product_id), '; ')         AS products_opened,
        ARRAY_JOIN(COLLECT_LIST(DISTINCT product_id), '; ') AS uniq_products_opened
    FROM (
        SELECT
            payload['productId'] AS product_id,
            user['userId'] AS user_id
        FROM {{ source('b2b_mart', 'device_events') }}
        WHERE type = 'productSelfServiceOpen'
            AND payload.pageUrl like '%https://joom.pro/pt-br%'
            AND CAST(event_ts_msk AS DATE) >= '2024-01-01'
    )
    GROUP BY 1
),

order_clicks AS (
    SELECT
        user['userId'] AS user_id,
        COUNT(*)       AS create_order_clicks
    FROM {{ source('b2b_mart', 'device_events') }}
    WHERE type = 'orderCreateClick'
        AND payload.pageUrl like '%https://joom.pro/pt-br%'
        AND payload["isRegistrationDataFilled"]    
    GROUP BY 1
),

deals as (
    SELECT DISTINCT
        user_id, 
        ARRAY_JOIN(COLLECT_LIST(CONCAT('https://admin.joompro.io/users/', user_id, '/deal/', deal_id)), '; ')         AS deals,
        SUM(gmv) as gmv
    FROM {{ ref('fact_deals') }}
    LEFT JOIN (
        select sum(planned_offer_cost/1000000) as gmv, deal_id
            from {{ ref('fact_customer_requests') }}
        group by deal_id
    ) USING (deal_id)
    WHERE next_effective_ts_msk IS NULL AND deal_id is not null
    GROUP BY user_id
),

main AS (
    SELECT
        user_id,
        registration_start,
        phone_number,
        user_name,
        user_email,
        user_company_role_other_value,
        company_name,
        company_website,
        company_employees_number,
        has_product_import,
        category_other_values,
        company_annual_turnover_range,
        grade,
        cnpj,
        utm_source,
        utm_medium,
        utm_campaign,
        categories,
        categories_opened,
        products_opened,
        uniq_products_opened,
        COALESCE(create_order_clicks, 0) AS create_order_clicks,
        deals,
        gmv,
        landing_id, 
        contact_id, 
        ARRAY_JOIN(
            COLLECT_LIST(
                CASE
                    WHEN company_op_field = 10
                        THEN 'Factory'
                    WHEN company_op_field = 20
                        THEN 'MarketplaceSeller'
                    WHEN company_op_field = 30
                        THEN 'OnlineRetailer'
                    WHEN company_op_field = 40
                        THEN 'OfflineRetailer'
                    WHEN company_op_field = 50
                        THEN 'B2BReseller'
                END
            ),
            '; '
        ) AS company_op_field_name
    FROM (SELECT *, EXPLODE_OUTER(company_op_fields) AS company_op_field FROM users_info_2) AS main
    LEFT JOIN categories   USING(user_id)
    LEFT JOIN products     USING(user_id)
    LEFT JOIN order_clicks USING(user_id)
    LEFT JOIN deals        USING(user_id)
    WHERE phone_number IS NOT NULL
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26
)

SELECT * FROM main
where phone_number not like '79%'
