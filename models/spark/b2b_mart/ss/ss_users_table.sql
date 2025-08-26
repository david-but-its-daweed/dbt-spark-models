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


WITH old_approach AS (
    SELECT 
        user_id, 
        MIN(interaction_create_time) AS registration_start
    FROM {{ ref('fact_attribution_interaction') }}
    WHERE type = 'Web' AND source = 'selfService' AND interaction_type = 10
    GROUP BY user_id
),

new_approach AS (
    SELECT
        user_id,
        MIN(event_ts_msk) AS registration_start
    FROM {{ ref('ss_events_authentication') }}
    GROUP BY 1
),
    
interactions as (
    SELECT user_id, MIN(registration_start) AS registration_start
    FROM (
        SELECT user_id, registration_start 
        FROM old_approach 
        UNION ALL 
        SELECT user_id, registration_start
        FROM new_approach 
    ) 
    GROUP BY 1
),
    
phone_numbers AS (
    SELECT DISTINCT 
        uid AS user_id, 
        _id AS phone_number
    FROM {{ source('mongo', 'b2b_core_phone_credentials_daily_snapshot') }}
),

users_info AS (
    SELECT DISTINCT 
        _id AS user_id,
        fn AS user_name,
        email AS user_email,
        roleOtherValue AS user_company_role_other_value,
        landingId AS landing_id, 
        contactId AS contact_id 
    FROM {{ source('mongo', 'b2b_core_users_daily_snapshot') }}
),
    
company_info AS (
    SELECT DISTINCT 
        _id AS user_id,
        companyName AS company_name,
        companyWebsite AS company_website,
        companyOpFields AS company_op_fields,
        companyEmployeesNumber AS company_employees_number,
        hasProductImport AS has_product_import,
        catIds AS product_categories,
        categoryOtherValues AS category_other_values,
        companyAnnualTurnoverRange AS company_annual_turnover_range, 
        cnpj AS cnpj,
        gradeInfo.grade AS grade
    FROM {{ source('mongo', 'b2b_core_customers_daily_snapshot') }}
),
    
utm_labels AS (
    SELECT DISTINCT
        FIRST_VALUE(utm_source) OVER (PARTITION BY user_id ORDER BY event_ts_msk) AS utm_source,
        FIRST_VALUE(utm_medium) OVER (PARTITION BY user_id ORDER BY event_ts_msk) AS utm_medium,
        FIRST_VALUE(utm_campaign) OVER (PARTITION BY user_id ORDER BY event_ts_msk) AS utm_campaign,
        user_id
    FROM {{ ref('ss_events_startsession') }}   
    WHERE type = 'sessionStart'
),

joompro_users_table AS (
    SELECT * 
    FROM interactions
    LEFT JOIN phone_numbers USING(user_id)
    LEFT JOIN users_info USING(user_id)
    LEFT JOIN company_info USING(user_id)
    LEFT JOIN utm_labels USING(user_id)
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
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21
), 

categories AS (
    SELECT
        user_id,
        ARRAY_JOIN(COLLECT_LIST(DISTINCT merchant_category_name), '; ') AS categories_opened
    FROM (
        SELECT
            merchant_category_name,
            user['userId'] AS user_id
        FROM {{ source('b2b_mart', 'device_events') }} AS main
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
        ARRAY_JOIN(COLLECT_LIST(CONCAT('https://admin.joompro.io/users/', user_id, '/deal/', deal_id)), '; ') AS deals,
        SUM(gmv) AS gmv
    FROM {{ ref('fact_deals') }}
    LEFT JOIN (
        SELECT sum(planned_offer_cost / 1000000) AS gmv, deal_id
            FROM {{ ref('fact_customer_requests') }}
        GROUP BY deal_id
    ) USING (deal_id)
    WHERE next_effective_ts_msk IS NULL AND deal_id IS NOT NULL
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
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26
), 
data_for_mql as (
select 
user_id, 
event_msk_date
from  {{ ref('ss_events_cart') }} 
where actionType  = 'add_to_cart'

union all 

select 
user_id, 
event_msk_date
from {{ ref('ss_events_cjm') }} 
where type = 'requestQuoteNowClick'
),

cart_activation AS (
    SELECT 
        user_id,
        MIN(event_msk_date) AS mql_msk_date,
        MAX(1) AS user_MQL
    FROM  data_for_mql 
    
    GROUP BY 1 
 ),
    
deal_activation AS (
    SELECT
        user_id,
        MIN(CASE WHEN deal_type = 'Sample' THEN deal_created_date END) AS mql_msk_date, 
        MIN(CASE WHEN deal_type != 'Sample' THEN deal_created_date END) AS sql_msk_date, 
        MAX(CASE WHEN deal_type != 'Sample' THEN 1 ELSE 0 END) AS user_SQL,
        MAX(CASE WHEN deal_type = 'Sample' THEN 1 ELSE 0 END) AS user_MQL
    FROM {{ ref('fact_deals_with_requests') }}
    WHERE deal_status NOT IN ('Test')
    GROUP BY 1
 )

SELECT 
    main.*,
    COALESCE(cart_activation.user_MQL, deal_activation.user_MQL, 0) AS user_MQL, 
    COALESCE(cart_activation.mql_msk_date, deal_activation.mql_msk_date) AS mql_msk_date,
    sql_msk_date,
    COALESCE(user_SQL, 0) AS user_SQL,
    CASE 
        WHEN user_SQL = 1 THEN 'SQL'
        WHEN COALESCE(cart_activation.user_MQL, deal_activation.user_MQL, 0) = 1 THEN 'MQL'
        WHEN cnpj IS NOT NULL OR company_annual_turnover_range IS NOT NULL THEN 'Lead'
        ELSE 'PreLead'
    END AS Marketing_Lead_Type,
    CASE
        WHEN company_annual_turnover_range IN ('XL','XXL') THEN 'A'
        WHEN company_annual_turnover_range IN ('M','L') THEN 'B'
        WHEN company_annual_turnover_range IN ('S') THEN 'C'
        WHEN company_annual_turnover_range IN ('XS','XXS') THEN 'D'
        WHEN cnpj IS NOT NULL THEN 'D'
    END AS questionnaire_grade
FROM main
    LEFT JOIN cart_activation USING(user_id)
    LEFT JOIN deal_activation USING(user_id)
WHERE phone_number NOT LIKE '79%'
