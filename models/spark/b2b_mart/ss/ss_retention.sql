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

WITH users AS (
    SELECT
        user['userId'] AS user_id
    FROM {{source('b2b_mart', 'device_events') }}
    WHERE payload.pageUrl like '%https://joom.pro/pt-br%'
    GROUP BY 1
    HAVING SUM(IF(type IN ("productPreview", "searchOpen", "categoryClick", "orderPreview", "productGalleryScroll", "categoryOpen", "popupFormSubmit", "popupFormNext", "mainClick", "orderClick"), 1, 0)) > 0
),

visits AS (
    SELECT DISTINCT
       user['userId'] AS user_id,
        CAST(event_ts_msk AS DATE) AS event_date_msk
    FROM {{source('b2b_mart', 'device_events') }}
    WHERE type = 'sessionStart'
        AND payload.pageUrl like '%https://joom.pro/pt-br%'
        AND user['userId'] NOT IN (
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

main AS (
    SELECT
        event_date_msk,
        DATE_TRUNC("WEEK", event_date_msk) AS week,
        DATE_TRUNC("MONTH", event_date_msk) AS month,
        autorisation, registration,
        user_id,
        IF(
            DATEDIFF(CURRENT_DATE() - 1, event_date_msk) >= 1,
            (COUNT(*) OVER (PARTITION BY user_id ORDER BY UNIX_DATE(event_date_msk) RANGE BETWEEN 1 FOLLOWING AND 1 FOLLOWING)) > 0,
            NULL
        ) AS is_rd1,
        IF(
            DATEDIFF(CURRENT_DATE() - 1, event_date_msk) >= 3,
            (COUNT(*) OVER (PARTITION BY user_id ORDER BY UNIX_DATE(event_date_msk) RANGE BETWEEN 3 FOLLOWING AND 3 FOLLOWING)) > 0,
            NULL
        ) AS is_rd3,
        IF(
            DATEDIFF(CURRENT_DATE() - 1, event_date_msk) >= 7,
            (COUNT(*) OVER (PARTITION BY user_id ORDER BY UNIX_DATE(event_date_msk) RANGE BETWEEN 7 FOLLOWING AND 7 FOLLOWING)) > 0,
            NULL
        ) AS is_rd7
    FROM visits
    INNER JOIN users USING(user_id)
    LEFT JOIN (SELECT DISTINCT user_id, autorisation, registration from {{ ref('ss_funnel_table') }}) using (user_id)
),

wau as (
    SELECT
        COUNT(DISTINCT user_id) AS wau,
        autorisation, registration,
        week
    FROM main
    GROUP BY 2, 3, 4
),

mau as (
    SELECT
        COUNT(DISTINCT user_id) AS wau,
        autorisation, registration,
        month
    FROM main
    GROUP BY 2, 3, 4
)


SELECT
    event_date_msk,
    autorisation, registration,
    MAX(wau) AS wau,
    MAX(mau) AS mau,
    COUNT(DISTINCT user_id) AS dau,
    AVG(INT(is_rd1)) AS is_rd1,
    AVG(INT(is_rd3)) AS is_rd3,
    AVG(INT(is_rd7)) AS is_rd7
FROM main
LEFT JOIN wau using (week, autorisation, registration)
LEFT JOIN mau using (month, autorisation, registration)
WHERE event_date_msk >= '2024-04-01'
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
