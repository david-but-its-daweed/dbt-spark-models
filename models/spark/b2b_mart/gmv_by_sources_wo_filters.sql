{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'priority_weight': '150',
      'bigquery_load': 'true'
    }
) }}



WITH not_jp_users AS (
    SELECT DISTINCT
        user_id,
        owner_id AS owner_moderator_id
    FROM {{ ref('dim_user') }}
    WHERE
        (NOT fake OR fake IS NULL)
        AND next_effective_ts_msk IS NULL
),

admin AS (
    SELECT
        admin_id,
        a.email,
        a.role AS owner_role
    FROM {{ ref('dim_user_admin') }} AS a
),

order_v2_mongo AS (
    SELECT
        fo.order_id AS order_id,
        fo.user_id,
        DATE(fo.created_ts_msk) AS created_ts_msk,
        DATE(fo.min_manufactured_ts_msk) AS manufactured_date,
        MIN(DATE(fo.min_manufactured_ts_msk)) OVER (PARTITION BY fo.user_id) AS min_manufactured_date,
        u.owner_moderator_id
    FROM {{ ref('fact_order') }} AS fo
    INNER JOIN not_jp_users AS u ON fo.user_id = u.user_id
    WHERE (order_id = '6735ffb2f182e3f4a318c0ee'  AND fo.next_effective_ts_msk IS NULL) or 
        (fo.last_order_status < 60
        AND fo.last_order_status >= 10
        AND fo.next_effective_ts_msk IS NULL
        AND fo.min_manufactured_ts_msk IS NOT NULL)
),

order_v2 AS (
    SELECT
        order_id,
        total_confirmed_price,
        final_gross_profit,
        initial_gross_profit,
        owner_moderator_id
    FROM (
        SELECT
            order_id,
            total_confirmed_price,
            final_gross_profit,
            initial_gross_profit,
            owner_moderator_id,
            ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY event_ts_msk DESC) AS rn
        FROM {{ ref('fact_order_change') }}
    )
    WHERE rn = 1
),

source AS (
    SELECT
        user_id,
        source,
        type,
        campaign,
        utm_campaign,
        utm_source,
        utm_medium,
        min_date_payed
    FROM {{ ref('fact_attribution_interaction') }}
    WHERE last_interaction_type
),

after_second_qrt_new_order AS (
    SELECT
        o.manufactured_date AS t,
        p.order_id,
        SUM(p.total_confirmed_price) AS gmv_initial,
        SUM(p.initial_gross_profit) AS initial_gross_profit,
        SUM(p.final_gross_profit) AS final_gross_profit,
        s.utm_campaign,
        s.utm_source,
        s.utm_medium,
        s.source,
        s.type,
        s.campaign,
        o.user_id,
        a.email AS owner_email,
        a.owner_role,
        COALESCE(NOT(s.min_date_payed IS NULL OR DATE(s.min_date_payed) >= o.created_ts_msk), TRUE) AS retention,
        CASE
            WHEN o.min_manufactured_date < o.manufactured_date THEN 'first order'
            ELSE 'repeated order'
        END AS first_order
    FROM order_v2 AS p
    INNER JOIN order_v2_mongo AS o ON p.order_id = o.order_id
    LEFT JOIN source AS s ON o.user_id = s.user_id
    LEFT JOIN admin AS a ON o.owner_moderator_id = a.admin_id
    WHERE p.order_id NOT IN ('6294f3dd4c428b23cd6f2547', '64466aad3519d01068153f0b')
    GROUP BY
        o.manufactured_date,
        p.order_id,
        s.utm_campaign,
        s.utm_source,
        s.utm_medium,
        s.source,
        s.type,
        s.campaign,
        COALESCE(NOT(s.min_date_payed IS NULL OR DATE(s.min_date_payed) >= o.created_ts_msk), TRUE),
        CASE
            WHEN o.min_manufactured_date < o.manufactured_date THEN 'first order'
            ELSE 'repeated order'
        END,
        o.user_id,
        a.email,
        a.owner_role
),

country AS (
    SELECT DISTINCT
        user_id,
        COALESCE(country, "RU") AS country
    FROM {{ ref('dim_user') }}
    WHERE next_effective_ts_msk IS NULL
),

users AS (
    SELECT DISTINCT
        user_id,
        day,
        client
    FROM (
        SELECT
            a.user_id,
            d.day,
            CASE
                WHEN
                    SUM(CASE WHEN a.t > add_months(d.day, -6) AND a.t <= d.day THEN a.gmv_initial ELSE 0 END)
                        OVER (PARTITION BY a.user_id, d.day) > 100000 THEN 'big client'
                WHEN
                    SUM(CASE WHEN a.t > add_months(d.day, -6) AND a.t <= d.day THEN a.gmv_initial ELSE 0 END)
                        OVER (PARTITION BY a.user_id, d.day) > 30000 THEN 'medium client'
                ELSE 'small client'
            END AS client
        FROM (
            SELECT DISTINCT
                order_id,
                user_id,
                gmv_initial,
                t,
                1 AS for_join
            FROM after_second_qrt_new_order
        ) AS a
        LEFT JOIN (
            SELECT
                EXPLODE(SEQUENCE(to_date('2022-03-01'), to_date(CURRENT_DATE()), INTERVAL 1 DAY)) AS day,
                1 AS for_join
        ) AS d ON a.for_join = d.for_join
    )
)

SELECT
    a.t,
    a.order_id,
    a.gmv_initial,
    a.initial_gross_profit,
    a.final_gross_profit,
    a.utm_campaign,
    a.utm_source,
    a.utm_medium,
    a.source,
    a.type,
    a.campaign,
    a.retention,
    a.user_id,
    c.country,
    a.owner_email,
    a.owner_role,
    a.first_order,
    users.client,
    CASE
        WHEN
            SUM(CASE WHEN a.t > add_months(current_date(), -6) AND a.t <= current_date() THEN a.gmv_initial ELSE 0 END)
                OVER (PARTITION BY a.user_id) > 100000 THEN 'big client'
        WHEN
            SUM(CASE WHEN a.t > add_months(current_date(), -6) AND a.t <= current_date() THEN a.gmv_initial ELSE 0 END)
                OVER (PARTITION BY a.user_id) > 30000 THEN 'medium client'
        ELSE 'small client'
    END AS current_client
FROM after_second_qrt_new_order AS a
LEFT JOIN users ON a.user_id = users.user_id AND a.t = users.day
LEFT JOIN country AS c ON a.user_id = c.user_id
WHERE a.gmv_initial > 0
