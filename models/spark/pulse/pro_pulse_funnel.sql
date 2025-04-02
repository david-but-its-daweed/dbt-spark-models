{{ config(
    schema='joompro_analytics_internal_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'bigquery_load': 'true',
      'bigquery_project_id': 'joom-analytics-joompro-public',
      'bigquery_check_counts': 'false',
      'priority_weight': '150'
    }
) }}

WITH order_deals AS (
    SELECT DISTINCT
        user_id,
        deal_id,
        order_id,
        created_date
    FROM {{ ref("fact_order_product_deal") }}
    WHERE current_order_status != "cancelled" AND current_order_status IS NOT NULL
),

utm_labels_before_order AS (
    SELECT
        utm_source,
        utm_medium,
        utm_campaign,
        user_id
    FROM (
        SELECT
            interaction.utm_source,
            interaction.utm_medium,
            interaction.utm_campaign,
            interaction.deal_id,
            interaction.user_id,
            interaction.visit_date,
            interaction.visit_ts_msk,
            ROW_NUMBER() OVER (PARTITION BY interaction.user_id ORDER BY interaction.visit_ts_msk DESC) AS rn
        FROM {{ ref("fact_marketing_deals_interactions") }} AS interaction
        LEFT JOIN order_deals USING (deal_id)
        WHERE order_deals.created_date >= interaction.visit_ts_msk OR order_deals.created_date IS NULL
    )
),

utm_labels AS (
    SELECT * FROM
    (
    SELECT
        interaction.utm_source,
        interaction.utm_medium,
        interaction.utm_campaign,
        interaction.user_id,
        interaction.visit_date,
        ROW_NUMBER() OVER (PARTITION BY interaction.user_id ORDER BY interaction.visit_date DESC) AS rn
    FROM {{ ref("fact_marketing_utm_interactions") }} AS interaction
    LEFT JOIN order_deals USING (user_id)
    WHERE order_deals.created_date >= interaction.visit_date OR order_deals.created_date IS NULL
    )
    WHERE rn = 1
),

gmv_by_sources AS (
    SELECT
        user_id,
        MIN(t) AS first_date_paid,
        SUM(gmv_initial) AS gmv_total,
        SUM(CASE WHEN first_order THEN gmv_initial END) AS gmv_first_order,
        COUNT(order_id) AS orders
    FROM
    (
    SELECT
        user_id,
        t,
        gmv_initial,
        order_id,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY t) = 1 AS first_order
    FROM {{ ref("gmv_by_sources") }}
    )
    GROUP BY user_id
)

SELECT
    users.user_id,
    users.phone_number,
    DATE(users.registration_start) AS segment,
    users.utm_source,
    users.utm_medium,
    users.utm_campaign,
    users.deals,
    users.gmv,
    users.user_MQL AS mql,
    users.user_SQL AS sql,
    users.Marketing_Lead_Type AS lead_type,
    users.mql_msk_date AS mql_date,
    users.sql_msk_date AS sql_date,
    gmv_by_sources.first_date_paid,
    gmv_by_sources.gmv_total,
    gmv_by_sources.gmv_first_order,
    gmv_by_sources.orders,
    gmv_by_sources.orders > 0 AND gmv_by_sources.orders IS NOT NULL AS paid,
    "first click" AS attribution
FROM {{ ref("ss_users_table") }} AS users
LEFT JOIN utm_labels_before_order AS utm ON users.user_id = utm.user_id
LEFT JOIN utm_labels ON users.user_id = utm_labels.user_id
LEFT JOIN gmv_by_sources ON users.user_id = gmv_by_sources.user_id

UNION ALL

SELECT
    users.user_id,
    users.phone_number,
    DATE(utm_labels.visit_date) AS segment,
    COALESCE(utm.utm_source, utm_labels.utm_source) AS utm_source,
    COALESCE(utm.utm_medium, utm_labels.utm_medium) AS utm_medium,
    COALESCE(utm.utm_campaign, utm_labels.utm_campaign) AS utm_campaign,
    users.deals,
    users.gmv,
    users.user_MQL AS mql,
    users.user_SQL AS sql,
    users.Marketing_Lead_Type AS lead_type,
    users.mql_msk_date AS mql_date,
    users.sql_msk_date AS sql_date,
    gmv_by_sources.first_date_paid,
    gmv_by_sources.gmv_total,
    gmv_by_sources.gmv_first_order,
    gmv_by_sources.orders,
    gmv_by_sources.orders > 0 AND gmv_by_sources.orders IS NOT NULL AS paid,
    "last click" AS attribution

FROM {{ ref("ss_users_table") }} AS users
LEFT JOIN utm_labels_before_order AS utm ON users.user_id = utm.user_id
LEFT JOIN utm_labels ON users.user_id = utm_labels.user_id
LEFT JOIN gmv_by_sources ON users.user_id = gmv_by_sources.user_id
