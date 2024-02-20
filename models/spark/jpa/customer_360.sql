{{ config(
    schema='joompro_analytics_internal_mart',
    materialized='view',
    meta = {
      'model_owner': '@aplotnikov',
      'bigquery_load': 'true',
      'bigquery_project_id': 'joom-analytics-joompro-public',
      'bigquery_check_counts': 'false'
    }
) }}

WITH t AS (
    SELECT
        user_id,
        COUNT(DISTINCT partition_date) user_activity_days,
        APPROX_COUNT_DISTINCT(request_id) user_activity_total_requests,
        MIN(partition_date) first_activity,
        MAX(partition_date) last_activity,
        SUM(IF(request_path = '/dashboard/checkout', 1, 0)) AS checkout_page_visits,
        SUM(IF(request_path = '/dashboard/plans', 1, 0)) AS plans_page_visits
    FROM {{ ref('fact_user_activity') }}
    GROUP BY 1
)
SELECT
    t.*,
    user.first_name,
    user.phone_number,
    user.email,
    user.has_store AS welcome_form_is_selling,
    user.store_link AS welcome_form_shop_url
FROM t
LEFT JOIN {{ ref('dim_analytics_user') }} user
    ON user_id = _id

