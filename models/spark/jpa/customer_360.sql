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
    user.has_ml_store AS welcome_form_has_ml_store,
    user.ml_shop_name AS welcome_form_ml_shop_name,
    user.sells_on_other_marketplaces AS welcome_form_sells_on_other_marketplaces,
    user.marketplaces AS welcome_form_marketplaces,
    user.does_import AS welcome_form_does_import,
    user.categories AS welcome_form_categories,
    user.store_link AS welcome_form_store_link,
    user.todos AS welcome_form_todos,
    user.force_non_premium AS welcome_form_force_non_premium,
    user.force_premium AS welcome_form_force_premium
FROM t
LEFT JOIN {{ ref('dim_analytics_user') }} user
    ON user_id = _id

