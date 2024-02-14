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
        MAX(partition_date) last_activity
    FROM {{ ref('fact_user_activity') }}
    GROUP BY 1
)
SELECT
    t.*,
    welcome_form.mlhs as welcome_form_is_selling,
    welcome_form.mlsl as welcome_form_shop_url
FROM t
LEFT JOIN {{ source('mongo', 'b2b_core_analytics_users_extras_daily_snapshot') }} welcome_form
    ON user_id = _id

