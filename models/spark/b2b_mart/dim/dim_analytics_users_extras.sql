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


SELECT
    _id AS user_id,
    mlhs AS has_store,
    mlsl AS store_link
FROM {{ source('mongo', 'b2b_core_analytics_users_extras_daily_snapshot') }}
