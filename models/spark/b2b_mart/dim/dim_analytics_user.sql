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

WITH phone_number AS (
    SELECT DISTINCT
        _id AS phone,
        uid AS user_id
    FROM {{ source('mongo', 'b2b_core_phone_credentials_daily_snapshot') }}
)

SELECT
    t._id,
    MILLIS_TO_TS_MSK(t.ctms) AS created_ts_msk,
    MILLIS_TO_TS_MSK(t.utms) AS updated_ts_msk,
    t.fn AS first_name,
    COALESCE(t.phone, pn.phone) AS phone_number,
    t.email,
    u_extra.has_store,
    u_extra.store_link
FROM {{ source('mongo', 'b2b_core_analytics_users_daily_snapshot') }} AS t
LEFT JOIN phone_number AS pn ON t._id = pn.user_id
LEFT JOIN {{ ref('dim_analytics_users_extras') }} AS u_extra ON t._id = u_extra.user_id
