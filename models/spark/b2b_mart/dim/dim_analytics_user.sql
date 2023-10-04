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
  millis_to_ts_msk(t.ctms) as created_ts_msk,
  millis_to_ts_msk(t.utms) as updated_ts_msk,
  coalesce(t.phone, pn.phone) as phone_number
FROM {{ source('mongo', 'b2b_core_analytics_users_daily_snapshot') }} t
left join phone_number pn on t._id = pn.user_id
