{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}

select _id as admin_id,
    a.email,
    coalesce(s.role, case when a.email like "%support.com" then "support"
         when a.email like "%joom.com" then "employee"
         else "unknown" end) as role,
    s.name as owner_name,
    timestamp(millis_to_ts_msk(ctms)) as created_ts_msk
from {{ source('mongo', 'b2b_core_admin_users_daily_snapshot') }} a
LEFT JOIN {{ ref('support_roles') }} s on a.email = s.email

