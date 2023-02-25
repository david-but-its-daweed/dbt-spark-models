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
    email,
    case when email like "%support.com" then "support"
         when email like "%joom.com" then "employee"
         else "unknown" end as role,
    timestamp(millis_to_ts_msk(ctms)) as created_ts_msk
from {{ source('mongo', 'b2b_core_admin_users_daily_snapshot') }}

