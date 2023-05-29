{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}

select distinct
    admin_id,
    email,
    fn,
    ln,
    language,
    fn||' '||ln as name,
    one_role as role,
    created_ts_msk
from {{ ref('scd2_admin_users_snapshot') }} a
where dbt_valid_to is null

