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

select distinct
    admin_id,
    email,
    fn,
    ln,
    language,
    fn||' '||ln as name,
    one_role as role,
    created_ts_msk
from {{ ref('scd2_mongo_admin_users') }} a
where dbt_valid_to is null

