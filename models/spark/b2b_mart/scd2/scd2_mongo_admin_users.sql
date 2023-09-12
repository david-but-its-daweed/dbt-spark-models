{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'bigquery_load': 'false'
    }
) }}



With roles as (
    SELECT 'KAM' as role,
    0 as priority
    union all 
    SELECT 'SalesManager' as role,
    1 as priority
    union all 
    SELECT 'ClientSupport' as role,
    2 as priority

),

one_role as (
    select    admin_id,
              u.role,
              first_value(u.role) over (partition by admin_id order by priority is null, priority) as one_role
    from
(
    select 
          _id as admin_id,
          explode(coalesce(roles, null)) as role
      from {{ ref('scd2_admin_users_snapshot') }}
      where dbt_valid_to is null
) u
left join roles on u.role = roles.role

)

select distinct 
    _id as admin_id,
    email,
    fn,
    language,
    ln,
    coalesce(role, 'Dev') as role,
    timestamp(millis_to_ts_msk(ctms)) as created_ts_msk,
    coalesce(one_role, 'Dev') as one_role,
    dbt_valid_to,
    dbt_valid_from
from {{ ref('scd2_admin_users_snapshot') }}
left join one_role on admin_id = _id
