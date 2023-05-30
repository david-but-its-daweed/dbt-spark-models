{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'team': 'general_analytics',
      'bigquery_load': 'true'
    }
) }}

with 

admin as (
    SELECT
        admin_id,
        email,
        role as owner_role
    FROM {{ ref('dim_user_admin') }}
),

users as (
select 
user_id,
key_validation_status.status as validation_status,
reject_reason.reason as reject_reason,
owner_id,
country,
du.amo_crm_id, 
du.amo_id, 
du.conversion_status,
invited_by_promo, 
du.last_name, 
du.first_name,
created_ts_msk
from {{ ref('dim_user') }} du
left join {{ ref('key_validation_status') }} on key_validation_status.id = validation_status
left join {{ ref('key_validation_reject_reason') }} reject_reason on reject_reason.id = du.reject_reason
where next_effective_ts_msk is null and (not is_partner or is_partner is null)
),

grades as (
  select "unknown" as grade, 0 as value
  union all 
  select "a" as grade, 1 as value
  union all 
  select "b" as grade, 2 as value
  union all 
  select "c" as grade, 3 as value
),

grades_prob as (
  select "unknown" as grade_prob, 0 as value
  union all 
  select "low" as grade_prob, 1 as value
  union all 
  select "medium" as grade_prob, 2 as value
  union all 
  select "high" as grade_prob, 3 as value
),

customers as (
    select distinct
    customer_id as user_id,
    company_name,
    monthly_turnover_from as volume_from,
    monthly_turnover_to as volume_to,
    tracked,
    grades.grade,
    grades_prob.grade_prob as grade_probability
    from {{ ref('scd2_mongo_customer_main_info') }} m
    left join grades on coalesce(m.grade, 0) = grades.value
    left join grades_prob on coalesce(m.grade_probability, 0) = grades_prob.value
    where TIMESTAMP(dbt_valid_to) is null
),

users_hist as (
  select
    user_id,
    min(partition_date_msk) as validated_date
    from {{ ref('fact_user_change') }}
    where validation_status = 'validated'
    group by user_id
)

select distinct
    u.user_id,
    created_ts_msk,
    validated_date,
    country,
    conversion_status,
    tracked,
    u.validation_status,
    u.reject_reason,
    u.owner_id,
    a.email as owner_email,
    a.owner_role,
    last_name, 
    first_name,
    company_name,
    volume_from,
    volume_to,
    coalesce(grade, "unknown") as grade,
    coalesce(grade_probability, "unknown") as grade_probability,
    amo_crm_id, 
    amo_id, 
    invited_by_promo
    from users as u
    left join admin as a on u.owner_id = a.admin_id
    left join customers as c on u.user_id = c.user_id
    left join users_hist as uh on u.user_id = uh.user_id
