{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'team': 'general_analytics',
      'bigquery_load': 'true'
    }
) }}

with gmv as (
    select distinct
        t as date_payed, 
        g.order_id,
        g.gmv_initial,
        g.initial_gross_profit,
        g.final_gross_profit,
        g.owner_email,
        g.owner_role,
        user_id
    FROM {{ ref('gmv_by_sources') }} g
),

gmv_user as (
    select user_id,
    sum(gmv_initial) as gmv_year,
    sum(case when date_payed >= date('{{ var("start_date_ymd") }}') - interval 3 month then gmv_initial else 0 end) as gmv_quarter
    from gmv 
    where date_payed >= date('{{ var("start_date_ymd") }}') - interval 1 year
    group by user_id
),

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
du.first_name
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
)

select distinct
    u.user_id,
    country,
    conversion_status,
    tracked,
    u.validation_status,
    u.reject_reason,
    a.email as owner_email,
    a.owner_role,
    last_name, 
    first_name,
    company_name,
    volume_from,
    volume_to,
    coalesce(grade, "unknown") as grade,
    coalesce(grade_probability, "unknown") as grade_probability,
    gmv_year,
    gmv_quarter,
    amo_crm_id, 
    amo_id, 
    invited_by_promo
    from users as u
    left join admin as a on u.owner_id = a.admin_id
    left join customers as c on u.user_id = c.user_id
    left join gmv_user gu on gu.user_id = u.user_id
