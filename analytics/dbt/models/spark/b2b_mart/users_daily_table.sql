{{ config(
    schema='b2b_mart',
    materialized='incremental',
    partition_by=['partition_date_msk'],
    file_format='delta',
    meta = {
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'bigquery_partitioning_date_column': 'partition_date_msk',
      'bigquery_known_gaps': ['2023-01-27']
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

gmv_user_admin as (
    select 
    date_payed,
    user_id, 
    owner_email, 
    owner_role,
    sum(gmv_initial) as gmv_user_admin
    from gmv
    group by date_payed, user_id, owner_email, owner_role
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
owner_id
from {{ ref('dim_user') }} du
left join {{ ref('key_validation_status') }} on key_validation_status.id = validation_status
left join {{ ref('key_validation_reject_reason') }} reject_reason on reject_reason.id = du.reject_reason
where next_effective_ts_msk is null
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
  select "low" as grage, 1 as value
  union all 
  select "medium" as grage, 2 as value
  union all 
  select "high" as grage, 3 as value
),

customers as (
    select distinct
    _id as user_id,
    companyName,
    estimatedPurchaseVolume.from as volume_from,
    estimatedPurchaseVolume.to as volume_to,
    grades.grade as grade,
    grades_prob.grade_prob as grade_probability
    from {{ source('mongo', 'b2b_core_customers_daily_snapshot') }}
    left join grades on gradeInfo.grade = grades.value
    left join grades_prob on gradeInfo.prob = grades_prob.value
),

current_admin as (select 
    date_payed,
    u.user_id,
    u.validation_status,
    u.reject_reason,
    a.email as owner_email,
    a.owner_role,
    companyName as company_name,
    volume_from,
    volume_to,
    grade,
    grade_probability,
    max(gmv_year) as gmv_year,
    max(gmv_quarter) as gmv_quarter,
    max(gmv_user_admin) as gmv_user_admin,
    'current admin' as admin_status
    from users as u
    left join admin as a on u.owner_id = a.admin_id
    left join customers as c on u.user_id = c.user_id
    left join gmv_user gu on gu.user_id = u.user_id
    left join gmv_user_admin gua on u.user_id = gua.user_id and a.email = gua.owner_email
    group by 
        date_payed,
        u.user_id,
        u.validation_status,
        u.reject_reason,
        a.email,
        a.owner_role,
        companyName,
        volume_from,
        volume_to,
        grade,
        grade_probability),
        
past_admin as (select 
    date_payed,
    u.user_id,
    u.validation_status,
    u.reject_reason,
    gua.owner_email,
    gua.owner_role,
    companyName as company_name,
    volume_from,
    volume_to,
    grade,
    grade_probability,
    max(gmv_year) as gmv_year,
    max(gmv_quarter) as gmv_quarter,
    max(gmv_user_admin) as gmv_user_admin,
    'past admin' as admin_status
    from gmv_user_admin gua
    left join users as u on u.user_id = gua.user_id 
    left join admin as a on u.owner_id = a.admin_id
    left join customers as c on u.user_id = c.user_id
    left join gmv_user gu on gu.user_id = u.user_id
    where a.email != gua.owner_email
    group by 
        date_payed,
        u.user_id,
        u.validation_status,
        u.reject_reason,
        gua.owner_email,
        gua.owner_role,
        companyName,
        volume_from,
        volume_to,
        grade,
        grade_probability
    )
    
    
select *, date('{{ var("start_date_ymd") }}') as partition_date_msk from current_admin
union all 
select *, date('{{ var("start_date_ymd") }}') as partition_date_msk from past_admin
