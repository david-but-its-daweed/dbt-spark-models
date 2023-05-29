{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}

with 
conversion as (
    select "NoConversionAttempt" as status,
    0 as status_int
    union all 
    select "ConversionFailed" as status,
    10 as status_int
    union all 
    select "Converting" as status,
    20 as status_int
    union all 
    select "Converted" as status,
    30 as status_int
),


users1 as (
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

users as (
select distinct
      u.user_id, 
      co.status as conversion_status,
      coalesce(u.country, "RU") as country,
      u.validation_status,
      u.reject_reason,
      a.owner_email,
      u.owner_id,
      u.first_name,
      u.last_name,
      a.owner_role,
      c.company_name,
      c.grade,
      u.created_ts_msk as user_created_time
    from users1 as u
    left join admin as a on u.owner_id = a.admin_id
    left join customers as c on u.user_id = c.user_id
    left join conversion co on du.conversion_status = c.status_int
 ),

user_interaction as 
(select 
    _id as interaction_id, 
    date(from_unixtime(ctms/1000 + 10800)) as interaction_create_date,
    uid as user_id, 
    popupRequestID as request_id,
    description,
    friendlyId as interaction_friendly_id,
    map_from_entries(utmLabels)["utm_campaign"] as utm_campaign,
    map_from_entries(utmLabels)["utm_source"] as utm_source,
    map_from_entries(utmLabels)["utm_medium"] as utm_medium,
    source as source, 
    type as type,
    campaign as campaign,
    websiteForm as website_form,
    createdAutomatically as created_automatically,
    interactionType as interaction_type,
    conversion_status,
    user_created_time,
    country,
    validation_status,
    reject_reason,
    owner_email,
    owner_role,
    first_name,
    last_name,
    company_name,
    grade,
    row_number() over (partition by user_id order by case when incorrectAttribution
        then 1 else 0 end, interactionType, ctms) = 1 as first_interaction_type,
    row_number() over (partition by user_id order by case when incorrectAttribution
        then 1 else 0 end, interactionType, ctms desc) = 1 
        as last_interaction_type,
    coalesce(incorrectAttribution, FALSE) as incorrect_attr,
    coalesce(incorrectUtm, FALSE) as incorrect_utm
from {{ ref('scd2_interactions_snapshot') }} m
inner join users n on n.user_id = m.uid
    where dbt_valid_to is null
    and (incorrectAttribution is null or not incorrectAttribution)
),


paied as (
    SELECT
        fo.user_id,
        MIN(DATE(fo.min_manufactured_ts_msk)) AS min_date_payed
    FROM {{ ref('fact_order') }} AS fo
    group by fo.user_id
)

select 
    interaction_id, 
    interaction_create_date,
    u.user_id, 
    request_id,
    description,
    interaction_friendly_id,
    utm_campaign,
    utm_source,
    utm_medium,
    source, 
    type,
    campaign,
    website_form,
    created_automatically,
    interaction_type,
    conversion_status,
    user_created_time,
    country,
    validation_status,
    reject_reason,
    owner_email,
    owner_role,
    first_name,
    last_name,
    company_name,
    grade,
    case when interaction_type = 0 and not incorrect_attr then first_interaction_type else FALSE end as first_interaction_type,
    case when interaction_type = 0 and not incorrect_attr then last_interaction_type else FALSE end as last_interaction_type,
    min_date_payed,
    incorrect_attr,
    incorrect_utm,
    case when interaction_create_date >= min_date_payed then TRUE ELSE FALSE END AS retention
    from user_interaction as u
    left join paied as p on u.user_id = p.user_id
    
