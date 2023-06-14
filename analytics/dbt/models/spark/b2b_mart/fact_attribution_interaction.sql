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

users AS (
  SELECT DISTINCT 
      user_id, 
      c.status as conversion_status,
      coalesce(country, "RU") as country,
      validation_status,
      reject_reason,
      owner_email,
      first_name,
      last_name,
      owner_role,
      company_name,
      grade,
      amo_id,
      created_ts_msk as user_created_time,
      validated_date
  FROM {{ ref('fact_customers') }} du
  left join conversion c on du.conversion_status = c.status_int
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
    validated_date,
    validation_status,
    reject_reason,
    owner_email,
    owner_role,
    first_name,
    last_name,
    company_name,
    grade,
    amo_id,
    row_number() over (partition by user_id order by case when incorrectAttribution
        then 1 else 0 end, coalesce(interactionType, 100), ctms) = 1 as first_interaction_type,
    row_number() over (partition by user_id order by case when incorrectAttribution
        then 1 else 0 end, coalesce(interactionType, 100), ctms desc) = 1 
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
),
    
orders as (
    SELECT
        fo.user_id,
        count(distinct fo.order_id) as orders
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
    case when 
        amo_id is not null 
        and admin is null 
        and closed is not null 
        and orders > 0
    then 'ConversionFailed' 
    when 
        amo_id is not null 
        and admin is null 
        and closed is not null 
        and orders > 0
    then 'Converting'
    when 
        amo_id is not null 
        and admin is null 
        and orders = 0
    then 'NoConversionAttempt'
    else conversion_status end as 
    conversion_status,
    user_created_time,
    country,
    validated_date,
    validation_status,
    reject_reason,
    owner_email,
    owner_role,
    first_name,
    last_name,
    company_name,
    grade,
    amo_id,
    case when admin is not null or amo_id is null then true else false end as admin,
    case when interaction_type = 0 and not incorrect_attr then first_interaction_type else FALSE end as first_interaction_type,
    case when interaction_type = 0 and not incorrect_attr then last_interaction_type else FALSE end as last_interaction_type,
    min_date_payed,
    incorrect_attr,
    incorrect_utm,
    case when interaction_create_date >= min_date_payed then TRUE ELSE FALSE END AS retention
    from user_interaction as u
    left join paied as p on u.user_id = p.user_id
    left join (
            select distinct leadId, true as admin
            from
            {{ source('mongo', 'b2b_core_amo_crm_raw_leads_daily_snapshot') }}
            where lossReasons[0].name in ('Передали КАМам', 'Передали Sales')
        ) amo on u.amo_id = amo.leadId
    left join (
        select distinct 
        leadId, true as closed
        from
        {{ source('mongo', 'b2b_core_amo_crm_raw_leads_daily_snapshot') }} amo
        left join {{ ref('key_amo_status') }} st on amo.status = st.status_id
        where st.status_name in ('решение отложено', 'закрыто и не реализовано')
    )
    left join orders o on u.user_id = o.user_id
