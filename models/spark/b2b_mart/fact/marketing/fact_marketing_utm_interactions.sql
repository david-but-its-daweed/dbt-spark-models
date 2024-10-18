{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@mkirusha',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}

with 
visits as (
select user_id, 
event_ts_msk as visit_ts_msk,
event_msk_date as visit_date,
utm_source,
case when utm_source = 'unrecognized_google_advertising' and utm_medium is Null then 'unrecognized_google_advertising' else utm_medium end utm_medium ,
utm_campaign, 
case when utm_source is Null and utm_medium is Null  then 'organic' else 'advertising' end as traffic_type,
---row_number() Over(partition by user_id order by event_ts_msk ) rn_1,
---row_number() Over(partition by user_id order by event_ts_msk desc ) rn_2,
'events' as data_source

from 
  {{ ref('ss_events_startsession') }}
where landing = 'pt-br' and bot_flag = 0 
----qualify max(bot_flag) Over(partition by user_id order by event_msk_date ) = 0
),
interactions as (
select interaction_id,
       interaction_create_date,
       interaction_create_time,
       user_id,
       utm_campaign,
       utm_source,
       utm_medium,
       source,
       type,
       campaign,
       first_interaction_type,
       last_interaction_type
 from {{ ref('fact_attribution_interaction') }}
),
users_with_visit as (
    select user_id, min(visit_ts_msk) as visit_ts_msk
    from visits 
),
interactions_visits as (
select  
 d.user_id, 
 interaction_create_time as visit_ts_msk,
 interaction_create_date as visit_date, 
 coalesce(i.utm_source,i.source) as utm_source ,
 i.utm_medium ,
  coalesce(i.utm_campaign, i.campaign) as utm_campaign , 
case 
when  type = 'Offline' then 'Offline'
when coalesce(i.utm_source,i.source) is null then 'organic'
else  'advertising'
  end as traffic_type,
---row_number() Over(partition by user_id order by interaction_create_time ) rn_1,
---row_number() Over(partition by user_id order by interaction_create_time desc ) rn_2,
'admin' as data_source
from  {{ ref('dim_user') }} d
left join users_with_visit using(user_id)
left join interactions i using(user_id)
where  (visit_ts_msk is null or i.first_interaction_type is True) 
and next_effective_ts_msk is null 
and country = 'BR'),

all_visits as (
select * from visits 

union all 

select * from interactions_visits) 


select 
user_id, 
visit_ts_msk,
visit_date,
traffic_type, 
utm_source,
utm_medium,
case when (utm_campaign is null or  utm_campaign = '') and 
utm_medium like '%google%' then 'unrecognized_google_campaign'
when  (utm_campaign is null or  utm_campaign = '') and  utm_medium like '%Exhibition%' then 'unrecognized_exhibition_campaign'
when traffic_type = 'advertising' and (utm_campaign is null or  utm_campaign = '')  then 'unrecognized_campaign'
else utm_campaign
end utm_campaign,
case when 
LOWER(utm_source) like '%acebook%' or LOWER(utm_source) like '%instagram%' or LOWER(utm_source) = 'fb'
    or LOWER(utm_medium) like '%acebook%' or LOWER(utm_medium) like '%instagram%' or LOWER(utm_medium)  = 'fb'
     then 'Facebook'
    when LOWER(utm_source) like '%instagram%' or  LOWER(utm_medium) like '%instagram%' then 'Instagram'
    when LOWER(utm_source) like '%google%' or  LOWER(utm_medium) like '%google%' then 'Google-ads'
    when LOWER(utm_source) like '%email%' or  LOWER(utm_medium) like '%email%' then 'Email'
    when LOWER(utm_source) like '%blog%' or  LOWER(utm_medium) like '%blog%' then 'Blog'
    when LOWER(utm_source) like '%linkedin%' or  LOWER(utm_medium) like '%linkedin%' then 'Linkedin'
    when LOWER(utm_source) like '%youtube%' or  LOWER(utm_medium) like '%youtube%' then 'Youtube'
    when LOWER(utm_source) like '%social%' or  LOWER(utm_medium) like '%social%' then 'Social'
    when LOWER(utm_source) like '%partners%' or  LOWER(utm_medium) like '%partners%' then 'Partners'
    when LOWER(utm_medium) like '%sponsored%' or LOWER(utm_medium) like '%event_folder%' or  LOWER(utm_medium) like '%content%' 
    or (utm_medium is Null and utm_source is not null)  then 'Unrecognised_source' 
    else utm_medium 
    end as friendly_source, 
    
    row_number() Over(partition by user_id order by visit_ts_msk ) number_visit,
    case when row_number() Over(partition by user_id order by visit_ts_msk ) = 1 
        then True else False end as first_visit_flag, 
    case when row_number() Over(partition by user_id order by visit_ts_msk  desc) = 1 
        then True else False end as last_visit_flag,
    data_source

from
 all_visits
