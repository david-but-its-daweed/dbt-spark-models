{{ config(
    schema='b2b_mart',
    materialized='incremental',
    partition_by=['partition_date_msk'],
    incremental_strategy='insert_overwrite',
    file_format='parquet',
    meta = {
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'bigquery_partitioning_date_column': 'partition_date_msk'
    }
) }}

with interactions_gmv as (
  select 
  fi.user_id,
  g.retention,
  fi.source,
  fi.type,
  fi.campaign,
  t as date_payed,
  gmv_initial,
  dim,
  country,
  promocode_id
  from
(
select user_id, 
max(case when last_interaction_type then source end) as source,
max(case when last_interaction_type then type end) as type,
max(case when last_interaction_type then campaign end) as campaign,
max(interaction_create_date) as day,
max(promocode_id) as promocode_id,
max(country) as country
from {{ ref('fact_attribution_interaction') }}
where last_interaction_type
group by user_id
)fi
left join (
  select distinct 
    order_id,
    retention,
    user_id,
    gmv_initial,
    t,
    'payed' as dim
    from {{ ref('gmv_by_sources') }}
    union all 
    select distinct 
    deal_id,
    retention,
    user_id,
    estimated_gmv as gmv,
    current_date,
    case when status_int >= 10 and status_int <= 50 then 'upside'
    when status_int >= 60 and status_int <= 60 then 'forecast'
    when status_int >= 70 and status_int <= 80 then 'commited'
    end as dim
from {{ ref('fact_deals') }}
where partition_date_msk = (select max(partition_date_msk) from {{ ref('fact_deals') }})
) g on fi.user_id = g.user_id
),

partners_data as (
select distinct 
  quarter,
  sdr.admin_id as admin_id,
  a.email,
  a.role,
  hypothesis_name,
  plan_gmv,
  plan_leads,
  plan_converted_leads,
  p.promocode_id,
  code,
  promocode_owner_id,
  partner_type,
  created_time_msk,
  date(created_time_msk) >= quarter and date(created_time_msk) < quarter + interval 3 month as this_period_partner,
  date(user_created_time) >= quarter and date(user_created_time) < quarter + interval 3 month as this_period_user,
  i.user_id,
  i.funnel_status,
  ig.date_payed,
  ig.gmv_initial,
  ig.dim,
  partition_date_msk
from {{ ref('sdr_plans') }} sdr
left join (select distinct admin_id, email, role from {{ ref('dim_user_admin') }}) a on sdr.admin_id = a.admin_id
left join (
  select distinct 
    promocode_id,
    code,
    promocode_owner_id,
    owner_id,
    partner_type,
    created_time_msk,
    source, type, campaign
  from {{ ref('fact_sdr_promocodes') }}
) p on hypothesis_name = campaign -- and a.admin_id = p.owner_id
left join (
  select distinct 
    user_id,
    user_created_time,
    funnel_status,
    promocode_id
  from {{ ref('fact_attribution_interaction') }}
) i on p.promocode_id = i.promocode_id
left join interactions_gmv ig on p.promocode_id = ig.promocode_id 
and date(date_payed) >= quarter and date(date_payed) < quarter + interval 3 month 
and ig.user_id = i.user_id
where sdr.partition_date_msk = (select max(sdr.partition_date_msk) from {{ ref('sdr_plans') }} sdr)
)

select
  quarter,
  admin_id,
  email,
  role,
  hypothesis_name,
  max(plan_gmv) as plan_gmv,
  max(plan_leads) as plan_leads,
  max(plan_converted_leads) as plan_converted_leads,
  count(distinct case when this_period_partner and partner_type = "Partner" then promocode_owner_id end) as new_partners,
  count(distinct case when partner_type = "Partner" then promocode_owner_id end) as partners,
  count(distinct case when this_period_user then user_id end) as new_users,
  count(distinct case when this_period_user and funnel_status = 'Converted' then user_id end) as converted_users,
  count(distinct case when this_period_user and funnel_status = 'Converting' then user_id end) as converting_users,
  sum(case when this_period_user and dim = 'upside' then gmv_initial end) as upside_gmv,
  sum(case when this_period_user and dim = 'forecast' then gmv_initial end) as forecast_gmv,
  sum(case when this_period_user and dim = 'commited' then gmv_initial end) as commited_gmv,
  sum(case when this_period_user and dim = 'payed' then gmv_initial end) as payed_gmv,
  sum(case when dim = 'payed' then gmv_initial end) as total_gmv,
  partition_date_msk
from partners_data
group by 
  quarter,
  admin_id,
  email,
  role,
  hypothesis_name,
  partition_date_msk
