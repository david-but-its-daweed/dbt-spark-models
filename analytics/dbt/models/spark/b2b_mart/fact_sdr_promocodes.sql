{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'bigquery_load': 'true'
    }
) }}


with customers as (
select
    fc.user_id, 
    fc.country, 
    fc.owner_id, 
    fc.owner_email, 
    fc.owner_role, 
    fc.last_name,
    fc.first_name,
    fc.company_name,
    du.is_partner,
    fc.partner_type,
    fc.partner_source
from {{ ref('fact_customers') }} fc
left join {{ ref('dim_user') }} du on fc.user_id = du.user_id
where du.next_effective_ts_msk is null
),

attr as (
select distinct 
    type, source, campaign, user_id
    from {{ ref('fact_attribution_interaction') }}
    where last_interaction_type
    )
    


select distinct 
_id as promocode_id,
code,
millis_to_ts_msk(ctms) as created_time_msk,
isActive as is_active,
ownerId as promocode_owner_id,
country, 
owner_id, 
owner_email, 
owner_role, 
last_name,
first_name,
company_name,
is_partner,
partner_type,
partner_source,
type, source, campaign,
day
from
(
select *, explode(sequence(to_date('2022-03-01'), to_date(CURRENT_DATE()), interval 1 day)) as day 
from {{ ref('scd2_promocodes_snapshot') }}
) p
left join customers c on p.ownerId = c.user_id
left join attr a on a.user_id = p.ownerId
where date(millis_to_ts_msk(ctms)) <= day and 
day >=  date(dbt_valid_from) and (day < date(dbt_valid_to) or dbt_valid_to is null)
