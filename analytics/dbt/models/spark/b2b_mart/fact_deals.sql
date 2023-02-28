{{ config(
    schema='b2b_mart',
    materialized='incremental',
    partition_by=['partition_date_msk'],
    file_format='delta',
    meta = {
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'bigquery_partitioning_date_column': 'partition_date_msk'
    }
) }}

with deals as (select 
dealId , 
dealType, 
date(FROM_UNIXTIME(estimatedEndDate/1000)) as estimated_date,
estimatedGmv.* ,
interactionId,
moderatorId,
name,
date(FROM_UNIXTIME(updatedTime/1000)) as updated_date,
userId,
row_number() over (partition by dealId order by updatedTime desc) as rn
from
(
select payload.* 
from {{ source('b2b_mart', 'operational_events') }}
where type = 'dealChanged'
{% if is_incremental() %}
       and date(FROM_UNIXTIME(updatedTime/1000)) <= date'{{ var("start_date_ymd") }}'
     {% else %}
       and partition_date  <= date'2023-02-27'
     {% endif %}
)
),

admin AS (
    SELECT
        admin_id,
        a.email,
        s.role as owner_role
    FROM {{ ref('dim_user_admin') }} a
    LEFT JOIN {{ ref('support_roles') }} s on a.email = s.email
),

gmv as (
    select distinct
    t as date_payed, 
    g.order_id,
    g.gmv_initial,
    g.initial_gross_profit,
    g.final_gross_profit,
    g.owner_email,
    g.owner_role,
    interaction_id
    FROM {{ ref('gmv_by_sources') }} g
    left join {{ ref('fact_interactions') }} i on g.order_id = i.order_id
)


select 
deal_id, 
deal_type, 
estimated_date,
estimated_gmv,
interaction_id,
owner_email,
owner_role,
deal_name,
updated_date,
user_id,
sum(gmv_initial) as gmv_initial,
sum(initial_gross_profit) as initial_gross_profit,
sum(final_gross_profit) as final_gross_profit,
partition_date_msk
from
(select 
dealId as deal_id, 
dealType as deal_type, 
estimated_date,
amount as estimated_gmv,
interactionId as interaction_id,
email as owner_email,
owner_role,
name as deal_name,
updated_date,
userId as user_id,
date('{{ var("start_date_ymd") }}') as partition_date_msk
from deals left join admin on deals.moderatorId = admin.admin_id
where rn = 1
) left join gmv on deals.interaction_id = gmv.interaction_id
group by deal_id, 
deal_type, 
estimated_date,
estimated_gmv,
interaction_id,
owner_email,
owner_role,
deal_name,
updated_date,
user_id,
partition_date_msk
