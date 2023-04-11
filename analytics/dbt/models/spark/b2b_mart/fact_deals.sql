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
       and date(FROM_UNIXTIME(payload.updatedTime/1000)) <= date'{{ var("start_date_ymd") }}'
     {% else %}
       and partition_date  <= date'{{ var("start_date_ymd") }}'
     {% endif %}
)
),

statuses as 
(select 'RequestRetrieval' as status,
10 as status_int
union all
select 'TrialPricing' as status,
20 as status_int
union all
select 'WaitingTrialPricingFeedback' as status,
30 as status_int
union all
select 'RFQ' as status,
40 as status_int
union all
select 'PreparingSalesQuote' as status,
50 as status_int
union all
select 'WaitingSalesQuoteFeedback' as status,
60 as status_int
union all
select 'FormingOrder' as status,
70 as status_int
union all
select 'SigningAndWaitingForPayment' as status,
80 as status_int
union all
select 'ManufacturingAndDelivery' as status,
90 as status_int
union all
select 'DealCompleted' as status,
100 as status_int
union all
select 'PriceTooHigh' as status,
110 as status_int
union all
select 'ClientNoResponse' as status,
120 as status_int
union all
select 'ProductNotFound' as status,
130 as status_int
union all
select 'ImpossibleToDeliver' as status,
140 as status_int
union all
select 'UnsuitablePartnershipTerms' as status,
150 as status_int
union all
select 'Other' as status,
160 as status_int
union all
select 'GoingIntoDetails' as status,
170 as status_int
union all
select 'InProgress' as status,
180 as status_int
union all
select 'Completed' as status,
190 as status_int
union all
select 'Failed' as status,
200 as status_int
union all
select 'Cancelled' as status,
210 as status_int
union all
select 'UnableToWork' as status,
220 as status_int),

deal_status as (
select distinct 
deal_id, status, status_int, current_date, min_date
from
(select distinct
first_value(status) over (partition by deal_id order by date desc) as current_status,
max(date) over (partition by deal_id order by date desc) as current_date,
min(date) over (partition by deal_id order by date desc) as min_date,
deal_id
from
(
select deal_id, millis_to_ts_msk(statuses.ctms) as date, 
statuses.status as status, statuses.moderatorId as owner_id
from
(select entityId as deal_id, explode(statusHistory) as statuses, etms
    from {{ source('mongo', 'b2b_core_issues_daily_snapshot') }}
    where type = 4 
    )
    order by date desc, deal_id
)
) d left join statuses s on s.status_int = d.current_status
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

select d.*,
ds.status, ds.status_int, ds.current_date, ds.min_date
from
(
select 
deal_id, 
deal_type, 
estimated_date,
estimated_gmv,
d.interaction_id,
gmv.owner_email,
gmv.owner_role,
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
name as deal_name,
updated_date,
userId as user_id,
date('{{ var("start_date_ymd") }}') as partition_date_msk
from deals
where rn = 1
) d left join gmv on d.interaction_id = gmv.interaction_id
group by deal_id, 
deal_type, 
estimated_date,
estimated_gmv,
d.interaction_id,
gmv.owner_email,
gmv.owner_role,
deal_name,
updated_date,
user_id,
partition_date_msk
) d left join deal_status ds on d.deal_id = ds.deal_id
