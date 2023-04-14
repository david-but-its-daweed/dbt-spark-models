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

t as (select distinct 
deal_id, 
status,
min_date,
status_int,
lead(min_date) over (partition by deal_id order by min_date) as next_status_date,
lag(min_date) over (partition by deal_id order by min_date) as prev_status_date,
lead(status) over (partition by deal_id order by min_date) as next_status,
lag(status_int) over (partition by deal_id order by min_date) as prev_status_int,
lead(status_int) over (partition by deal_id order by min_date) as next_status_int,
lag(status) over (partition by deal_id order by min_date) as prev_status
from
(
select distinct 
deal_id, status, status_int, min_date
from
(select distinct
status as current_status,
min(date) over (partition by deal_id, status) as min_date,
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
)
     )

select * from t
