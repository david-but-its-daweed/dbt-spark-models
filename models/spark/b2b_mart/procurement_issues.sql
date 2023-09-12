{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}

with statuses as 
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

types as (
select 'CloseTheDeal' as type,
4 as type_int
union all
select 'ClarifyPricesAndPackaging' as type,
5 as type_int
union all
select 'CheckMerchantScheme' as type,
6 as type_int
union all
select 'OfferAdjustment' as type,
7 as type_int
union all
select 'FindOffer' as type,
8 as type_int
union all
select 'RFQHelp' as type,
9 as type_int
)

select * from
(
select distinct
explode(sequence(week_created, week_created + interval 1 month, interval 1 week)) as week,
week_created,
status, 
status_int,
time_change, 
created_time, 
assignee_id,
issue_id, 
entity_id, 
reporter_id, 
type,
type_int,
case when status_int >= 190 then TRUE else FALSE end as closed_deal
from
(
select
s.status, 
s.status_int,
date(millis_to_ts_msk(ctms)) - dayofweek(millis_to_ts_msk(ctms)) + 1 as week_created,
millis_to_ts_msk(change) as time_change, 
millis_to_ts_msk(ctms) as created_time, 
assigneeId as assignee_id,
issue_id, 
entity_id, 
reporterId as reporter_id, 
t.type,
t.type_int,
row_number() over (partition by issue_id order by change desc) as rn
from
(
select distinct statuses.status , statuses.ctms as change, ctms, assigneeId,
    _id as issue_id, entityId as entity_id, reporterId, type
    from
    (
    select *, explode(statusHistory) as statuses, ctms
    from {{ source('mongo', 'b2b_core_issues_daily_snapshot') }}
    where type > 4
    )
) i
left join statuses s on s.status_int = i.status
left join types t on t.type_int = i.type
) t
where rn = 1
) where week <= current_date()
