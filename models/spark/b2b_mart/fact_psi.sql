{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'bigquery_load': 'true'
    }
) }}

with statuses as (
    select "waiting" as status, 10 as s_id
    union all 
    select "running" as status, 20 as s_id
    union all 
    select "ready" as status, 30 as s_id
    union all 
    select "fail" as status, 40 as s_id
    union all 
    select "success" as status, 50 as s_id
),

psi as (
select distinct
    status, status_id, stms as time, _id, merchant_order_id, product_id, lag_status,
    sum(case when lag_status = 40 or lag_status is null then 1 else 0 end) over (partition by merchant_order_id, product_id order by stms) as psi_number,
    sum(case when lag_status = 40 or lag_status is null then 1 else 0 end) over (partition by merchant_order_id, product_id) as psis
from
(
select 
    status, status_id, stms, _id, merchant_order_id, product_id, 
    lag(status_id) over (partition by merchant_order_id, product_id order by stms) as lag_status
from 
(
    select distinct status, statusId as status_id, stms, _id, ctxId as merchant_order_id, scndCtxId as product_id
    from {{ source('mongo', 'b2b_core_form_with_status_daily_snapshot') }} f
    left join statuses s on f.statusId = s.s_id
    where type = 10
)
)
where status_id != lag_status or lag_status is null
),

problems as (
    select _id, 
max(case when name = 'problems' and problem.value = 'goodQuality' then 1 else 0 end) as quality,
max(case when name = 'failureProblems' and problem.value = 'goodQuality' then 1 else 0 end) as client_quality,
max(case when name = 'problems' and problem.value = 'customsAndLogisticsRequirements' then 1 else 0 end) as customs_and_logistics,
max(case when name = 'failureProblems' and problem.value = 'customsAndLogisticsRequirements' then 1 else 0 end) as client_customs_and_logistics,
max(case when name = 'problems' and problem.value = 'customsRequirements' then 1 else 0 end) as customs,
max(case when name = 'failureProblems' and problem.value = 'customsRequirements' then 1 else 0 end) as client_customs,
max(case when name = 'problems' and problem.value = 'logisticsRequirements' then 1 else 0 end) as logistics,
max(case when name = 'failureProblems' and problem.value = 'logisticsRequirements' then 1 else 0 end) as client_logistics,
max(case when name = 'problems' and problem.value = 'discrepancy' then 1 else 0 end) as discrepancy,
max(case when name = 'failureProblems' and problem.value = 'discrepancy' then 1 else 0 end) as client_discrepancy,
max(case when name = 'problems' and problem.value = 'clientsRequirements' then 1 else 0 end) as requirements,
max(case when name = 'failureProblems' and problem.value = 'clientsRequirements' then 1 else 0 end) as client_requirements,
max(case when name = 'problems' and problem.value = 'other' then 1 else 0 end) as other,
max(case when name = 'problems' and problem.value = 'other' then problem.comment end) as comment,
max(case when name = 'failureProblems' and problem.value = 'other' then 1 else 0 end) as client_other,
max(case when name = 'failureProblems' and problem.value = 'other' then problem.comment end) as client_comment
from(
select explode(enumPayload.selectedItems) as problem, name, _id from (
    select col.*, _id from (select explode(payloadNew), _id 
    from {{ source('mongo', 'b2b_core_form_with_status_daily_snapshot') }}
          where payloadNew is not Null)
          ) where name in ('problems', 'failureProblems')
          )
          group by _id),
          
psi_stat as (
    select 
        product_id, 
        merchant_order_id,
        min(case when status_id = 10 then time end) as psi_waiting_time,
        min(case when status_id = 20 then time end) as psi_start_time,
        min(case when status_id = 30 then time end) as psi_ready_time,
        min(case when status_id = 40 then time end) as psi_failed_time,
        max(case when status_id = 50 then time end) as psi_end_time
    from psi
    group by 
        product_id, 
        merchant_order_id
        ),
          
all_psi as (
select a.*, psi_waiting_time, psi_start_time, psi_ready_time, psi_failed_time, psi_end_time
from 
(
select 
    psi.product_id, 
    psi.merchant_order_id, 
    psi._id as psi_id,
    psi_number,
    psis,
    min(case when status_id = 10 then time end) as waiting_time,
    min(case when status_id = 20 then time end) as running_time,
    min(case when status_id = 30 then time end) as ready_time,
    min(case when status_id = 40 then time end) as failed_time,
    max(case when status_id = 50 then time end) as end_time,
    max(quality) as quality,
    max(client_quality) as client_quality,
    max(customs_and_logistics) as customs_and_logistics,
    max(client_customs_and_logistics) as client_customs_and_logistics,
    max(customs) as customs,
    max(client_customs) as client_customs,
    max(logistics) as logistics,
    max(client_logistics) as client_logistics,
    max(discrepancy) as discrepancy,
    max(client_discrepancy) as client_discrepancy,
    max(requirements) as requirements,
    max(client_requirements) as client_requirements,
    max(other) as other,
    max(comment) as comment,
    max(client_other) as client_other,
    max(client_comment) as client_comment
from psi 
left join problems pr on pr._id = psi._id
group by 
    psi.product_id, 
    psi.merchant_order_id, 
    psi_number,
    psis
    ) a 
    left join psi_stat b on a.product_id = b.product_id and a.merchant_order_id = b.merchant_order_id
),

products as (
    select product_id, 
    merchant_order_id, 
    min(case when status = 'readyForPsi' then event_ts_msk end) as ready_for_psi
    from
    (select 
        merchant_order_id,
        product_id,
        status,
        event_ts_msk
    from {{ ref('statuses_events') }}
    where entity = 'product'
    )
    group by product_id, 
    merchant_order_id
 ),

date_of_inspection as (
    select merchant_order_id, product_id, unix_timestamp(Timestamp(min(col.datePayload.value)))*1000 as date_of_inspection
from
    (select statusId as status_id, stms, _id, ctxId as merchant_order_id, scndCtxId as product_id, explode(payloadNew)
    from {{ source('mongo', 'b2b_core_form_with_status_daily_snapshot') }} f
    where type = 10
    )
    where col.type = 'date' and col.name = 'dateOfInspection' and col.datePayload is not null
group by merchant_order_id, product_id
    ),

solution as (
    select col.enumPayload.selectedItems[0].value as solution, _id
    from (
    select explode(payloadNew), _id
    from {{ source('mongo', 'b2b_core_form_with_status_daily_snapshot') }}
    where payloadNew is not Null)
    where col.name = 'solution'
)


select 
    t.product_id, 
    t.merchant_order_id, 
    a.psi_status_id,
    statuses.status as current_status,
    date(ready_for_psi) as ready_for_psi,
    date(coalesce(
        from_unixtime(date_of_inspection/1000),
        from_unixtime(psi_ready_time/1000),
        from_unixtime(psi_start_time/1000)
            )
    ) as psi_start,
    date(from_unixtime(psi_waiting_time/1000)) as psi_waiting,
    date(from_unixtime(psi_ready_time/1000)) as psi_ready,
    date(from_unixtime(psi_failed_time/1000)) as psi_failed,
    date(from_unixtime(psi_end_time/1000)) as psi_end_time,
    date(from_unixtime(waiting_time/1000)) as waiting_time,
    date(from_unixtime(running_time/1000)) as running_time,
    date(from_unixtime(ready_time/1000)) as ready_time,
    int((ready_time/1000 - running_time/1000)/86400) as time_checking,
    date(from_unixtime(failed_time/1000)) as failed_time,
    psi_number,
    psi_id,
    solution as vtrust_solution,
    max(psi_number) over (partition by t.merchant_order_id, t.product_id) as psis,
    max(psi_number) over (partition by order_id) as psis_order,
    order_id,
    order_friendly_id,
    merchant_id, 
    merchant_order_friendly_id,
    quality,
    client_quality,
    customs_and_logistics,
    client_customs_and_logistics,
    customs,
    client_customs,
    logistics,
    client_logistics,
    discrepancy,
    client_discrepancy,
    requirements,
    client_requirements,
    other,
    comment,
    client_other,
    client_comment
from all_psi t
left join {{ ref('fact_order_product_deal') }} a on a.merchant_order_id = t.merchant_order_id and a.product_id = t.product_id
left join statuses on a.psi_status = statuses.s_id
left join date_of_inspection d on d.merchant_order_id = t.merchant_order_id and d.product_id = t.product_id
left join solution s on s._id = psi_id
left join products p on p.merchant_order_id = t.merchant_order_id and p.product_id = t.product_id
