{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'bigquery_load': 'true'
    }
) }}


with 

products as
(select product_id, 
    merchant_order_id, 
    psi_status_id, 
    status,
    type,
    min(time_psi) as ready_for_psi
from
(
select statuses1.status as running_status, statuses1.updatedTime as time_psi, 
    product_id, 
    merchant_order_id, 
    psi_status_id, 
    status,
    type 
from
(
select id as product_id, 
    merchOrdId as merchant_order_id, 
    psiStatusID as psi_status_id, 
    status,
    explode(statuses) as statuses1,
    type 
from {{ source('mongo', 'b2b_core_order_products_daily_snapshot') }}
)
)
where running_status = 10
group by product_id, 
    merchant_order_id, 
    psi_status_id, 
    status,
    type 
),

statuses as (
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
    select status, statusId as status_id, stms, _id, ctxId as merchant_order_id, scndCtxId as product_id
    from {{ source('mongo', 'b2b_core_form_with_status_daily_snapshot') }} f
    left join statuses s on f.statusId = s.s_id
    where type = 10
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

problems as (
    select _id, 
max(case when name = 'problems' and problem.value = 'goodQuality' then 1 else 0 end) as quality,
max(case when name = 'failureProblems' and problem.value = 'goodQuality' then 1 else 0 end) as client_quality,
max(case when name = 'problems' and problem.value = 'customsAndLogisticsRequirements' then 1 else 0 end) as customs,
max(case when name = 'failureProblems' and problem.value = 'customsAndLogisticsRequirements' then 1 else 0 end) as client_customs,
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
    select col.*, _id from (select explode(payloadNew), _id from {{ source('mongo', 'b2b_core_form_with_status_daily_snapshot') }} 
          where payloadNew is not Null)
          ) where name in ('problems', 'failureProblems')
          )
          group by _id),

merchant as (
    select 
        _id as merchant_order_id, 
        orderId as order_id,
        merchantId as merchant_id, 
        manDays as man_days,
        friendlyId as merchant_order_friendly_id
from {{ source('mongo', 'b2b_core_merchant_orders_v2_daily_snapshot') }}
),

not_jp_users AS (
    SELECT DISTINCT order_id, friendly_id
    FROM {{ ref('fact_user_request') }} f
    left join 
        (select distinct order_id, friendly_id, user_id from {{ ref('fact_order') }}) o on f.user_id = o.user_id
    WHERE is_joompro_employee != TRUE OR is_joompro_employee IS NULL
),

table1 as (
select * from (
select product_id, 
    merchant_order_id, 
    psi_status_id,
    product_status,
    type,
    status,
    status_id,
    time,
    order_id,
    merchant_id, 
    merchant_order_friendly_id,
    friendly_id,
    man_days,
    quality,
    client_quality,
    customs,
    client_customs,
    discrepancy,
    client_discrepancy,
    requirements,
    client_requirements,
    other,
    comment,
    client_other,
    client_comment,
    min(case when status_id = 10 then time end) over (partition by product_id, merchant_order_id) as psi_waiting_time,
    min(case when status_id = 20 then time end) over (partition by product_id, merchant_order_id) as psi_start_time,
    min(case when status_id = 30 then time end) over (partition by product_id, merchant_order_id) as psi_ready_time,
    min(case when status_id = 50 then time end) over (partition by product_id, merchant_order_id) as psi_end_time,
    max(case when current_status = 1 then status_id end) over (partition by product_id, merchant_order_id) as current_status,
    lag(status_id) over (partition by product_id, merchant_order_id order by time) as lag_status
from
(
select distinct
    p.product_id, 
    p.merchant_order_id, 
    m.merchant_order_friendly_id,
    case when psi_status_id = psi._id then 1 else 0 end as current_status, 
    psi_status_id,
    p.status as product_status,
    type,
    psi.status,
    psi.status_id,
    stms as time,
    m.order_id,
    m.merchant_id, 
    n.friendly_id,
    m.man_days,
    quality,
    client_quality,
    customs,
    client_customs,
    discrepancy,
    client_discrepancy,
    requirements,
    client_requirements,
    other,
    comment,
    client_other,
    client_comment
from merchant m 
inner join not_jp_users n on m.order_id = n.order_id
left join products p on m.merchant_order_id = p.merchant_order_id
left join psi on p.product_id = psi.product_id and p.merchant_order_id = psi.merchant_order_id
left join problems pr on pr._id = psi._id
where psi_status_id is not null and psi.status_id > 0)
)
where (lag_status != status_id or lag_status is null) and status_id != 50 and psi_start_time is not null),


status_10 as (
select 
    coalesce(lead(time) over (partition by product_id, merchant_order_id order by time), unix_timestamp(current_date())*1000) as lead_time,
    row_number() over (partition by product_id, merchant_order_id order by time) as psi_number,
    time,
    product_id, 
    merchant_order_id,
    status
from
(select distinct
    product_id, 
    merchant_order_id,
    'waiting' as status,
    psi_start_time as time
from table1 where status_id = 10
union all 
select distinct
    product_id, 
    merchant_order_id,
    'fixing' as status,
    time
    from table1 where status_id = 40
)
)


select distinct product_id, 
    merchant_order_id, 
    psi_status_id,
    product_status,
    type,
    status,
    current_status,
    date(from_unixtime(ready_for_psi/1000)) as ready_for_psi,
    date(from_unixtime(psi_start_time/1000)) as psi_start,
    date(from_unixtime(psi_waiting_time/1000)) as psi_waiting,
    date(from_unixtime(psi_ready_time/1000)) as psi_ready,
    date(from_unixtime(psi_end_time/1000)) as psi_end_time,
    date(from_unixtime(waiting_time/1000)) as waiting_time,
    date(from_unixtime(running_time/1000)) as running_time,
    date(from_unixtime(ready_time/1000)) as ready_time,
    int((ready_time/1000 - running_time/1000)/86400) as time_checking,
    date(from_unixtime(failed_time/1000)) as failed_time,
    case when status = 'fixing' then int((running_time/1000 - psi_waiting_time/1000)/86400) end as fixing_time,
    psis,
    psis_order,
    psi_number,
    order_id,
    merchant_id, 
    merchant_order_friendly_id,
    friendly_id,
    man_days,
    quality,
    client_quality,
    customs,
    client_customs,
    discrepancy,
    client_discrepancy,
    requirements,
    client_requirements,
    other,
    comment,
    client_other,
    client_comment
from
(
select 
    product_id, 
    merchant_order_id, 
    psi_status_id,
    product_status,
    type,
    status,
    status_id,
    time,
    current_status,
    date_of_inspection as psi_start_time,
    psi_ready_time,
    psi_waiting_time,
    psi_end_time,
    min(status_10_time) over (partition by merchant_order_id, product_id, psi_number) as waiting_time,
    min(case when status_id = 20 and psi_number = 1 then date_of_inspection when status_id = 20 and psi_number > 1 then time end) 
        over (partition by merchant_order_id, product_id, psi_number) as running_time,
    min(case when status_id = 30 then time end) over (partition by merchant_order_id, product_id, psi_number) as ready_time,
    min(case when status_id = 40 and status_10_time < time then time end) over (partition by merchant_order_id, product_id, psi_number) as failed_time,
    max(psi_number) over (partition by merchant_order_id, product_id) as psis,
    max(psi_number) over (partition by order_id) as psis_order,
    psi_number,
    order_id,
    merchant_id, 
    merchant_order_friendly_id,
    friendly_id,
    man_days,
    max(quality) over (partition by merchant_order_id, product_id, psi_number) as quality,
    max(client_quality) over (partition by merchant_order_id, product_id, psi_number) as client_quality,
    max(customs) over (partition by merchant_order_id, product_id, psi_number) as customs,
    max(client_customs) over (partition by merchant_order_id, product_id, psi_number) as client_customs,
    max(discrepancy) over (partition by merchant_order_id, product_id, psi_number) as discrepancy,
    max(client_discrepancy) over (partition by merchant_order_id, product_id, psi_number) as client_discrepancy,
    max(requirements) over (partition by merchant_order_id, product_id, psi_number) as requirements,
    max(client_requirements) over (partition by merchant_order_id, product_id, psi_number) as client_requirements,
    max(other) over (partition by merchant_order_id, product_id, psi_number) as other,
    max(comment) over (partition by merchant_order_id, product_id, psi_number) as comment,
    max(client_other) over (partition by merchant_order_id, product_id, psi_number) as client_other,
    max(client_comment) over (partition by merchant_order_id, product_id, psi_number) as client_comment,
    ready_for_psi
    from
(
select 
    t.product_id, 
    t.merchant_order_id, 
    t.psi_status_id,
    product_status,
    t.type,
    s.status,
    status_id,
    t.time,
    current_status,
    psi_start_time,
    psi_ready_time,
    psi_waiting_time,
    psi_end_time,
    s.time as status_10_time,
    date_of_inspection,
    psi_number,
    order_id,
    merchant_id, 
    merchant_order_friendly_id,
    friendly_id,
    man_days,
    quality,
    client_quality,
    customs,
    client_customs,
    discrepancy,
    client_discrepancy,
    requirements,
    client_requirements,
    other,
    comment,
    client_other,
    client_comment,
    ready_for_psi
from table1 t
left join status_10 s on s.merchant_order_id = t.merchant_order_id and s.product_id = t.product_id
left join date_of_inspection d on d.merchant_order_id = t.merchant_order_id and d.product_id = t.product_id
left join products p on p.merchant_order_id = t.merchant_order_id and p.product_id = t.product_id
where s.time <= t.time and s.lead_time >= t.time
)
)
