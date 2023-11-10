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
(select status, cast(id as int) as status_int
from  b2b_mart.key_issue_status
),

types as (
select cast(id as int) as type_int, type
from b2b_mart.key_issue_type
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
