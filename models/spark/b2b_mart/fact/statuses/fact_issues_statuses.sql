{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'bigquery_load': 'true'
    }
) }}

with statuses as 
(select status, cast(id as int) as status_int
from  {{ ref('key_issue_status') }}
),

types as (
select cast(id as int) as type_int, type
from {{ ref('key_issue_type') }}
)

select * from
(
select distinct
status, 
status_int,
event_ts_msk,
created_ts_msk, 
assignee_id,
issue_id, 
entity_id, 
reporter_id, 
type,
type_int,
team,
team_assigned_ts,
teams,
case when status_int >= 190 then TRUE else FALSE end as closed_deal
from
(
select
s.status, 
s.status_int,
millis_to_ts_msk(event_ts_msk) as event_ts_msk, 
millis_to_ts_msk(ctms) as created_ts_msk, 
assigneeId as assignee_id,
issue_id, 
entity_id, 
reporterId as reporter_id, 
t.type,
t.type_int,
team.team,
team.ctms as team_assigned_ts,
teams,
row_number() over (partition by issue_id, s.status, team order by event_ts_msk desc) as rn
from
(
    select distinct statuses.status , statuses.ctms as event_ts_msk, ctms, assigneeId,
    _id as issue_id, entityId as entity_id, reporterId, type,
    element_at(teamHistory, cast((array_position(teamHistory.ctms,
                                       array_max(teamHistory.ctms))) as INTEGER)) as team,
    size(teamHistory) as teams
    from
    (
    select *, explode(statusHistory) as statuses, ctms
    from {{ ref('scd2_issues_snapshot') }}
    where type > 4
    )
) i
left join statuses s on s.status_int = i.status
left join types t on t.type_int = i.type
) t
where rn = 1
)
