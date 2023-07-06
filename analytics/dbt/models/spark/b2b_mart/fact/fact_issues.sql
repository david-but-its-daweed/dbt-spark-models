{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'bigquery_load': 'true'
    }
) }}


select 
    issue_id,
    type,
    type_id,
    assignee_id,
    assignee.email as assignee_email,
    assignee.role as assignee_role,
    created_time,
    entity_id,
    start_time,
    etms,
    end_time,
    issue_friendly_id,
    parent_issue_id,
    priority,
    reporter_id,
    reporter.email as reporter_email,
    reporter.role as reporter_role,
    status_time,
    moderator_id,
    status,
    status_id
from
(
select 
    _id as issue_id,
    type as type_id,
    key_type as type,
    assigneeId as assignee_id,
    millis_to_ts_msk(ctms) as created_time,
    entityId as entity_id,
    start_time,
    etms,
    millis_to_ts_msk(etms) as end_time,
    friendlyId as issue_friendly_id,
    parentId as parent_issue_id,
    priority,
    reporterId as reporter_id,
    millis_to_ts_msk(history.ctms) as status_time,
    history.moderatorId as moderator_id,
    key.id as status_id,
    key.status,
    row_number() over (partition by _id order by history.ctms desc) as rn
from 
(
    select 
        i.* , 
        millis_to_ts_msk(startTime) as start_time,
        explode(statusHistory) as history,
        t.type as key_type
        from {{ ref('scd2_issues_snapshot') }} i
        left join {{ ref('key_issue_type') }} t on i.type = t.id
) i
left join (
    select distinct id, status 
    from {{ ref('key_issue_status') }}
) key on i.history.status = key.id
) issues
left join (
    select distinct admin_id, email, role
    from {{ ref('dim_user_admin') }}
) as assignee on issues.assignee_id = assignee.admin_id
left join (
    select distinct admin_id, email, role
    from {{ ref('dim_user_admin') }}
) as reporter on issues.assignee_id = assignee.admin_id
where rn = 1
