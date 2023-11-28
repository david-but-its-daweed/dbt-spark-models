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



with admin as (
    select role, admin_id, email from {{ ref('dim_user_admin') }}
)



select
    issue_id,
    assignee_history.assigneeId as assignee_id,
    assignee.email as assignee_email,
    assignee.role as assignee_role,
    millis_to_ts_msk(assignee_history.ctms) as assignee_ts,
    row_number() over (partition by issue_id order by assignee_history.ctms desc) = 1 as current_assignee,
    current_assignee_id,
    current_assignee.email as current_assignee_email,
    current_assignee.role as current_assignee_role,
    issue_created_time,
    entity_id,
    issue_friendly_id,
    issue_parent_id,
    priority,
    reporter_id,
    reporter.email as reporter_email,
    reporter.role as reporter_role,
    issue_start_time,
    team_ts,
    next_team_ts,
    next_team,
    team
from
(
select 
    issue_id,
    assignee_history,
    current_assignee_id,
    issue_created_time,
    entity_id,
    issue_friendly_id,
    issue_parent_id,
    priority,
    reporter_id,
    issue_start_time,
    team_ts,
    next_team_ts,
    next_team,
    team
from    
(
select distinct
    _id,
    _id as issue_id,
    assigneeId as current_assignee_id,
    ctms as issue_created_time,
    entityId as entity_id,
    friendlyId as issue_friendly_id,
    parentId as issue_parent_id,
    priority,
    reporterId as reporter_id,
    startTime as issue_start_time,
    millis_to_ts_msk(teams.ctms) as team_ts,
    lead(millis_to_ts_msk(teams.ctms)) over (partition by _id order by millis_to_ts_msk(teams.ctms)) as next_team_ts,
    lead(teams.team) over (partition by _id order by millis_to_ts_msk(teams.ctms)) as next_team,
    teams.team
from
(
select
    *
from {{ ref('scd2_issues_snapshot') }}
left join (
    select
        _id,
        explode(teamHistory) as teams
    from {{ ref('scd2_issues_snapshot') }}
    where type > 4 and dbt_valid_to is null
    ) using (_id)
where type > 4 and dbt_valid_to is null
)
)
left join (
    select
        _id,
        explode(assigneeHistory) as assignee_history
    from {{ ref('scd2_issues_snapshot') }}
    where type > 4 and dbt_valid_to is null
) using (_id)
)
left join admin as assignee on assignee_history.assigneeId = assignee.admin_id
left join admin as current_assignee on current_assignee_id = current_assignee.admin_id
left join admin as reporter on reporter_id = reporter.admin_id
where (millis_to_ts_msk(assignee_history.ctms) >= team_ts or team_ts is null) 
    and (millis_to_ts_msk(assignee_history.ctms) < next_team_ts or next_team_ts is null)
