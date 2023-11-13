{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'bigquery_load': 'true'
    }
) }}



with 
statuses as (select issue_id, team,
min(case when status = 'New' then event_ts_msk end) as new_ts,
min(case when status = 'Cancelled' then event_ts_msk end) as cancelled_ts,
min(case when status = 'InProgress' then event_ts_msk end) as in_progress_ts,
min(case when status = 'Completed' then event_ts_msk end) as completed_ts,
min(case when status = 'Failed' then event_ts_msk end) as failed_ts
from {{ ref('fact_issues_statuses') }}
where team is not null and type = 'FindOffer'
group by issue_id, team
),


issues as (
select issue_id, type, assignee_id, assignee_email, assignee_role, created_time,
start_time, entity_id as customer_request_id, team.team
from {{ ref('fact_issues') }}
where type = 'FindOffer' and team.team is not null
and next_effective_ts_msk is null
), 

customer_requests as 
(
  select deal_id, user_id, customer_request_id, status, reject_reason
  from {{ ref('fact_customer_requests') }}
  where next_effective_ts_msk is null
),

deal as (
select deal_id, status as deal_status, status_int as deal_status_int
from {{ ref('fact_deals') }}
where next_effective_ts_msk is null)

select distinct
    deal_id,
    user_id,
    customer_request_id,
    status as customer_request_status,
    reject_reason,
    deal_status,
    deal_status_int,
    issue_id,
    type,
    assignee_id,
    assignee_email,
    assignee_role,
    team,
    created_time,
    start_time,
    new_ts,
    cancelled_ts,
    in_progress_ts,
    completed_ts,
    failed_ts,
    lead(new_ts) over (partition by deal_id, user_id, customer_request_id, issue_id order by new_ts) as next_team_new_ts,
    lead(team) over (partition by deal_id, user_id, customer_request_id, issue_id order by new_ts) as next_team
from
issues 
left join statuses using (issue_id, team)
left join customer_requests using (customer_request_id)
left join deal using (deal_id)
join (select distinct user_id from {{ ref('fact_customers') }}) using (user_id)
