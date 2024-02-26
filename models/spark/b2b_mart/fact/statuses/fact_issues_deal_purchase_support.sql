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
min(case when status = 'InProgress' then event_ts_msk end) as in_progress_ts,
min(case when status = 'Done' then event_ts_msk end) as done_ts
from {{ ref('fact_issues_statuses') }}
where team is not null and type = 'DealPurchaseSupport'
group by issue_id, team
),


issues as (
select
    issue_id,
    type,
    assignee_id,
    assignee_email,
    assignee_role,
    created_time,
    start_time,
    entity_id as customer_request_id,
    team.team,
    issue_friendly_id
from {{ ref('fact_issues') }}
where type = 'FindOffer' and team.team is not null
and next_effective_ts_msk is null
), 

customer_requests as 
(
  select
    deal_id,
    user_id,
    customer_request_id,
    status,
    reject_reason,
    planned_offer_cost,
    planned_offer_currency
  from {{ ref('fact_customer_requests') }}
  where next_effective_ts_msk is null
),

deal as (
select
    issue_friendly_id as deal_friendly_id,
    deal_id,
    status as deal_status,
    status_int as deal_status_int
from {{ ref('fact_deals') }}
where next_effective_ts_msk is null)

select distinct
    deal_id,
    deal_friendly_id,
    user_id,
    country,
    customer_request_id,
    planned_offer_cost,
    planned_offer_currency,
    status as customer_request_status,
    reject_reason,
    deal_status,
    deal_status_int,
    issue_id,
    issue_friendly_id,
    type,
    assignee_id,
    assignee_email,
    assignee_role,
    assignee_ts,
    created_time,
    start_time,
    in_progress_ts,
    done_ts
from
issues 
left join statuses using (issue_id, team)
left join customer_requests using (customer_request_id)
left join deal using (deal_id)
join (select distinct user_id, country from {{ ref('fact_customers') }}) using (user_id)
left join (select assignee_ts, issue_id from {{ ref('fact_issues_assignee_history') }}
where current_assignee) using (issue_id)
