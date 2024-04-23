{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'bigquery_load': 'true'
    }
) }}



WITH statuses AS (
  SELECT
      issue_id,
      team,
      MIN(CASE WHEN status = 'New' THEN event_ts_msk END) AS new_ts,
      MIN(CASE WHEN status = 'Cancelled' THEN event_ts_msk END) AS cancelled_ts,
      MIN(CASE WHEN status = 'InProgress' THEN event_ts_msk END) AS in_progress_ts,
      MIN(CASE WHEN status = 'Completed' THEN event_ts_msk END) AS completed_ts,
      MIN(CASE WHEN status = 'Failed' THEN event_ts_msk END) AS failed_ts
  FROM {{ ref('fact_issues_statuses') }}
  WHERE
      team IS NOT NULL
      AND type = 'ContactMerchant'
  GROUP BY
      issue_id,
      team
),


issues AS (
    SELECT
        issues.issue_id,
        issues.type,
        issues.assignee_id,
        issues.assignee_email,
        issues.assignee_role,
        issues.created_time,
        issues.start_time,
        issues.entity_id as customer_request_id,
        team.team,
        issues.issue_friendly_id
    FROM {{ ref('fact_issues') }} AS issues
    WHERE
        type = 'ContactMerchant'
        AND team.team IS NOT NULL
        AND next_effective_ts_msk IS NULL
), 

customer_requests AS (
    SELECT
        deal_id,
        user_id,
        customer_request_id,
        status,
        reject_reason,
        planned_offer_cost,
        planned_offer_currency
    FROM {{ ref('fact_customer_requests') }}
    WHERE next_effective_ts_msk IS NULL
),

deal AS (
    SELECT
        issue_friendly_id AS deal_friendly_id,
        deal_id,
        status AS deal_status,
        status_int AS deal_status_int
    FROM {{ ref('fact_deals') }}
    WHERE next_effective_ts_msk IS NULL
),

customers AS (
    SELECT DISTINCT
        user_id,
        country
    FROM {{ ref('fact_customers') }}
),


assignee AS (
    SELECT
        assignee_ts,
        issue_id
    FROM {{ ref('fact_issues_assignee_history') }}
    WHERE current_assignee
)

SELECT DISTINCT
    deal.deal_id,
    deal.deal_friendly_id,
    deal.user_id,
    customers.country,
    issues.customer_request_id,
    customer_requests.planned_offer_cost,
    customer_requests.planned_offer_currency,
    status AS customer_request_status,
    deal.reject_reason,
    deal.deal_status,
    deal.deal_status_int,
    issues.issue_id,
    issues.issue_friendly_id,
    issues.type,
    issues.assignee_id,
    issues.assignee_email,
    issues.assignee_role,
    issues.assignee_ts,
    issues.team,
    issues.created_time,
    statuses.start_time,
    statuses.new_ts,
    statuses.cancelled_ts,
    statuses.in_progress_ts,
    statuses.completed_ts,
    statuses.failed_ts
FROM issues 
LEFT JOIN statuses USING (issue_id, team)
LEFT JOIN customer_requests USING (customer_request_id)
LEFT JOIN deal USING (deal_id)
JOIN customers USING (user_id)
LEFT JOIN assignee USING (issue_id)
