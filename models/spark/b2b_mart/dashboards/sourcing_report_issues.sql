{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@abadoyan',
      'bigquery_load': 'true'
    }
) }}


WITH fact_deals AS (
    SELECT deal_id,
           MAX(deal_type) AS deal_type,
           MAX(sourcing_deal_type) AS sourcing_deal_type,
           MAX(CASE WHEN self_service THEN 1 ELSE 0 END) AS self_service
    FROM {{ ref('fact_deals') }}
    WHERE next_effective_ts_msk IS NULL
    GROUP BY 1
),
fact_issues AS (
    SELECT *
    FROM {{ ref('fact_issues') }}
    WHERE next_effective_ts_msk IS NULL
      AND coalesce(assignee_role, 'null') IN ('null', 'SourcingManager', 'MerchantSupport')
),
fact_customer_requests AS (
    SELECT customer_request_id,
           deal_id,
           user_id,
           country,
           category_name,
           status AS customer_request_status,
           planned_offer_cost,
           planned_offer_currency,
           creation_source,
           fake_door,
           manual,
           standart_deal,
           rfq_deal,
           sample,
           merchant_price_currency,
           merchant_price_per_item / 1000000 AS merchant_price_per_item,
           merchant_price_total_amount / 1000000 AS merchant_price_total_amount,
           created_time AS customer_request_created_time,
           updated_time AS customer_request_updated_time
    FROM {{ ref('fact_customer_requests') }}
    WHERE country != 'RU'
      AND DATE(created_time) >= '2024-04-01'
      AND next_effective_ts_msk IS NULL
),
users AS (
    SELECT user_id,
           coalesce(nullIf(current_grade, 'unknown'), grade) AS current_grade
    FROM {{ ref('fact_customers') }}
),
grade_history AS (
    SELECT user_id,
           grade,
           grade_from_ts,
           grade_to_ts
    FROM {{ ref('fact_customers_grade_history') }}
),
sourcing_customer_reqeusts AS (
    SELECT fcr.*,
           fd.self_service,
           coalesce(u.current_grade, 'unknown') AS current_grade
    FROM fact_customer_requests AS fcr
    LEFT JOIN fact_deals AS fd ON fcr.deal_id = fd.deal_id
    LEFT JOIN users AS u ON fcr.user_id = u.user_id
),
teams AS (
    SELECT
        t1.issue_id,
        t1.count_unique_teams,
        t3.assignee_history,
        array_sort(
            collect_list(
                named_struct(
                    'team_ts', t2.team_ts,
                    'team', t2.team
                )
            )
        ) AS team_history
    FROM (
        SELECT issue_id,
               COUNT(DISTINCT team) AS count_unique_teams
        FROM {{ ref('fact_issues_assignee_history') }}
        GROUP BY 1
    ) AS t1
    LEFT JOIN (
        SELECT DISTINCT issue_id,
               team_ts,
               team
        FROM {{ ref('fact_issues_assignee_history') }}
        WHERE team_ts IS NOT NULL
    ) AS t2 ON t1.issue_id = t2.issue_id
    LEFT JOIN (
        SELECT
            issue_id,
            array_sort(
                collect_list(
                    named_struct(
                        'assignee_ts', assignee_ts,
                        'assignee_id', assignee_id,
                        'assignee_email', assignee_email,
                        'assignee_role', assignee_role
                    )
                )
            ) AS assignee_history
        FROM {{ ref('fact_issues_assignee_history') }}
        WHERE assignee_id IS NOT NULL
        GROUP BY 1
    ) AS t3 ON t1.issue_id = t3.issue_id
    GROUP BY 1,2,3
),
issues_ AS (
    SELECT fcr.customer_request_id,
           fcr.deal_id,
           fcr.user_id,
           fcr.country,
           fcr.category_name,
           fcr.customer_request_status,
           fcr.planned_offer_cost,
           fcr.planned_offer_currency,
           fcr.creation_source,
           fcr.fake_door,
           fcr.manual,
           fcr.standart_deal,
           fcr.rfq_deal,
           fcr.sample,
           fcr.customer_request_created_time,
           fcr.customer_request_updated_time,
           fi.issue_id,
           fi.type AS issue_type,
           fi.status AS issue_status,
           fi.reject_reason AS issue_reject_reason,
           fi.created_time AS issue_created_time,
           fi.start_time AS issue_start_time,
           fi.status_time AS issue_status_time,
           CASE WHEN fi.status IN ('Completed', 'Failed') THEN fi.status_time END AS issue_ended_time,
           fi.issue_friendly_id,
           fi.parent_issue_id,
           fi.reporter_email,
           fi.reporter_role,
           fi.first_assignee_email,
           fi.first_assignee_role,
           fi.first_time_assigned,
           fi.assignee_email AS last_assignee_email,
           fi.assignee_role AS last_assignee_role,
           fi.last_time_assigned,
           fi.times_assigned,
           t.assignee_history,
           team_history[0].team AS first_team,
           team_history[0].team_ts AS first_team_ts,
           team_history[size(team_history) - 1].team AS last_team,
           team_history[size(team_history) - 1].team_ts AS last_team_ts,
           GREATEST(size(team_history) - 1, 0) AS count_changes_team,
           t.team_history
    FROM sourcing_customer_reqeusts AS fcr
    /* Задачи сорсеров (в админке) */
    LEFT JOIN fact_issues AS fi ON fcr.customer_request_id = fi.entity_id
    /* Информация по команде по задаче */
    LEFT JOIN teams AS t ON fi.issue_id = t.issue_id
),
issues AS (
    SELECT
        issues_.*,
        fd.self_service,
        fd.deal_type,
        fd.sourcing_deal_type,
        CASE WHEN issue_ended_time IS NOT NULL THEN 1 ELSE 0 END AS is_issue_ended,
        CASE WHEN issue_ended_time IS NOT NULL THEN
            timestampdiff(SECOND, issue_created_time, coalesce(last_time_assigned, issue_ended_time))
        END AS in_queue_second,
        CASE WHEN issue_ended_time IS NOT NULL THEN
            timestampdiff(SECOND, last_time_assigned, issue_ended_time)
        END AS processing_second,
        CASE WHEN issue_ended_time IS NOT NULL THEN
            timestampdiff(SECOND, issue_created_time, issue_ended_time)
        END AS from_create_to_end_second,
        CASE WHEN issue_status = 'Completed' AND issue_type = 'FindOffer' THEN
            CASE
                WHEN sourcing_deal_type = 'vip' THEN 2
                ELSE 1
            END
            ELSE 0
        END AS task_score,
        coalesce(gh.grade, 'unknown') AS grade_by_issue_created_time
    FROM issues_
    LEFT JOIN fact_deals AS fd
        ON issues_.deal_id = fd.deal_id
    LEFT JOIN grade_history AS gh
        ON issues_.user_id = gh.user_id
        AND issues_.issue_created_time BETWEEN gh.grade_from_ts AND coalesce(gh.grade_to_ts, current_timestamp + INTERVAL 3 hour)
)

SELECT *
FROM issues
