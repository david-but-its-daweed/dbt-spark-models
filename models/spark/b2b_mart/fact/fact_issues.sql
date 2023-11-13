{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'bigquery_load': 'true'
    }
) }}

WITH admins AS (
    SELECT DISTINCT
        admin_id,
        email,
        role
    FROM {{ ref('dim_user_admin') }}
)
,
assigned AS (
    SELECT
        f._id,
        f.first_assignee_id,
        fa.email AS first_assignee_email,
        fa.role AS first_assignee_role,
        f.first_time_assigned,
        f.last_time_assigned,
        f.times_assigned
    FROM (
        SELECT
            _id,
            first_value(assignee_id) OVER (PARTITION BY _id ORDER BY created_time) AS first_assignee_id,
            MIN(created_time) OVER (PARTITION BY _id) AS first_time_assigned,
            MAX(created_time) OVER (PARTITION BY _id) AS last_time_assigned,
            COUNT(assignee_id) OVER (PARTITION BY _id) AS times_assigned
        FROM (
            SELECT
                _id,
                assignee_history.assigneeId AS assignee_id,
                millis_to_ts_msk(assignee_history.ctms) AS created_time
            FROM (
                SELECT
                    _id,
                    explode(assigneeHistory) AS assignee_history
                FROM {{ ref('scd2_issues_snapshot') }}
            )
        )
    ) AS f
    LEFT JOIN admins AS fa ON f.first_assignee_id = fa.admin_id
)

SELECT DISTINCT
    issues.issue_id,
    issues.type,
    issues.type_id,
    issues.assignee_id,
    assignee.email AS assignee_email,
    assignee.role AS assignee_role,
    issues.created_time,
    issues.entity_id,
    issues.start_time,
    issues.etms,
    issues.end_time,
    issues.issue_friendly_id,
    issues.parent_issue_id,
    issues.priority,
    issues.reporter_id,
    reporter.email AS reporter_email,
    reporter.role AS reporter_role,
    issues.status_time,
    issues.moderator_id,
    issues.status,
    issues.status_id,
    assigned.first_assignee_id,
    assigned.first_assignee_email,
    assigned.first_assignee_role,
    assigned.first_time_assigned,
    assigned.last_time_assigned,
    assigned.times_assigned,
    team,
    teams,
    effective_ts_msk,
    next_effective_ts_msk
FROM (
    SELECT
        i._id AS issue_id,
        i.type AS type_id,
        i.key_type AS type,
        i.assigneeId AS assignee_id,
        millis_to_ts_msk(i.ctms) AS created_time,
        i.entityId AS entity_id,
        i.start_time,
        i.etms,
        millis_to_ts_msk(i.etms) AS end_time,
        i.friendlyId AS issue_friendly_id,
        i.parentId AS parent_issue_id,
        i.priority,
        i.reporterId AS reporter_id,
        millis_to_ts_msk(i.history.ctms) AS status_time,
        i.history.moderatorId AS moderator_id,
        key.id AS status_id,
        key.status,
        element_at(teamHistory, cast((array_position(teamHistory.ctms,
                                       array_max(teamHistory.ctms))) as INTEGER)) as team,
        size(teamHistory) as teams,
        TIMESTAMP(dbt_valid_from) as effective_ts_msk,
        TIMESTAMP(dbt_valid_to) as next_effective_ts_msk
    FROM (
        SELECT
            i.*,
            millis_to_ts_msk(i.startTime) AS start_time,
            element_at(statusHistory, cast((array_position(statusHistory.ctms,
                                       array_max(statusHistory.ctms))) as INTEGER)) as history,
            t.type AS key_type
        FROM {{ ref('scd2_issues_snapshot') }} AS i
        LEFT JOIN {{ ref('key_issue_type') }} AS t ON i.type = t.id
    ) AS i
    LEFT JOIN (
        SELECT DISTINCT
            id,
            status
        FROM {{ ref('key_issue_status') }}
    ) AS key ON i.history.status = key.id
) AS issues
LEFT JOIN admins AS assignee ON issues.assignee_id = assignee.admin_id
LEFT JOIN admins AS reporter ON issues.assignee_id = reporter.admin_id
LEFT JOIN assigned ON issues.issue_id = assigned._id
