{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'model_owner' : '@abadoyan',
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}


WITH main AS (
    WITH customers AS (
        SELECT
            _id,
            initialGrade,
            ARRAY_UNION(
                COALESCE(gradeInfoHistory, ARRAY()),
                CASE
                    WHEN gradeInfo.utms != 0
                        THEN ARRAY(STRUCT(
                            gradeInfo.grade,
                            gradeInfo.moderatorId,
                            gradeInfo.prob,
                            gradeInfo.reason,
                            gradeInfo.utms
                        ))
                    ELSE ARRAY()
                END
            ) AS gradeInfoHistory
        FROM {{ source('mongo', 'b2b_core_customers_daily_snapshot') }}
    ),
         /* Вставляем initialGrade для тех, у кого нет gradeInfoHistory */
         without_grade_history AS (
        SELECT
            _id,
            initialGrade AS grade,
            null AS reason,
            null AS moderator_id,
            CURRENT_TIMESTAMP() + INTERVAL '3 hour' AS updated_ts
        FROM customers
        WHERE SIZE(gradeInfoHistory) = 0
    ),
         /* Разворачиваем gradeInfoHistory */
         grade_history AS (
        SELECT
            _id,
            grade_info.grade,
            nullIf(grade_info.reason, '') AS reason,
            grade_info.moderatorId AS moderator_id,
            MILLIS_TO_TS_MSK(grade_info.utms) AS updated_ts
        FROM (
            SELECT
                _id,
                initialGrade,
                EXPLODE(gradeInfoHistory) AS grade_info
            FROM customers
        ) AS history
    ),
         /* Вставляем initialGrade перед первой записью в gradeInfoHistory*/
         before_grade_history AS (
        SELECT
            _id,
            initialGrade AS grade,
            null AS reason,
            null AS moderator_id,
            MIN(MILLIS_TO_TS_MSK(grade_info.utms)) - INTERVAL '1 minute' AS updated_ts
        FROM (
            SELECT
                _id,
                initialGrade,
                EXPLODE(gradeInfoHistory) AS grade_info
            FROM customers
        ) AS history
        GROUP BY 1, 2
    )



    SELECT
        _id AS user_id,
        CASE
            WHEN grade = 0 THEN 'unknown'
            WHEN grade = 1 THEN 'A'
            WHEN grade = 2 THEN 'B'
            WHEN grade = 3 THEN 'C'
            WHEN grade = 4 THEN 'D'
            ELSE 'unknown'
        END AS grade,
        CASE
            WHEN ROW_NUMBER() OVER (PARTITION BY _id ORDER BY updated_ts) = 1
                THEN TIMESTAMP('2020-01-01 00:00:00')
            ELSE updated_ts
        END AS grade_from_ts,
        LEAD(updated_ts) OVER (PARTITION BY _id ORDER BY updated_ts) AS grade_to_ts,
        reason,
        moderator_id
    FROM (
        SELECT *
        FROM without_grade_history
        UNION ALL
        SELECT *
        FROM grade_history
        UNION ALL
        SELECT *
        FROM before_grade_history
    ) AS m
    ORDER BY 1,3
),
     /* Схлопываем строки в интервалы по user_id, grade, reason, moderator_id, если они идут подряд */
     ordered AS (
    SELECT
        *,
        LAG(grade) OVER (
            PARTITION BY user_id
            ORDER BY grade_from_ts
        ) AS prev_grade,
        LAG(reason) OVER (
            PARTITION BY user_id
            ORDER BY grade_from_ts
        ) AS prev_reason,
        LAG(moderator_id) OVER (
            PARTITION BY user_id
            ORDER BY grade_from_ts
        ) AS prev_moderator
    FROM main
),
     marked AS (
    SELECT
        *,
        CASE
            WHEN grade != prev_grade
              OR reason != prev_reason
              OR moderator_id != prev_moderator
              OR prev_grade IS NULL THEN 1
            ELSE 0
        END AS is_new_group
    FROM ordered
),

     grouped AS (
    SELECT
        *,
        SUM(is_new_group) OVER (
            PARTITION BY user_id
            ORDER BY grade_from_ts
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS group_id
    FROM marked
)


SELECT
    user_id,
    grade,
    reason,
    moderator_id,
    MIN(grade_from_ts) AS grade_from_ts,
    CASE
        WHEN COUNT(*) != COUNT(grade_to_ts) THEN NULL
        ELSE MAX(grade_to_ts)
    END AS grade_to_ts
FROM grouped
GROUP BY 1,2,3,4
ORDER BY
    user_id,
    grade_from_ts
