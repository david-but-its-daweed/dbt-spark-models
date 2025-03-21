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


WITH customers AS (
    SELECT _id,
           initialGrade,
           ARRAY_UNION(
               COALESCE(gradeInfoHistory, ARRAY()),
               ARRAY(STRUCT(gradeInfo.grade, gradeInfo.moderatorId, gradeInfo.prob, gradeInfo.reason, gradeInfo.utms))
           ) AS gradeInfoHistory
    FROM {{ source('mongo', 'b2b_core_customers_daily_snapshot') }}
)


SELECT _id AS user_id,
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
       LEAD(updated_ts) OVER (PARTITION BY _id ORDER BY updated_ts) AS grade_to_ts
FROM (
    SELECT
        _id,
        initialGrade AS grade,
        CURRENT_TIMESTAMP() + INTERVAL '3 hour' AS updated_ts
    FROM customers
    WHERE SIZE(gradeInfoHistory) = 0
    UNION ALL
    SELECT 
        _id,
        grade_info.grade AS grade,
        millis_to_ts_msk(grade_info.utms) AS updated_ts
    FROM (
        SELECT 
            _id,
            initialGrade,
            EXPLODE(gradeInfoHistory) AS grade_info
        FROM customers
    ) AS history
    UNION ALL
    SELECT 
        _id,
        initialGrade AS grade,
        MIN(millis_to_ts_msk(grade_info.utms)) - INTERVAL '1 minute' AS updated_ts
    FROM (
        SELECT
            _id,
            initialGrade,
            EXPLODE(gradeInfoHistory) AS grade_info
        FROM customers
    ) AS history
    GROUP BY 1,2
) AS m
ORDER BY 1,3
