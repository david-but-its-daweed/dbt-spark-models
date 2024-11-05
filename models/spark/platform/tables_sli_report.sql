{{ config(
    tags=['data_readiness'],
    meta = {
      'model_owner' : '@analytics.duty',
      'bigquery_load': 'true',
    },
    schema='platform',
    materialized='view'
) }}

WITH data_readiness_aggregate (
    SELECT
        source_id,
        input_name,
        input_type,
        partition_date AS date,
        day_of_week_no,
        MIN(ready_time_hours) AS ready_time_hours
    FROM {{ ref("data_readiness") }}
    WHERE
        partition_date > NOW() - INTERVAL 3 MONTHS
        AND partition_date <= TO_DATE(NOW())
        AND input_rank = 1
    GROUP BY source_id, input_name, input_type, partition_date, day_of_week_no
),

prepared_data (
    SELECT
        data_readiness.source_id,
        details_all.business_name,
        data_readiness.date,
        COALESCE(details_days.dow, -1) AS dow,
        details_all.alert_channels,
        COALESCE(details_days.target_sli, details_all.target_sli) AS target_sli,
        details_all.owner,
        details_all.description,
        COALESCE(details_days.priority, details_all.priority) AS priority,
        MAX(data_readiness.ready_time_hours) AS ready_time_hours,
        MAX(COALESCE(details_days.expected_time_utc_hours, details_all.expected_time_utc_hours)) AS expected_time_utc_hours
    FROM data_readiness_aggregate AS data_readiness
    INNER JOIN {{ ref("slo_details") }} AS details_all
        ON
            data_readiness.source_id = details_all.slo_id
            AND details_all.dow IS NULL
    LEFT JOIN {{ ref("slo_details") }} AS details_days
        ON
            data_readiness.source_id = details_days.slo_id
            AND data_readiness.day_of_week_no = details_days.dow
    WHERE COALESCE(details_days.expected_time_utc_hours, details_all.expected_time_utc_hours) IS NOT NULL
    GROUP BY
        data_readiness.source_id,
        details_all.business_name,
        data_readiness.date,
        COALESCE(details_days.dow, -1),
        details_all.alert_channels,
        COALESCE(details_days.target_sli, details_all.target_sli),
        details_all.owner,
        details_all.description,
        COALESCE(details_days.priority, details_all.priority)
),

data_3_month AS (
    SELECT
        source_id,
        dow,
        business_name,
        target_sli,
        alert_channels,
        owner,
        description,
        priority,
        MAX(expected_time_utc_hours) AS expected_time_utc_hours,
        SUM(
            CASE
                WHEN ready_time_hours > expected_time_utc_hours THEN 0
                ELSE 1
            END
        ) AS successes,
        COUNT(DISTINCT date) AS days
    FROM prepared_data
    GROUP BY source_id, dow, business_name, target_sli, alert_channels, owner, description, priority
),

data_month AS (
    SELECT
        source_id,
        dow,
        business_name,
        target_sli,
        alert_channels,
        owner,
        description,
        priority,
        SUM(
            CASE
                WHEN ready_time_hours > expected_time_utc_hours THEN 0
                ELSE 1
            END
        ) AS successes,
        COUNT(DISTINCT date) AS days
    FROM prepared_data
    WHERE date > NOW() - INTERVAL 1 MONTHS
    GROUP BY source_id, dow, business_name, target_sli, alert_channels, owner, description, priority
),

data_week AS (
    SELECT
        source_id,
        dow,
        business_name,
        target_sli,
        alert_channels,
        owner,
        description,
        priority,
        SUM(
            CASE
                WHEN ready_time_hours > expected_time_utc_hours THEN 0
                ELSE 1
            END
        ) AS successes,
        COUNT(DISTINCT date) AS days
    FROM prepared_data
    WHERE date > NOW() - INTERVAL 1 WEEKS
    GROUP BY source_id, dow, business_name, target_sli, alert_channels, owner, description, priority
)

SELECT
    source_id,
    CASE
        WHEN data_3_month.dow = 1 THEN 'Monday'
        WHEN data_3_month.dow = 2 THEN 'Tuesday'
        WHEN data_3_month.dow = 3 THEN 'Wednesday'
        WHEN data_3_month.dow = 4 THEN 'Thursday'
        WHEN data_3_month.dow = 5 THEN 'Friday'
        WHEN data_3_month.dow = 6 THEN 'Saturday'
        WHEN data_3_month.dow = 7 THEN 'Sunday'
        ELSE ''
    END AS day_of_week,
    data_3_month.business_name,
    data_3_month.priority,
    data_week.successes / data_week.days * 100 AS sli_last_week,
    data_month.successes / data_month.days * 100 AS sli_last_month,
    data_3_month.successes / data_3_month.days * 100 AS sli__3_months,
    data_3_month.target_sli,
    data_3_month.expected_time_utc_hours,
    data_3_month.alert_channels,
    data_3_month.owner,
    data_3_month.description
FROM data_3_month
LEFT JOIN data_month USING (source_id, dow)
LEFT JOIN data_week USING (source_id, dow)
ORDER BY source_id
