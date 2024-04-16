{{ config(
    tags=['data_readiness'],
    meta = {
      'model_owner' : '@gburg',
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
        date,
        MIN(ready_time_hours) AS ready_time_hours
    FROM platform.data_readiness
    WHERE
        date > NOW() - INTERVAL 3 MONTHS
        AND date <= TO_DATE(NOW())
        AND input_rank = 1
    GROUP BY source_id, input_name, input_type, date
),

data (
    SELECT
        data_readiness.source_id,
        details.business_name,
        data_readiness.date,
        details.alert_channels,
        details.target_sli,
        details.owner,
        details.description,
        MAX(data_readiness.ready_time_hours) AS ready_time_hours,
        MAX(details.expected_time_utc_hours) AS expected_time_utc_hours
    FROM data_readiness_aggregate AS data_readiness
    LEFT JOIN platform_slo.slo_details AS details ON data_readiness.source_id = details.slo_id
    WHERE expected_time_utc_hours IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5, 6, 7
),

data_3_month AS (
    SELECT
        source_id,
        business_name,
        target_sli,
        alert_channels,
        owner,
        description,
        MAX(expected_time_utc_hours) AS expected_time_utc_hours,
        SUM(
            CASE
                WHEN ready_time_hours > expected_time_utc_hours THEN 0
                ELSE 1
            END
        ) AS successes,
        COUNT(DISTINCT date) AS days
    FROM data
    GROUP BY source_id, business_name, target_sli, alert_channels, owner, description
),

data_month AS (
    SELECT
        source_id,
        business_name,
        target_sli,
        alert_channels,
        owner,
        description,
        SUM(
            CASE
                WHEN ready_time_hours > expected_time_utc_hours THEN 0
                ELSE 1
            END
        ) AS successes,
        COUNT(DISTINCT date) AS days
    FROM data
    WHERE date > NOW() - INTERVAL 1 MONTHS
    GROUP BY source_id, business_name, target_sli, alert_channels, owner, description
),

data_week AS (
    SELECT
        source_id,
        business_name,
        target_sli,
        alert_channels,
        owner,
        description,
        SUM(
            CASE
                WHEN ready_time_hours > expected_time_utc_hours THEN 0
                ELSE 1
            END
        ) AS successes,
        COUNT(DISTINCT date) AS days
    FROM data
    WHERE date > NOW() - INTERVAL 1 WEEKS
    GROUP BY 1, 2, 3, 4, 5, 6
)

SELECT
    source_id,
    data_3_month.business_name,
    slo_details.priority,
    data_week.successes / data_week.days * 100 AS sli_last_week,
    data_month.successes / data_month.days * 100 AS sli_last_month,
    data_3_month.successes / data_3_month.days * 100 AS sli__3_months,
    data_3_month.target_sli,
    data_3_month.expected_time_utc_hours,
    data_3_month.alert_channels,
    data_3_month.owner,
    data_3_month.description
FROM data_3_month
LEFT JOIN data_month USING (source_id)
LEFT JOIN data_week USING (source_id)
LEFT JOIN platform_slo.slo_details ON data_3_month.source_id = slo_details.slo_id
ORDER BY source_id
