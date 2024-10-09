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
        date,
        day_of_week_no,
        MIN(ready_time_hours) AS ready_time_hours
    FROM {{ref("data_readiness")}}
    WHERE
        date > NOW() - INTERVAL 3 MONTHS
        AND date <= TO_DATE(NOW())
        AND input_rank = 1
    GROUP BY source_id, input_name, input_type, date, day_of_week_no
),

data (
    SELECT
        data_readiness.source_id,
        details.business_name,
        data_readiness.date,
        coalesce(details.dow, -1) dow,
        details.alert_channels,
        details.target_sli,
        details.owner,
        details.description,
        details.priority,
        MAX(data_readiness.ready_time_hours) AS ready_time_hours,
        MAX(details.expected_time_utc_hours) AS expected_time_utc_hours
    FROM data_readiness_aggregate AS data_readiness
    LEFT JOIN {{ref("slo_details")}} AS details 
      ON data_readiness.source_id = details.slo_id
     and data_readiness.day_of_week_no = coalesce(details.dow, data_readiness.day_of_week_no)
    WHERE expected_time_utc_hours IS NOT NULL
    GROUP BY 
        data_readiness.source_id,
        details.business_name,
        data_readiness.date,
        details.dow,
        details.alert_channels,
        details.target_sli,
        details.owner,
        details.description,
        details.priority
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
    FROM data
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
    FROM data
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
    FROM data
    WHERE date > NOW() - INTERVAL 1 WEEKS
    GROUP BY source_id, dow, business_name, target_sli, alert_channels, owner, description, priority
)

SELECT
    source_id,
    case 
        when data_3_month.dow = 1 then 'Monday'
        when data_3_month.dow = 2 then 'Tuesday'
        when data_3_month.dow = 3 then 'Wednesday'
        when data_3_month.dow = 4 then 'Thursday'
        when data_3_month.dow = 5 then 'Friday'
        when data_3_month.dow = 6 then 'Saturday'
        when data_3_month.dow = 7 then 'Sunday'
        else ''
    end day_of_week,
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
