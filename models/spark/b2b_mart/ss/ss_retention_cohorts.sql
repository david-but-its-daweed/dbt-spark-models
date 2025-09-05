{{ config(
    schema='b2b_mart',
    materialized='view',
    partition_by={
         "field": "cohort_date"
    },
    meta = {
      'model_owner' : '@kirill_melnikov',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}

WITH interactions AS (
    SELECT
        user_id,
        friendly_source,
        utm_campaign
    FROM {{ ref('fact_marketing_utm_interactions') }}
    WHERE first_visit_flag
),

ss_users AS (
    SELECT
        user_id,
        questionnaire_grade,
        Marketing_Lead_Type,
        1 AS ss_user
    FROM {{ ref('ss_users_table') }}
),

cohort AS (
    SELECT
        user_id,
        MIN(event_msk_date) AS cohort_date,
        DATE_TRUNC('WEEK', MIN(event_msk_date)) AS cohort_week,
        DATE_TRUNC('QUARTER', MIN(event_msk_date)) AS cohort_quarter,
        DATE_TRUNC('MONTH', MIN(event_msk_date)) AS cohort_month,
        FLOOR(DATEDIFF(DATE_TRUNC('WEEK', CURRENT_DATE()), DATE_TRUNC('WEEK', MIN(event_msk_date))) / 7) AS max_week_number,
        FLOOR(DATEDIFF(DATE_TRUNC('QUARTER', CURRENT_DATE()), DATE_TRUNC('QUARTER', MIN(event_msk_date))) / 92) AS max_quarter_number,
        CAST(MONTHS_BETWEEN(DATE_TRUNC('MONTH', CURRENT_DATE()), DATE_TRUNC('MONTH', MIN(event_msk_date))) AS INT) AS max_month_number
    FROM {{ ref('ss_events_startsession') }}
    WHERE landing IN ('pt-br', 'es-mx') AND bot_flag != 1
    GROUP BY user_id
),

activity AS (
    SELECT
        user_id,
        event_msk_date
    FROM {{ ref('ss_events_startsession') }}
    GROUP BY user_id, event_msk_date
),

activity_week AS (
    SELECT
        cohort.user_id,
        FLOOR(DATEDIFF(event_msk_date, cohort_week) / 7) AS week_number,
        MAX(1) AS is_active
    FROM cohort
    INNER JOIN activity USING (user_id)
    GROUP BY 1, 2
),

activity_quarter AS (
    SELECT
        cohort.user_id,
        FLOOR(DATEDIFF(event_msk_date, cohort_quarter) / 92) AS quarter_number,
        MAX(1) AS is_active
    FROM cohort
    INNER JOIN activity USING (user_id)
    GROUP BY 1, 2
),

activity_month AS (
    SELECT
        cohort.user_id,
        CAST(MONTHS_BETWEEN(event_msk_date, cohort_month) AS INT) AS month_number,
        MAX(1) AS is_active
    FROM cohort
    INNER JOIN activity USING (user_id)
    GROUP BY 1, 2
),

deals AS (
    SELECT
        deal_created_date,
        deal_id,
        user_id,
        CASE
            WHEN deal_type IN ('VIP', 'Big Deal') THEN 'Big Deal'
            ELSE 'Small Deal'
        END AS deal_type,
        COALESCE(final_gmv, 0) AS final_gmv,
        COALESCE(gmv_initial, 0) AS gmv_initial
    FROM {{ ref('fact_deals_with_requests') }}
    WHERE deal_type != 'Sample'
),

base_s AS (
    SELECT
        *,
        CAST(MONTHS_BETWEEN(DATE_TRUNC('MONTH', deal_created_date), cohort_month) AS INT) AS month_number,
        FLOOR(DATEDIFF(DATE_TRUNC('QUARTER', deal_created_date), cohort_quarter) / 92) AS quarter_number,
        FLOOR(DATEDIFF(DATE_TRUNC('WEEK', deal_created_date), cohort_week) / 7) AS week_number
    FROM cohort
    INNER JOIN deals USING (user_id)
    WHERE deal_created_date >= cohort_date
),

agg_week AS (
    SELECT
        user_id,
        week_number,
        deal_type,
        COUNT(deal_id) AS deals,
        SUM(gmv_initial) AS gmv,
        SUM(final_gmv) AS final_gmv
    FROM base_s
    WHERE week_number IS NOT NULL
    GROUP BY user_id, week_number, deal_type
),

agg_quarter AS (
    SELECT
        user_id,
        quarter_number,
        deal_type,
        COUNT(deal_id) AS deals,
        SUM(gmv_initial) AS gmv,
        SUM(final_gmv) AS final_gmv
    FROM base_s
    WHERE quarter_number IS NOT NULL
    GROUP BY user_id, quarter_number, deal_type
),

agg_month AS (
    SELECT
        user_id,
        month_number,
        deal_type,
        COUNT(deal_id) AS deals,
        SUM(gmv_initial) AS gmv,
        SUM(final_gmv) AS final_gmv
    FROM base_s
    WHERE month_number IS NOT NULL
    GROUP BY user_id, month_number, deal_type
),

counter AS (
    SELECT POSEXPLODE(SEQUENCE(0, 500)) AS (x, dummy)  -- sequence от 0 до 500
),

deal_types AS (
    SELECT
        c.user_id,
        d.deal_type
    FROM cohort c
    LEFT JOIN (SELECT DISTINCT user_id, deal_type FROM deals) d USING (user_id)
),

week_retention AS (
    SELECT
        'week' AS retention_detalization,
        cohort.*,
        dt.deal_type,
        x AS period_number,
        COALESCE(is_active, 0) AS is_active,
        COALESCE(agg_week.deals, 0) AS deals,
        COALESCE(agg_week.gmv, 0) AS gmv
    FROM cohort
    INNER JOIN counter ON x <= max_week_number
    LEFT JOIN deal_types AS dt ON cohort.user_id = dt.user_id
    LEFT JOIN agg_week
        ON
            cohort.user_id = agg_week.user_id
            AND agg_week.week_number = x
            AND agg_week.deal_type <=> dt.deal_type
    LEFT JOIN activity_week
        ON
            cohort.user_id = activity_week.user_id
            AND activity_week.week_number = x
),

quarter_retention AS (
    SELECT
        'quarter' AS retention_detalization,
        cohort.*,
        dt.deal_type,
        x AS period_number,
        COALESCE(is_active, 0) AS is_active,
        COALESCE(agg_quarter.deals, 0) AS deals,
        COALESCE(agg_quarter.gmv, 0) AS gmv
    FROM cohort
    INNER JOIN counter ON x <= max_quarter_number
    LEFT JOIN deal_types dt ON cohort.user_id = dt.user_id
    LEFT JOIN agg_quarter
        ON
            cohort.user_id = agg_quarter.user_id
            AND agg_quarter.quarter_number = x
            AND agg_quarter.deal_type <=> dt.deal_type
    LEFT JOIN activity_quarter
        ON
            cohort.user_id = activity_quarter.user_id
            AND activity_quarter.quarter_number = x
),

month_retention AS (
    SELECT
        'month' AS retention_detalization,
        cohort.*,
        dt.deal_type,
        x AS period_number,
        COALESCE(is_active, 0) AS is_active,
        COALESCE(agg_month.deals, 0) AS deals,
        COALESCE(agg_month.gmv, 0) AS gmv
    FROM cohort
    INNER JOIN counter ON x <= max_month_number
    LEFT JOIN deal_types dt ON cohort.user_id = dt.user_id
    LEFT JOIN agg_month
        ON
            cohort.user_id = agg_month.user_id
            AND agg_month.month_number = x
            AND agg_month.deal_type <=> dt.deal_type
    LEFT JOIN activity_month
        ON
            cohort.user_id = activity_month.user_id
            AND activity_month.month_number = x
),

data_ AS (
    SELECT * FROM month_retention
    UNION ALL
    SELECT * FROM week_retention
    UNION ALL
    SELECT * FROM quarter_retention
)

SELECT
    data_.*,
    friendly_source,
    utm_campaign,
    questionnaire_grade,
    Marketing_Lead_Type,
    COALESCE(ss_user, 0) AS ss_user
FROM data_
LEFT JOIN interactions USING (user_id)
LEFT JOIN ss_users USING (user_id)
