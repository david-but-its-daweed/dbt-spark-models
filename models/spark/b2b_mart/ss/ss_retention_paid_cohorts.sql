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

WITH ss_users AS (
    SELECT
        user_id,
        questionnaire_grade,
        Marketing_Lead_Type,
        1 AS ss_user
    FROM {{ ref('ss_users_table') }}
),

statuses AS (
    SELECT
        deal_id,
        MAX(1) AS achieved_paid,
        MIN(CAST(event_ts_msk AS DATE)) AS achieved_paid_date
    FROM {{ ref('fact_deals_status_history') }}
    WHERE status_name_small_deal LIKE '%ProcurementConfirmation' OR status_name LIKE '%PaymentToMerchant'
    GROUP BY 1
),

base AS (
    SELECT *
   -- FROM {{ ref('fact_deals_with_requests') }}
    FROM {{ ref('marketing_deals_with_orders') }}
    ---INNER JOIN statuses USING (deal_id)
    WHERE
        achieved_paid_date IS NOT null
        AND deal_type
        IN ('Small Deal', 'Big Deal', 'VIP') AND country IN ('BR', 'MX') AND deal_status NOT IN ('Test')
),

cohort AS (

    SELECT
        user_id,
        country,
        MIN(achieved_paid_date) AS cohort_date,
        DATE_TRUNC('WEEK', MIN(achieved_paid_date)) AS cohort_week,
        DATE_TRUNC('MONTH', MIN(achieved_paid_date)) AS cohort_month,
        DATE_TRUNC('QUARTER', MIN(achieved_paid_date)) AS cohort_quarter,
        FLOOR(DATEDIFF(DATE_TRUNC('WEEK', CURRENT_DATE()), DATE_TRUNC('WEEK', MIN(achieved_paid_date))) / 7) AS max_week_number,
        FLOOR(DATEDIFF(DATE_TRUNC('QUARTER', CURRENT_DATE()), DATE_TRUNC('QUARTER', MIN(achieved_paid_date))) / 92) AS max_quarter_number,
        CAST(MONTHS_BETWEEN(DATE_TRUNC('MONTH', CURRENT_DATE()), DATE_TRUNC('MONTH', MIN(achieved_paid_date))) AS INT) AS max_month_number
    FROM base
    GROUP BY 1, 2
),

deals AS (
    SELECT
        achieved_paid_date,
        deal_id,
        user_id,
        COALESCE(final_gmv, 0) AS final_gmv,
        COALESCE(gmv_initial, 0) AS gmv_initial
    FROM base

),

base_s AS (
    SELECT
        *,
        CAST(MONTHS_BETWEEN(DATE_TRUNC('MONTH', achieved_paid_date), cohort_month) AS INT) AS month_number,
        FLOOR(DATEDIFF(DATE_TRUNC('QUARTER', achieved_paid_date), cohort_quarter) / 92) AS quarter_number,
        FLOOR(DATEDIFF(DATE_TRUNC('WEEK', achieved_paid_date), cohort_week) / 7) AS week_number
    FROM cohort
    INNER JOIN deals USING (user_id)
    WHERE achieved_paid_date >= cohort_date
),

agg_week AS (
    SELECT
        user_id,
        week_number,
        MAX(1) AS users_with_deal,
        COUNT(deal_id) AS deals,
        SUM(gmv_initial) AS gmv,
        SUM(final_gmv) AS final_gmv
    FROM base_s
    WHERE week_number IS NOT null
    GROUP BY user_id, week_number
),

agg_month AS (
    SELECT
        user_id,
        month_number,
        MAX(1) AS users_with_deal,
        COUNT(deal_id) AS deals,
        SUM(gmv_initial) AS gmv,
        SUM(final_gmv) AS final_gmv
    FROM base_s
    WHERE month_number IS NOT null
    GROUP BY user_id, month_number
),

agg_quarter AS (
    SELECT
        user_id,
        quarter_number,
        MAX(1) AS users_with_deal,
        COUNT(deal_id) AS deals,
        SUM(gmv_initial) AS gmv,
        SUM(final_gmv) AS final_gmv
    FROM base_s
    WHERE quarter_number IS NOT null
    GROUP BY user_id, quarter_number
),

counter AS (
    SELECT POSEXPLODE(SEQUENCE(0, 500)) AS (x, dummy)  -- sequence от 0 до 500
),

week_retention AS (
    SELECT
        'week' AS retention_detalization,
        cohort.*,
        x AS period_number,
        COALESCE(users_with_deal, 0) AS users_with_deal,
        COALESCE(deals, 0) AS deals,
        COALESCE(gmv, 0) AS gmv
    FROM cohort
    INNER JOIN counter ON x <= max_week_number
    LEFT JOIN agg_week ON cohort.user_id = agg_week.user_id AND agg_week.week_number = x
),

quarter_retention AS (
    SELECT
        'quarter' AS retention_detalization,
        cohort.*,
        x AS period_number,
        COALESCE(users_with_deal, 0) AS users_with_deal,
        COALESCE(deals, 0) AS deals,
        COALESCE(gmv, 0) AS gmv
    FROM cohort
    INNER JOIN counter ON x <= max_quarter_number
    LEFT JOIN agg_quarter ON cohort.user_id = agg_quarter.user_id AND agg_quarter.quarter_number = x
),

month_retention AS (
    SELECT
        'month' AS retention_detalization,
        cohort.*,
        x AS period_number,
        COALESCE(users_with_deal, 0) AS users_with_deal,
        COALESCE(deals, 0) AS deals,
        COALESCE(gmv, 0) AS gmv
    FROM cohort
    INNER JOIN counter ON x <= max_month_number
    LEFT JOIN agg_month ON cohort.user_id = agg_month.user_id AND agg_month.month_number = x

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
    questionnaire_grade,
    Marketing_Lead_Type,
    COALESCE(ss_user, 0) AS ss_user
FROM data_
LEFT JOIN ss_users USING (user_id)
