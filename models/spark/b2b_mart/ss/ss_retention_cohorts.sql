{{ config(
    schema='b2b_mart',
    materialized='view',
    partition_by={
         "field": "cohort_date"
    },
    meta = {
      'model_owner' : '@kirill_melnikov',
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}

WITH 
interactions AS (
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
    DATE_TRUNC('MONTH', MIN(event_msk_date)) AS cohort_month,
    FLOOR(DATEDIFF(DATE_TRUNC('WEEK', current_date()), DATE_TRUNC('WEEK', MIN(event_msk_date))) / 7) AS max_week_number, 
    CAST(MONTHS_BETWEEN(DATE_TRUNC('MONTH', current_date()), DATE_TRUNC('MONTH', MIN(event_msk_date))) AS INT) AS max_month_number
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
  JOIN activity USING(user_id)
  GROUP BY cohort.user_id, cohort_week, event_msk_date
),

activity_month AS (
  SELECT 
    cohort.user_id, 
    CAST(MONTHS_BETWEEN(event_msk_date, cohort_month) AS INT) AS month_number,
    MAX(1) AS is_active 
  FROM cohort 
  JOIN activity USING(user_id)
  GROUP BY cohort.user_id, cohort_month, event_msk_date
),

deals AS (
  SELECT 
    deal_created_date,
    deal_id, 
    user_id,
    COALESCE(final_gmv, 0) AS final_gmv
  FROM {{ ref('fact_deals_with_requests') }}
  WHERE deal_type != 'Sample'
),

base_s AS (
  SELECT *, 
    CAST(MONTHS_BETWEEN(DATE_TRUNC('MONTH', deal_created_date), cohort_month) AS INT) AS month_number, 
    FLOOR(DATEDIFF(DATE_TRUNC('WEEK', deal_created_date), cohort_week) / 7) AS week_number
  FROM cohort
  JOIN deals USING(user_id)
  WHERE deal_created_date >= cohort_date
),

agg_week AS (
  SELECT 
    user_id,
    week_number, 
    COUNT(deal_id) AS deals, 
    SUM(final_gmv) AS gmv
  FROM base_s 
  WHERE week_number IS NOT NULL  
  GROUP BY user_id, week_number
),

agg_month AS (
  SELECT 
    user_id,
    month_number, 
    COUNT(deal_id) AS deals, 
    SUM(final_gmv) AS gmv
  FROM base_s 
  WHERE month_number IS NOT NULL  
  GROUP BY user_id, month_number
),

counter AS (
  SELECT posexplode(sequence(0, 500)) AS (x, dummy)  -- sequence от 0 до 500
),

week_retention AS (
  SELECT 
    'week' AS retention_detalization,
    cohort.*,
    x AS period_number, 
    COALESCE(is_active, 0) AS is_active, 
    COALESCE(deals, 0) AS deals,
    COALESCE(gmv, 0) AS gmv
  FROM cohort 
  JOIN counter ON x <= max_week_number
  LEFT JOIN agg_week ON agg_week.user_id = cohort.user_id AND agg_week.week_number = x 
  LEFT JOIN activity_week ON activity_week.user_id = cohort.user_id AND activity_week.week_number = x 
),

month_retention AS (
  SELECT 
    'month' AS retention_detalization,
    cohort.*,
    x AS period_number, 
    COALESCE(is_active, 0) AS is_active, 
    COALESCE(deals, 0) AS deals,
    COALESCE(gmv, 0) AS gmv
  FROM cohort 
  JOIN counter ON x <= max_month_number
  LEFT JOIN agg_month ON agg_month.user_id = cohort.user_id AND agg_month.month_number = x 
  LEFT JOIN activity_month ON activity_month.user_id = cohort.user_id AND activity_month.month_number = x
),

data_ AS (
  SELECT * FROM month_retention 
  UNION ALL
  SELECT * FROM week_retention
)

SELECT 
  data_.*, 
  friendly_source, 
  utm_campaign, 
  questionnaire_grade,
  Marketing_Lead_Type, 
  COALESCE(ss_user, 0) AS ss_user
FROM data_
LEFT JOIN interactions USING(user_id)
LEFT JOIN ss_users USING(user_id)



