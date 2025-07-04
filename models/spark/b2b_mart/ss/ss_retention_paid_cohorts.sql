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

with ss_users AS (
  SELECT 
    user_id,
    questionnaire_grade,
    Marketing_Lead_Type,
    1 AS ss_user
  FROM  {{ ref('ss_users_table') }}
), 
statuses AS (
    SELECT
        deal_id,
        MAX(1) AS achieved_paid,
        min(cast(event_ts_msk as date)) as achieved_paid_date
    FROM {{ ref('fact_deals_status_history') }}
    WHERE status_name_small_deal LIKE '%ProcurementConfirmation' OR status_name LIKE '%PaymentToMerchant'
    GROUP BY 1
),
base as (
select *
from {{ ref('fact_deals_with_requests') }}
join statuses using(deal_id) 
where 
achieved_paid_date is not null 
and deal_type 
in ('Small Deal', 'Big Deal','VIP' ) and country in ('BR','MX') and  deal_status NOT IN ('Test')
), 
cohort as (

select 
user_id,
country, 
MIN(achieved_paid_date) AS cohort_date,
DATE_TRUNC('WEEK', MIN(achieved_paid_date)) AS cohort_week, 
DATE_TRUNC('MONTH', MIN(achieved_paid_date)) AS cohort_month,
FLOOR(DATEDIFF(DATE_TRUNC('WEEK', current_date()), DATE_TRUNC('WEEK', MIN(achieved_paid_date))) / 7) AS max_week_number, 
CAST(MONTHS_BETWEEN(DATE_TRUNC('MONTH', current_date()), DATE_TRUNC('MONTH', MIN(achieved_paid_date))) AS INT) AS max_month_number
from base
group by 1,2
),
deals AS (
  SELECT 
    achieved_paid_date,
    deal_id, 
    user_id,
    COALESCE(final_gmv, 0) AS final_gmv,
    COALESCE(gmv_initial, 0) as gmv_initial
  FROM base
  
),

base_s AS (
  SELECT *, 
    CAST(MONTHS_BETWEEN(DATE_TRUNC('MONTH', achieved_paid_date), cohort_month) AS INT) AS month_number, 
    FLOOR(DATEDIFF(DATE_TRUNC('WEEK', achieved_paid_date), cohort_week) / 7) AS week_number
  FROM cohort
  JOIN deals USING(user_id)
  WHERE achieved_paid_date >= cohort_date
),

agg_week AS (
  SELECT 
    user_id,
    week_number, 
    MAX(1) as users_with_deal,
    COUNT(deal_id) AS deals, 
    SUM(gmv_initial) AS gmv,
    SUM(final_gmv) as final_gmv
  FROM base_s 
  WHERE week_number IS NOT NULL  
  GROUP BY user_id, week_number
),

agg_month AS (
  SELECT 
    user_id,
    month_number, 
    MAX(1) as users_with_deal,
    COUNT(deal_id) AS deals, 
    SUM(gmv_initial) AS gmv,
    SUM(final_gmv) as final_gmv
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
    COALESCE(users_with_deal, 0) AS users_with_deal,
    COALESCE(deals, 0) AS deals,
    COALESCE(gmv, 0) AS gmv
  FROM cohort 
  JOIN counter ON x <= max_week_number
  LEFT JOIN agg_week ON agg_week.user_id = cohort.user_id AND agg_week.week_number = x 
   
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
  JOIN counter ON x <= max_month_number
  LEFT JOIN agg_month ON agg_month.user_id = cohort.user_id AND agg_month.month_number = x 
  
),

data_ AS (
  SELECT * FROM month_retention 
  UNION ALL
  SELECT * FROM week_retention
)

SELECT 
  data_.*,
  questionnaire_grade,
  Marketing_Lead_Type, 
  COALESCE(ss_user, 0) AS ss_user
FROM data_
LEFT JOIN ss_users USING(user_id)
