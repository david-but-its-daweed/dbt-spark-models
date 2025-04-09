{{
  config(
    meta = {
      'model_owner' : '@itangaev',
      'bigquery_load': 'true',
      'priority_weight': '1000',
      'bigquery_upload_horizon_days': '230',
      'bigquery_check_counts': 'false'
    },
    materialized='table',
    file_format='delta',
  )
}}


WITH rawPointBalance AS (
    SELECT
        payload.userId AS user_id,
        payload.pointsBalance.value AS value,
        payload.pointsBalance.ccy AS ccy,
        payload.pointsBalance.mult AS mult,
        DATE(FROM_UNIXTIME(payload.txTime / 1000 + 3 * 3600)) AS dt
    FROM {{ source('mart', 'unclassified_events') }}
    WHERE type = 'pointsBalanceSync'
        AND DATE(FROM_UNIXTIME(payload.txTime / 1000 + 3 * 3600)) >= ADD_MONTHS(CURRENT_DATE(), -24)
),

calendar AS (
    SELECT
        ADD_MONTHS(TRUNC(CURRENT_DATE(), 'MM'), -n) AS month_start
    FROM VALUES
        (0), (1), (2), (3), (4), (5), (6), (7), (8), (9),
        (10), (11), (12), (13), (14), (15), (16), (17)
    AS t(n)
),

user_months AS (
    SELECT DISTINCT r.user_id, c.month_start
    FROM rawPointBalance r
    CROSS JOIN calendar c
),

candidates AS (
    SELECT
        um.user_id,
        um.month_start,
        r.value,
        r.ccy,
        r.mult,
        r.dt,
        ROW_NUMBER() OVER (PARTITION BY um.user_id, um.month_start ORDER BY r.dt DESC) AS rn
    FROM user_months um
    LEFT JOIN rawPointBalance r
      ON um.user_id = r.user_id AND r.dt <= um.month_start
),

latest_balance_per_month AS (
    SELECT *
    FROM candidates
    WHERE rn = 1 AND dt IS NOT NULL
),

user_country AS (
    SELECT
        user_id,
        FIRST(top_country_code) as top_country_code
    FROM {{ ref('gold_active_users') }}
    WHERE date_msk >= ADD_MONTHS(CURRENT_DATE(), -24)
    GROUP BY 1
),

with_usd_and_country AS (
    SELECT
        l.month_start,
        l.user_id,
        l.value,
        l.mult,
        l.ccy,
        r.rate,
        l.value / l.mult * r.rate / 1000000.0 AS value_usd,
        coalesce(uc.top_country_code, "Other") AS top_country_code
    FROM latest_balance_per_month l
    JOIN mart.dim_currency_rate r
      ON l.ccy = r.currency_code AND l.dt = r.effective_date
    LEFT JOIN user_country uc
      ON l.user_id = uc.user_id
)

SELECT
    month_start,
    top_country_code,
    SUM(value_usd) AS total_balance_usd
FROM with_usd_and_country
GROUP BY month_start, top_country_code
ORDER BY month_start, top_country_code
