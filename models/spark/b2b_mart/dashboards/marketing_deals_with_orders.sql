{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='delta',
    meta = {
      'model_owner' : '@tigran',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}

WITH first_deals AS (
    SELECT
        deal_id AS first_deal_id,
        user_id,
        deal_created_date AS first_deal_created_date
    FROM {{ ref('fact_deals_with_requests') }}
    WHERE
        number_user_deal = 1
        AND deal_status NOT IN ('Test')
),

previous_deals AS (
    SELECT
        deal_id,
        LAG(deal_id) OVER (PARTITION BY user_id ORDER BY deal_created_ts) AS previous_deal,
        DATEDIFF(deal_created_date, LAG(deal_created_date) OVER (PARTITION BY user_id ORDER BY deal_created_ts)) AS day_after_previous_deal
    FROM {{ ref('fact_deals_with_requests') }}
    WHERE
        deal_status NOT IN ('Test')
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

users AS (
    SELECT
        user_id,
        questionnaire_grade
    FROM {{ ref('ss_users_table') }}
),

device_types_ AS (
    SELECT
        user_id,
        event_msk_date,
        CASE
            WHEN osType IN ('android', 'ios', 'tizen', 'harmonyos') THEN 'mobile'
            WHEN osType IN ('ubuntu', 'linux', 'mac os', 'windows', 'chromium os') THEN 'desktop'
            ELSE 'other'
        END AS device_type,
        event_ts_msk,
        ROW_NUMBER() OVER (PARTITION BY user_id, event_msk_date ORDER BY event_ts_msk DESC) AS rn
    FROM {{ ref('ss_events_startsession') }}
    WHERE
        event_msk_date >= '2024-04-06'
        AND osType IS NOT NULL
),

-- device_types AS (
--     SELECT
--         user_id,
--         event_msk_date,
--         event_ts_msk,
--         device_type
--     FROM device_types_
--     WHERE rn = 1
-- ),

candidates AS (
    SELECT
        d.deal_id,
        d.user_id,
        d.deal_created_ts,
        d.deal_created_date,
        t.event_ts_msk,
        t.event_msk_date,
        t.device_type,
        (UNIX_TIMESTAMP(d.deal_created_ts) - UNIX_TIMESTAMP(t.event_ts_msk)) AS diff_sec
    FROM {{ ref("fact_deals_with_requests") }} AS d
    LEFT JOIN device_types_ AS t
        ON
            d.user_id = t.user_id
            AND t.event_ts_msk <= d.deal_created_ts
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY deal_id, user_id
            ORDER BY
                CASE WHEN event_ts_msk IS NULL THEN 1 ELSE 0 END ASC,
                diff_sec ASC,
                event_ts_msk DESC
        ) AS rn
    FROM candidates
),

deals_with_types AS (
    SELECT
        deal_id,
        user_id,
        deal_created_ts,
        event_ts_msk,
        device_type,
        diff_sec
    FROM ranked
    WHERE rn = 1
)

SELECT
    t1.deal_id,
    t1.deal_friendly_id,
    t1.user_id,
    us.questionnaire_grade,
    dt.device_type,
    t1.country,
    t1.deal_name,
    t1.payment_method,
    t1.estimated_gmv,
    t1.deal_type,
    t1.self_service,
    t1.ss_customer,
    t1.deal_status_group,
    t1.deal_status,
    t1.deal_reject_reason,
    CASE
        WHEN t1.deal_reject_reason IS NULL OR t1.deal_reject_reason = t1.deal_status THEN t1.deal_status
        ELSE CONCAT(t1.deal_status, ':', t1.deal_reject_reason)
    END AS full_deal_status,
    t1.deal_created_ts,
    t1.deal_created_date,
    t1.order_id,
    t1.count_customer_requests AS customer_requests,
    t1.ddp,
    t1.ddp_usd,
    t1.order_created_date,
    t1.order_friendly_id,
    t1.order_current_status,
    t1.total_confirmed_price,
    t1.final_gross_profit,
    t1.initial_gross_profit,
    t1.final_gmv,
    t1.gmv_initial,
    t1.utm_campaign,
    t1.utm_source,
    t1.utm_medium,
    t1.source,
    t1.type,
    CASE
        WHEN DATEDIFF(t1.deal_created_date, t1.first_visit_date) = 0 THEN CONCAT(REPEAT('\u200a', 1), 'first day')
        WHEN
            DATEDIFF(t1.deal_created_date, t1.first_visit_date) > 0
            AND DATEDIFF(t1.deal_created_date, t1.first_visit_date) < 8 THEN CONCAT(REPEAT('\u200a', 2), 'first week')
        WHEN
            DATEDIFF(t1.deal_created_date, t1.first_visit_date) > 7
            AND DATEDIFF(t1.deal_created_date, t1.first_visit_date) < 31 THEN CONCAT(REPEAT('\u200a', 3), 'first month')
        WHEN
            DATEDIFF(t1.deal_created_date, t1.first_visit_date) > 30
            AND DATEDIFF(t1.deal_created_date, t1.first_visit_date) < 93 THEN CONCAT(REPEAT('\u200a', 4), 'first quarter')
        WHEN
            DATEDIFF(t1.deal_created_date, t1.first_visit_date) > 92
            AND DATEDIFF(t1.deal_created_date, t1.first_visit_date) < 181 THEN CONCAT(REPEAT('\u200a', 5), 'first halfyear')
        WHEN
            DATEDIFF(t1.deal_created_date, t1.first_visit_date) > 180
            AND DATEDIFF(t1.deal_created_date, t1.first_visit_date) < 366 THEN CONCAT(REPEAT('\u200a', 6), 'first year')
        WHEN DATEDIFF(t1.deal_created_date, t1.first_visit_date) > 365 THEN CONCAT(REPEAT('\u200a', 7), 'more than a year')
    END AS first_visit_deal_flg,
    t1.first_visit_date,
    t1.first_utm_campaign,
    t1.first_utm_sourceas,
    t1.first_utm_medium,
    t1.first_source,
    t1.first_type,
    t1.count_visits,
    t1.number_user_deal,
    CASE WHEN t1.deal_created_date >= DATE_TRUNC('ISOWEEK', CURRENT_DATE) THEN 1 ELSE 0 END AS is_current_week,
    t1.count_customer_requests,
    t1.count_customer_requests_variants,
    fd.first_deal_created_date,
    DATEDIFF(t1.deal_created_date, fd.first_deal_created_date) AS deals_delta_day,
    fd.first_deal_id,
    pr.day_after_previous_deal,
    CASE
        WHEN pr.day_after_previous_deal IS NULL THEN 'a.First_Deal'
        WHEN pr.day_after_previous_deal = 0 THEN 'Same_Date'
        WHEN pr.day_after_previous_deal <= 7 THEN 'b.One_Week'
        WHEN pr.day_after_previous_deal <= 14 THEN 'c.Two_Week'
        WHEN pr.day_after_previous_deal <= 30 THEN 'd.One_Month'
        WHEN pr.day_after_previous_deal <= 60 THEN 'e.Two_Month'
        WHEN pr.day_after_previous_deal <= 90 THEN 'f.Three_Month'
        WHEN pr.day_after_previous_deal > 90 THEN 'g.Four_Month_and_more'
    END AS previous_deal_days_group,
    COALESCE(st.achieved_paid, 0) AS achived_payment,
    st.achieved_paid_date,
    t1.promo_code,
    t1.promo_code_discount,
    t1.promo_code_type
FROM {{ ref("fact_deals_with_requests") }} AS t1
LEFT JOIN first_deals AS fd USING (user_id)
LEFT JOIN statuses AS st USING (deal_id)
LEFT JOIN previous_deals AS pr USING (deal_id)
LEFT JOIN users AS us USING (user_id)
LEFT JOIN deals_with_types AS dt
    ON
        t1.user_id = dt.user_id AND t1.deal_id = dt.deal_id
        -- AND (t1.deal_created_date = dt.event_msk_date OR ABS(TO_UNIX_TIMESTAMP(dt.event_ts_msk) - TO_UNIX_TIMESTAMP(t1.deal_created_ts)) <= 72 * 3600)
WHERE t1.deal_status NOT IN ('Test')
