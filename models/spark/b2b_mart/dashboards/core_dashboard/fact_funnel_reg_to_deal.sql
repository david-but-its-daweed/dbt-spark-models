{{ config(
    schema='b2b_mart',
    file_format='delta',
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    meta = {
      'model_owner' : '@tigran',
      'bigquery_load': 'true'
    }
) }}

WITH base AS (
    SELECT
        user_id,
        MIN(event_msk_date) AS cohort_date,
        MIN(event_ts_msk) AS first_visit
    FROM {{ ref('ss_events_startsession') }}
    GROUP BY 1
),

aut_data AS (
    SELECT
        user_id,
        MIN(CASE WHEN type = 'signIn' THEN event_msk_date END) AS sign_in,
        MIN(CASE WHEN type = 'selfServiceRegistrationFinished' THEN event_msk_date END) AS registration,
        MIN(CASE WHEN type = 'signIn' THEN event_ts_msk END) AS sign_in_ts,
        MIN(CASE WHEN type = 'selfServiceRegistrationFinished' THEN event_ts_msk END) AS registration_ts
    FROM {{ ref('ss_events_authentication') }}
    GROUP BY user_id
),

cart_and_deal AS (
    SELECT
        user_id,
        MAX(CASE WHEN actionType = 'add_to_cart' THEN 1 ELSE 0 END) AS once_add_to_cart,
        MAX(CASE WHEN actionType = 'move_to_deal' THEN 1 ELSE 0 END) AS once_made_deal
    FROM {{ ref('ss_events_cart') }}
    GROUP BY 1
),

paid_deals AS (
    SELECT DISTINCT
        user_id,
        1 AS once_made_paid_deal
    FROM {{ ref('gmv_by_sources') }}
),

search AS (
    SELECT
        user_id,
        MAX(CASE WHEN user_id IS NOT null THEN 1 ELSE 0 END) AS once_had_search,
        MAX(CASE WHEN is_search_by_image THEN 1 ELSE 0 END) AS once_had_search_by_image
    FROM {{ ref('ss_events_search') }}
    WHERE query <> ''
    GROUP BY 1
),

products AS (
    SELECT
        user_id,
        MAX(CASE WHEN event_type = 'productClick' THEN 1 ELSE 0 END) AS once_had_product_click,
        MAX(CASE WHEN event_type = 'productClick' AND search_id IS NOT null THEN 1 ELSE 0 END) AS once_had_product_click_after_search
    FROM {{ ref('ss_events_funnel') }}
    GROUP BY 1
),

product_views AS (
    SELECT DISTINCT
        user_id,
        1 AS once_had_product_view
    FROM {{ ref('ss_product_preview_next_steps_allocaton') }}
    WHERE next_type = 'pageView' AND next_pageName = 'product' AND type = 'productPreview'
)


SELECT
    base.user_id,
    base.cohort_date,
    base.first_visit,
    a.sign_in,
    a.registration,
    a.sign_in_ts,
    a.registration_ts,
    c.once_add_to_cart,
    c.once_made_deal,
    p.once_made_paid_deal,
    s.once_had_search,
    s.once_had_search_by_image,
    pr.once_had_product_click,
    pr.once_had_product_click_after_search,
    prv.once_had_product_view,
    ABS(UNIX_TIMESTAMP(a.sign_in_ts) - UNIX_TIMESTAMP(base.first_visit)) / 60 AS minutes_to_sign_in,
    ABS(UNIX_TIMESTAMP(a.registration_ts) - UNIX_TIMESTAMP(base.first_visit)) / 60 AS minutes_to_registration,
    ABS(UNIX_TIMESTAMP(a.registration_ts) - UNIX_TIMESTAMP(a.sign_in_ts)) / 60 AS minutes_to_registration_from_sign
FROM base
LEFT JOIN aut_data AS a USING (user_id)
LEFT JOIN cart_and_deal AS c USING (user_id)
LEFT JOIN paid_deals AS p USING (user_id)
LEFT JOIN search AS s USING (user_id)
LEFT JOIN products AS pr USING (user_id)
LEFT JOIN product_views AS prv USING (user_id)
