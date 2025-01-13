{{
  config(
    materialized='table',
    alias='merchants_requests_for_influencer',
    file_format='parquet',
    schema='category_management',
    meta = {
        'model_owner' : '@catman-analytics.duty',
        'bigquery_load': 'true'
    }
  )
}}

WITH products AS (
    SELECT product_id
    FROM {{ source('mart', 'published_products_current') }}
    WHERE ARRAY_CONTAINS(labels.key, "influencer-exchange-suggested") = TRUE
),

reviews AS (
    SELECT
        review_id,
        DATE(next_effective_ts) >= "9999-12-31" AND is_deleted = FALSE AS is_review_active,
        num_of_images AS review_images_cnt,
        LENGTH(text) AS text_length
    FROM {{ source('mart', 'dim_product_review') }}
    WHERE DATE(next_effective_ts) >= "9999-12-31"
),

requests AS (
    SELECT
        c._id AS request_id,
        DATE(MILLIS_TO_TS(c.c)) AS request_created_date_utc,
        DATE(MILLIS_TO_TS(c.u)) AS request_updated_date_utc,
        c.mid AS merchant_id,
        c.pid AS product_id,
        CASE
            WHEN c.s = 1 THEN "published"
            WHEN c.s = 2 THEN "bidSelected"
            WHEN c.s = 3 THEN "paid"
            WHEN c.s = 4 THEN "approved"
            WHEN c.s = 5 THEN "shipped"
            WHEN c.s = 6 THEN "delivered"
            WHEN c.s = 10 THEN "completed"
            WHEN c.s = 11 THEN "canceled"
        END AS request_status,
        c.bidid AS bid_id,
        c.postid AS post_id,
        c.rid AS review_id,
        c.selectionmode AS selection_mode,
        CASE
            WHEN c.im = 1 THEN "notChina"
            WHEN c.im = 2 THEN "china"
            WHEN c.im = 3 THEN "russia"
            WHEN c.im = 4 THEN "europe"
        END AS market,
        p.product_id IS NOT NULL AS is_recomnded_product,
        r.is_review_active,
        r.review_images_cnt,
        r.text_length
    FROM {{ source('mongo', 'core_paid_post_tenders_daily_snapshot') }} AS c
    LEFT JOIN products AS p ON c.pid = p.product_id
    LEFT JOIN reviews AS r ON c.rid = r.review_id
),

bids AS (
    SELECT
        s._id AS bid_id,
        s.pid AS product_id,
        s.suid AS social_user_id,
        CASE
            WHEN s.s = 0 THEN "pending"
            WHEN s.s = 1 THEN "selected"
            WHEN s.s = 2 THEN "paid"
            WHEN s.s = 3 THEN "approved"
            WHEN s.s = 4 THEN "shipped"
            WHEN s.s = 5 THEN "delivered"
            WHEN s.s = 10 THEN "completed"
            WHEN s.s = 11 THEN "discarded"
            WHEN s.s = 12 THEN "canceled"
            WHEN s.s = 13 THEN "expired"
            WHEN s.s = 14 THEN "refunded"
        END AS bid_status,
        s.oid AS order_id,
        m.countrycode AS influencer_country_code
    FROM {{ source('mongo', 'user_paid_post_bids_daily_snapshot') }} AS s
    INNER JOIN {{ source('mongo', 'social_social_users_daily_snapshot') }} AS m ON s.suid = m._id
),

events AS (
    SELECT
        payload.tenderid AS tender_id,
        MAX(IF(payload.state = "completed", FROM_UNIXTIME(server_ts_utc / 1000), NULL)) AS completed_time_utc,
        MAX(IF(payload.state = "delivered", FROM_UNIXTIME(server_ts_utc / 1000), NULL)) AS delivered_time_utc
    FROM {{ source('mart', 'unclassified_events') }}
    WHERE
        type = "paidPostTenderStatusChanged"
        AND payload.state IN ("completed", "delivered")
        AND partition_date >= "2023-12-01"
    GROUP BY 1
),

orders AS (
    SELECT
        order_id,
        order_datetime_utc
    FROM {{ ref('gold_orders') }}
)

SELECT
    r.request_id,
    r.request_created_date_utc,
    r.request_updated_date_utc,
    r.merchant_id,
    r.product_id,
    r.request_status,
    r.bid_id,
    r.post_id,
    r.review_id,
    r.selection_mode,
    r.market,
    r.is_recomnded_product,
    r.review_images_cnt,
    r.text_length,
    r.is_review_active,
    b.social_user_id,
    b.bid_status,
    b.order_id,
    b.influencer_country_code,
    o.order_datetime_utc,
    e.completed_time_utc,
    e.delivered_time_utc
FROM requests AS r
LEFT JOIN bids AS b ON r.bid_id = b.bid_id
LEFT JOIN orders AS o ON b.order_id = o.order_id
LEFT JOIN events AS e ON r.request_id = e.tender_id


