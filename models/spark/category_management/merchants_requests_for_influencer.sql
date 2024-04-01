{{
  config(
    materialized='table',
    alias='merchants_requests_for_influencer',
    file_format='parquet',
    schema='category_management',
    meta = {
        'model_owner' : '@ekutynina',
        'bigquery_load': 'true'
    }
  )
}}

WITH products AS (
    SELECT product_id
    FROM {{ source('mart', 'published_products_current') }}
    WHERE ARRAY_CONTAINS(labels.key, "influencer-exchange-suggested") = TRUE
),

requests AS (
    SELECT
        c._id AS request_id,
        DATE(MILLIS_TO_TS(c.c)) AS request_created_date_utc,
        DATE(MILLIS_TO_TS(c.u)) AS request_updated_date_utc,
        c.mid AS merchant_id,
        c.pid AS product_id,
        CASE
            WHEN c.s = 0 THEN "pending"
            WHEN c.s = 1 THEN "selected"
            WHEN c.s = 2 THEN "paid"
            WHEN c.s = 3 THEN "approved"
            WHEN c.s = 4 THEN "shipped"
            WHEN c.s = 5 THEN "delivered"
            WHEN c.s = 10 THEN "completed"
            WHEN c.s = 11 THEN "discarded"
            WHEN c.s = 12 THEN "canceled"
            WHEN c.s = 13 THEN "expired"
            WHEN c.s = 14 THEN "refunded"
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
        DATE(r.next_effective_ts) >= "9999-12-31" AND r.is_deleted = FALSE AS is_review_active,
        r.num_of_images AS review_images_cnt,
        LENGTH(r.text) AS text_length
    FROM {{ source('mongo', 'core_paid_post_tenders_daily_snapshot') }} AS c
    LEFT JOIN products AS p ON c.pid = p.product_id
    LEFT JOIN {{ source('mart', 'dim_product_review') }} AS r ON c.rid = r.review_id
),

bids AS (
    SELECT
        s._id AS bid_id,
        s.pid AS product_id,
        s.suid AS social_user_id,
        s.s AS bid_status,
        s.oid AS order_id,
        m.countrycode AS influencer_country_code
    FROM {{ source('mongo', 'user_paid_post_bids_daily_snapshot') }} AS s
    INNER JOIN {{ source('mongo', 'social_social_users_daily_snapshot') }} AS m ON m._id = s.suid
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
    b.social_user_id,
    b.bid_status,
    b.order_id,
    b.influencer_country_code
FROM requests AS r
LEFT JOIN bids AS b ON r.bid_id = b.bid_id


