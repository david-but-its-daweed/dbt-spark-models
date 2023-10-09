{{
  config(
    materialized='table',
    file_format='delta',
    meta = {
        'model_owner' : '@espander',
        'bigquery_load': 'true',
        'priority_weight': '1000',
    }
  )
}}

SELECT
    user_id,
    IF(referral_count > 0, 'referral', 'brand') AS blogger_type
FROM (
    SELECT
        user_id,
        SUM(IF(blogger_type = 'referral', 1, 0)) AS referral_count,
        SUM(IF(blogger_type = 'brand', 1, 0)) AS brand_count
    FROM
        (
            SELECT DISTINCT
                blogger_id AS user_id,
                'referral' AS blogger_type
            FROM {{ source('ads','referral_payout_stats') }}
            UNION ALL
            SELECT DISTINCT
                user_id,
                'brand' AS blogger_type
            FROM {{ ref('user_points_transactions') }} WHERE point_transaction_type = 'admin'
        )
    GROUP BY 1
)