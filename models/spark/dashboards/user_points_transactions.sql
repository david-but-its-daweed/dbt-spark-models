{{
  config(
    materialized='table',
    file_format='delta',
  )
}}


WITH active_users AS (
    SELECT *
    FROM
        {{ ref('active_users') }}
    WHERE
        is_ephemeral = FALSE
),

points_transactions AS (
    SELECT
        id,
        user_id,
        amount.value AS points_ccy,
        amount.ccy AS ccy,
        type AS point_transaction_type,
        date_msk
    FROM
        {{ ref('fact_user_points_transactions') }}
    WHERE
        type IN ("referral", "cashback")
),

user_point_transactions AS (
    SELECT
        refid,
        effective_usd
    FROM
        {{ ref('fact_user_points_transactions') }}
    WHERE
        type IN ("finalize")
),

points_transactions_with_finalize AS (
    SELECT
        t1.user_id AS user_id,
        t1.point_transaction_type,
        t1.date_msk,
        COUNT(*) AS num_transactions,
        SUM(t2.effective_usd) AS effective_usd
    FROM points_transactions AS t1
    INNER JOIN user_point_transactions AS t2
        ON
            t1.id = t2.refid
    GROUP BY 1, 2, 3
),

points_transactions_wo_finalize AS (
    SELECT
        user_id,
        type AS point_transaction_type,
        date_msk,
        COUNT(*) AS num_transactions,
        SUM(effective_usd) AS effective_usd
    FROM
        {{ ref('fact_user_points_transactions') }}
    WHERE
        type NOT IN ("referral", "cashback", "finalize")
    GROUP BY 1, 2, 3
),

points AS (
    SELECT
        user_id,
        point_transaction_type,
        date_msk,
        num_transactions,
        effective_usd
    FROM
        points_transactions_with_finalize
    UNION ALL
    SELECT
        user_id,
        point_transaction_type,
        date_msk,
        num_transactions,
        effective_usd
    FROM
        points_transactions_wo_finalize
),

base AS (
    SELECT
        points.date_msk AS day,
        active_users.country,
        active_users.platform,
        points.user_id AS user_id,
        points.point_transaction_type,
        points.effective_usd AS point_usd,
        points.num_transactions
    FROM
        points
    LEFT JOIN
        active_users ON
        points.user_id = active_users.user_id
        AND points.date_msk = active_users.day
)

SELECT *
FROM
    base
