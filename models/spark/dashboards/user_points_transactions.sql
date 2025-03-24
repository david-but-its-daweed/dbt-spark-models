{{
  config(
    meta = {
      'model_owner' : '@general_analytics',
      'bigquery_load': 'true',
      'bigquery_partitioning_date_column': 'day',
      'priority_weight': '1000',
      'bigquery_upload_horizon_days': '230',
      'bigquery_check_counts': 'false'
    },
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

finalized AS (
    SELECT
        refid,
        effective_usd,
        amount
    FROM
        {{ ref('fact_user_points_transactions') }}
    WHERE
        type IN ("finalize")
),

types_for_finalize AS (
    SELECT INLINE(ARRAY(
        STRUCT("referral" AS transaction_type),
        STRUCT("cashback" AS transaction_type)  -- this list will be expanded in future
    ))
),

points AS (
    SELECT
        t1.user_id,
        t1.type AS point_transaction_type,
        t1.date_msk,
        COUNT(*) AS num_transactions,
        SUM(IF(t1.type IN (SELECT f.transaction_type FROM types_for_finalize AS f), t2.effective_usd, t1.effective_usd)) AS effective_usd,
        SUM(IF(t1.type IN (SELECT f.transaction_type FROM types_for_finalize AS f), t2.amount.value, t1.amount.value)) AS value,
        FIRST(IF(t1.type IN (SELECT f.transaction_type FROM types_for_finalize AS f), t2.amount.ccy, t1.amount.ccy)) AS ccy,
        FIRST(IF(t1.type IN (SELECT f.transaction_type FROM types_for_finalize AS f), t2.amount.mult, t1.amount.mult)) AS mult
    FROM {{ ref('fact_user_points_transactions') }} AS t1
    LEFT JOIN finalized AS t2
        ON t1.id = t2.refid
    WHERE
        t1.type != "finalize"
        AND NOT (t1.type IN (SELECT f.transaction_type FROM types_for_finalize AS f) AND t2.refid IS NULL) -- filter transactions which must have finalize status but don't have it after join by refid
    GROUP BY 1, 2, 3
),

base AS (
    SELECT
        points.date_msk AS day,
        active_users.country,
        active_users.platform,
        points.user_id,
        points.point_transaction_type,
        points.effective_usd AS point_usd,
        points.num_transactions,
        points.ccy,
        points.mult,
        points.value
    FROM
        points
    LEFT JOIN active_users ON
        points.user_id = active_users.user_id
        AND points.date_msk = active_users.day
),

points_type_to_group_mapping AS (
    SELECT *
    FROM INLINE(ARRAY(
        STRUCT("cashback" AS point_transaction_type, "Marketing" AS point_transaction_group),
        STRUCT("refund" AS point_transaction_type, "Refund" AS point_transaction_group),
        STRUCT("customUserRefund" AS point_transaction_type, "Refund" AS point_transaction_group),
        STRUCT("admin_bloggers" AS point_transaction_type, "Marketing" AS point_transaction_group),
        STRUCT("admin_refunds" AS point_transaction_type, "Refund" AS point_transaction_group),
        STRUCT("migration" AS point_transaction_type, "Technical" AS point_transaction_group),
        STRUCT("migrationGift" AS point_transaction_type, "Technical" AS point_transaction_group),
        STRUCT("referralUserInvited" AS point_transaction_type, "Marketing" AS point_transaction_group),
        STRUCT("referralUserReferrer" AS point_transaction_type, "Marketing" AS point_transaction_group),
        STRUCT("revenueShareExternalLink" AS point_transaction_type, "Marketing" AS point_transaction_group),
        STRUCT("revenueShareSocial" AS point_transaction_type, "Marketing" AS point_transaction_group),
        STRUCT("staffEnrollment" AS point_transaction_type, "Compensation" AS point_transaction_group),
        STRUCT("finalize" AS point_transaction_type, "Technical" AS point_transaction_group),
        STRUCT("testing" AS point_transaction_type, "Technical" AS point_transaction_group),
        STRUCT("revenueShareProductCollection" AS point_transaction_type, "Marketing" AS point_transaction_group),
        STRUCT("refundNotDelivered" AS point_transaction_type, "Refund" AS point_transaction_group),
        STRUCT("reward" AS point_transaction_type, "Marketing" AS point_transaction_group),
        STRUCT("referral" AS point_transaction_type, "Marketing" AS point_transaction_group),
        STRUCT("newUser" AS point_transaction_type, "Marketing" AS point_transaction_group),
        STRUCT("socialSignIn" AS point_transaction_type, "Marketing" AS point_transaction_group),
        STRUCT("dailyLogin" AS point_transaction_type, "Marketing" AS point_transaction_group),
        STRUCT("initial" AS point_transaction_type, "Marketing" AS point_transaction_group),
        STRUCT("crowdSource" AS point_transaction_type, "Marketing" AS point_transaction_group),
        STRUCT("deliveryMethodChanged" AS point_transaction_type, "Refund" AS point_transaction_group)
    ))
),

points_type_rename_mapping AS (
    SELECT *
    FROM INLINE(ARRAY(
        STRUCT("customUserRefund" AS point_transaction_type, "top-up refunds" AS business_point_transaction_type),
        STRUCT("refund" AS point_transaction_type, "refundrefund after using points" AS business_point_transaction_type),
        STRUCT("revenueShareExternalLink" AS point_transaction_type, "bloggers referral" AS business_point_transaction_type)
    ))
),

admin_points AS (
    SELECT
        b.*,
        CASE
            WHEN b.point_transaction_type != "admin" THEN b.point_transaction_type
            WHEN rb.user_id IS NOT NULL THEN "admin_bloggers"
            ELSE "admin_refunds"
        END AS technical_point_transaction_type
    FROM base AS b
    LEFT JOIN ads.referral_bloggers AS rb
        ON (b.user_id = rb.user_id)
)

SELECT
    t.*,
    COALESCE(m.point_transaction_group, "other_group") AS point_transaction_group,
    COALESCE(rn.business_point_transaction_type, t.point_transaction_type) AS business_point_transaction_type
FROM admin_points AS t
LEFT JOIN points_type_to_group_mapping AS m
    ON t.technical_point_transaction_type = m.point_transaction_type
LEFT JOIN points_type_rename_mapping AS rn
    ON t.point_transaction_type = rn.point_transaction_type
