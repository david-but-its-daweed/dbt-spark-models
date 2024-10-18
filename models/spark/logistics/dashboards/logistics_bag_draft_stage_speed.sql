{{
  config(
    materialized='table',
    meta = {
      'model_owner' : '@general_analytics',
      'bigquery_load': 'true'
    }
  )
}}


WITH draft_time AS (
    SELECT
        parcel_id,
        MIN(FILTER(statusHistory, x -> x.status = 10)[0].createdTime) as bag_draft_time_utc
    FROM {{ source('mongo', 'logistics_bags_daily_snapshot') }}
    LATERAL VIEW EXPLODE(parcelIds) AS parcel_id
    GROUP BY 1
),

raw_data_channels AS (
    SELECT
        fo.order_created_date_utc,
        CASE
            WHEN COALESCE(ch.E2E, FALSE) THEN 'E2E'
            ELSE 'Crossborder'
        END AS channel,
        fo.check_in_time_utc,
        dt.bag_draft_time_utc,
        fo.outbound_time_utc
    FROM {{ source('logistics_mart', 'fact_order') }} fo
    LEFT JOIN draft_time dt ON fo.parcel_id = dt.parcel_id
    LEFT JOIN {{ source('mongo', 'logistics_channels_daily_snapshot') }} ch ON fo.channel_id = ch._id
    WHERE
        fo.consolidation_group_id IS NULL
        AND (fo.refund_type IS NULL OR fo.refund_type NOT IN ('cancelled_by_customer', 'cancelled_by_merchant'))
        AND fo.origin_country = 'CN' -- китайские заказы
        AND DATE(fo.check_in_time_utc) >= DATE '2024-02-05'
        AND DATE(fo.order_created_date_utc) >= DATE '2024-07-01'
),

raw_data_total AS (
    SELECT
        order_created_date_utc,
        'All' AS channel,
        check_in_time_utc,
        bag_draft_time_utc,
        outbound_time_utc
    FROM raw_data_channels
),

raw_data AS (
    SELECT 
        order_created_date_utc,
        channel,
        check_in_time_utc,
        bag_draft_time_utc,
        outbound_time_utc
    FROM raw_data_total

    UNION ALL

    SELECT
        order_created_date_utc,
        channel,
        check_in_time_utc,
        bag_draft_time_utc,
        outbound_time_utc
    FROM raw_data_channels
),

calculate_diff AS (
SELECT
    *,
    COALESCE((UNIX_TIMESTAMP(bag_draft_time_utc) - UNIX_TIMESTAMP(check_in_time_utc)) / (24.*60*60), 9999) AS check_in_draft_diff,
    COALESCE((UNIX_TIMESTAMP(outbound_time_utc)  - UNIX_TIMESTAMP(bag_draft_time_utc)) / (24.*60*60), 9999) AS draft_outbound_diff,
    COALESCE((UNIX_TIMESTAMP(outbound_time_utc)  - UNIX_TIMESTAMP(check_in_time_utc)) / (24.*60*60), 9999) AS check_in_outbound_diff
FROM raw_data
), 

expand_types AS (
    SELECT
        DATE(check_in_time_utc) AS stage_start,
        'Check-in - Draft' AS stage_type,
        channel,
        check_in_draft_diff AS stage_duration
    FROM calculate_diff
    WHERE check_in_time_utc IS NOT NULL

    UNION ALL

    SELECT
        DATE(bag_draft_time_utc) AS stage_start,
        'Draft - Outbound' AS stage_type,
        channel,
        draft_outbound_diff AS stage_duration
    FROM calculate_diff
    WHERE bag_draft_time_utc IS NOT NULL

    UNION ALL

    SELECT
        DATE(check_in_time_utc) AS stage_start,
        'Check-in - Outbound' AS stage_type,
        channel,
        check_in_outbound_diff AS stage_duration
    FROM calculate_diff
    WHERE check_in_time_utc IS NOT NULL
),

percentile AS (
    SELECT
        stage_start,
        stage_type,
        channel,
        PERCENTILE(stage_duration, 0.9) AS stage_duration
    FROM expand_types
    GROUP BY 1,2,3
)

SELECT
    stage_start,
    stage_type,
    channel,
    ROUND(IF(stage_duration > 100, NULL, stage_duration), 2) AS stage_duration
FROM percentile