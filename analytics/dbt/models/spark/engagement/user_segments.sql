{{ config(
    schema='engagement',
    materialized='table',
    file_format='delta',
    meta = {
      'team': 'clan',
      'bigquery_load': 'true'
    }
) }}

WITH purchases AS (
    SELECT DISTINCT
        real_user_id,
        partition_date,
        DATE(real_user_join_ts_msk) AS day_join_msk,
        COLLECT_SET(partition_date)
            OVER (PARTITION BY real_user_id)
        AS partition_date_set
    FROM {{ source('mart', 'star_order_2020') }}
    WHERE partition_date >= '2018-01-01'
),

calculation_dates AS (
    SELECT
        real_user_id,
        partition_date_set,
        day_join_msk AS partition_date_new
    FROM purchases

    UNION

    SELECT
        real_user_id,
        partition_date_set,
        partition_date AS partition_date_new
    FROM purchases

    UNION

    SELECT
        real_user_id,
        partition_date_set,
        partition_date + INTERVAL '3' MONTH AS partition_date_new
    FROM purchases

    UNION

    SELECT
        real_user_id,
        partition_date_set,
        partition_date + INTERVAL '6' MONTH AS partition_date_new
    FROM purchases

    UNION

    SELECT
        real_user_id,
        partition_date_set,
        partition_date + INTERVAL '12' MONTH AS partition_date_new
    FROM purchases
),

purchase_stats AS (
    SELECT
        real_user_id,
        partition_date_new AS active_window_dt,
        CARDINALITY(
            ARRAY_DISTINCT(
                TRANSFORM(
                    FILTER(
                        partition_date_set,
                        x -> x > partition_date_new - INTERVAL '3' MONTH
                        AND x <= partition_date_new
                    ), x -> MONTH(x) % 3
                )
            )
        ) AS stat_3m,
        CARDINALITY(
            ARRAY_DISTINCT(
                TRANSFORM(
                    FILTER(
                        partition_date_set,
                        x -> x > partition_date_new - INTERVAL '6' MONTH
                        AND x <= partition_date_new
                    ), x -> MONTH(x) % 6
                )
            )
        ) AS stat_6m,
        CARDINALITY(
            ARRAY_DISTINCT(
                TRANSFORM(
                    FILTER(
                        partition_date_set,
                        x -> x > partition_date_new - INTERVAL '12' MONTH
                        AND x <= partition_date_new
                    ), x -> MONTH(x)
                )
            )
        ) AS stat_12m
    FROM calculation_dates
),

user_segments_process AS (
    SELECT
        t1.*,
        active_window_dt AS day_msk,
        IF(
            stat_12m > 10,
            "Constant buyers",
            IF(
                stat_3m = 3 OR stat_6m >= 4,
                "Regular buyers",
                IF(
                    stat_3m = 0,
                    "Non-buyers",
                    "Occasional buyers"
                )
            )
        ) AS user_segment,
        LAG(active_window_dt, 3) OVER (
            PARTITION BY t1.real_user_id ORDER BY active_window_dt
        ) AS active_window_dt_prev3,
        LAG(active_window_dt, 2) OVER (
            PARTITION BY t1.real_user_id ORDER BY active_window_dt
        ) AS active_window_dt_prev2,
        LAG(active_window_dt, 1) OVER (
            PARTITION BY t1.real_user_id ORDER BY active_window_dt
        ) AS active_window_dt_prev
    FROM purchase_stats AS t1
),

user_segments_start AS (
    SELECT
        *,
        COALESCE(
            day_msk,
            active_window_dt_prev3 + INTERVAL 3 MONTH,
            active_window_dt_prev2 + INTERVAL 3 MONTH,
            active_window_dt_prev + INTERVAL 3 MONTH
        ) + INTERVAL 1 DAY AS effective_ts,
        IF(
            user_segment != LAG(user_segment) OVER (
                PARTITION BY real_user_id ORDER BY active_window_dt
            ),
            1,
            0
        ) AS user_segment_change_flg
    FROM user_segments_process
),

user_segments_end AS (
    SELECT
        *,
        COALESCE(
            LEAD(effective_ts) OVER (
                PARTITION BY real_user_id ORDER BY active_window_dt
            ) - INTERVAL 1 DAY,
            "9999-12-31"
        ) AS next_effective_ts,
        SUM(user_segment_change_flg) OVER (
            PARTITION BY real_user_id ORDER BY active_window_dt
        ) AS user_segment_change_cnt
    FROM user_segments_start
),

user_segments_agg AS (
    SELECT
        real_user_id,
        user_segment,
        user_segment_change_cnt,
        MIN(effective_ts) AS effective_ts,
        MAX(next_effective_ts) AS next_effective_ts
    FROM user_segments_end
    GROUP BY 1, 2, 3
)

SELECT
    real_user_id,
    user_segment,
    effective_ts,
    next_effective_ts
FROM user_segments_agg
