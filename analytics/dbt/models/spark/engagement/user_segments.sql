{{ config(
    schema='engagement',
    materialized='table',
    file_format='delta',
    meta = {
      'team': 'clan',
      'bigquery_load': 'true'
    }
) }}



WITH activity AS (
    SELECT
        t2.real_user_id,
        date(date_trunc('WEEK', t1.date_msk)) as week_msk,
        first(t1.os_type) AS os_type,
        min(date(date_trunc('WEEK', t2.real_user_join_ts_msk))) AS week_join_msk,
        first(t1.user_id) AS user_id,
        upper(first(t1.country)) AS country,
        count(*) AS num_evs
    FROM (
        SELECT
            device_id,
            date_msk,
            os_type,
            user_id,
            country
        FROM
            {{ source('mart', 'star_active_device') }}
        WHERE
            date_msk >= '2018-01-01'
            AND os_type IN ('android', 'ios', 'trampolineweb', 'fullweb')
            AND NOT ephemeral
    ) AS t1
    INNER JOIN {{ source('mart', 'link_device_real_user') }} AS t2 ON
        t1.device_id = t2.device_id
    GROUP BY 1, 2
),

activity2 AS (
    SELECT
        real_user_id,
        week_msk,
        os_type,
        country,
        week_join_msk,
        user_id,
        num_evs,
        floor(datediff(week_msk, week_join_msk) / 28) AS cohort_month,
        floor(datediff(week_msk, week_join_msk) / 7) AS cohort_week
    FROM
        activity
),

users_first_identification AS (
    SELECT
        real_user_id,        
        os_type,
        week_join_msk,
        user_id,
        country
    FROM 
        activity2
    WHERE
        cohort_week = 0
),

purchases AS (
    SELECT
        real_user_id,
        floor(datediff(week_msk, week_join_msk) / 7) AS cohort_week,
        sum(num_orders) AS num_orders
    FROM (
        SELECT
            real_user_id,
            date(date_trunc('WEEK', real_user_join_ts_msk)) AS week_join_msk,
            date(date_trunc('WEEK', created_time_utc + INTERVAL 3 HOUR)) AS week_msk,
            count(*) AS num_orders
        FROM
            {{ source('mart', 'star_order_2020') }}
        WHERE
            date(created_time_utc + INTERVAL 3 HOUR) >= '2018-01-01'
        GROUP BY 1, 2, 3
    )
    GROUP BY 1, 2
),

activity_purch AS (
    SELECT
        t0.os_type as os_type__first,
        t0.user_id as user_id__first,
        t0.country as country__first,
        t1.real_user_id,
        t1.week_msk,
        t1.os_type,
        t1.country,
        t1.week_join_msk,
        t1.user_id,
        t1.num_evs AS num_active_days,
        t1.cohort_month,
        t1.cohort_week,
        if(t2.num_orders >= 1, 1, 0) AS num_orders,
        if(datediff(t1.week_msk, t1.week_join_msk) > 364,
            true, false) AS user1yearold_flg
    FROM
        users_first_identification AS t0 
    JOIN activity2 AS t1 ON
        t0.real_user_id = t1.real_user_id
        AND t0.week_join_msk = t1.week_join_msk
    LEFT JOIN purchases AS t2 ON
        t1.real_user_id = t2.real_user_id
        AND t1.cohort_week = t2.cohort_week
),

util1 AS (
    SELECT explode(array('year', 'quarter', 'half-year')) AS time_unit
),

util2 AS (
    SELECT explode(array(-1, 1)) AS segment
),

utility_table AS (
    SELECT
        u1.time_unit,
        u2.segment,
        if(u2.segment = 1, 0, if(u1.time_unit = 'quarter', 3,
            if(u1.time_unit = 'half-year', 6, 12))) AS add_dt
    FROM
        util1 AS u1
    CROSS JOIN
        util2 AS u2
),

activity_extended AS (
    SELECT
        a.real_user_id,
        a.week_join_msk,
        a.num_orders,
        u.time_unit,
        a.cohort_month AS active_dt,
        a.cohort_month + u.add_dt AS active_window_dt,
        if(a.num_orders = 1, u.segment * 1, 0) AS delta_action_cnt
    FROM (
        SELECT
            real_user_id,
            week_join_msk,
            cohort_month,
            if(sum(num_orders) >= 1, 1, 0) AS num_orders
        FROM
            activity_purch
        GROUP BY 1, 2, 3
    ) AS a
    CROSS JOIN
        utility_table AS u
),

activity_extended_grouped AS (
    SELECT
        real_user_id,
        week_join_msk,
        time_unit,
        active_window_dt,
        sum(delta_action_cnt) AS delta_action_cnt
    FROM
        activity_extended
    GROUP BY 1, 2, 3, 4
),

count_deltas AS (
    SELECT
        real_user_id,
        week_join_msk,
        time_unit,
        active_window_dt,
        sum(delta_action_cnt) OVER (
            PARTITION BY real_user_id, time_unit
            ORDER BY active_window_dt
        ) AS action_cnt
    FROM
        activity_extended_grouped
),

count_deltas_quarter AS (
    SELECT
        real_user_id,
        week_join_msk,
        active_window_dt,
        action_cnt
    FROM
        count_deltas
    WHERE
        time_unit = 'quarter'
),

count_deltas_half_year AS (
    SELECT
        real_user_id,
        week_join_msk,
        active_window_dt,
        action_cnt
    FROM
        count_deltas
    WHERE
        time_unit = 'half-year'
),

count_deltas_year AS (
    SELECT
        real_user_id,
        week_join_msk,
        active_window_dt,
        action_cnt
    FROM
        count_deltas
    WHERE
        time_unit = 'year'
),

segs_util AS (
    SELECT
        t1.real_user_id,
        t1.week_join_msk,
        t1.active_window_dt,
        t1.action_cnt AS stat_3m,
        coalesce(t2.action_cnt, 0) AS stat_6m,
        coalesce(t3.action_cnt, 0) AS stat_12m
    FROM
        count_deltas_quarter AS t1
    LEFT JOIN count_deltas_half_year AS t2 ON
        t1.real_user_id = t2.real_user_id
        AND t1.week_join_msk = t2.week_join_msk
        AND t1.active_window_dt = t2.active_window_dt
    LEFT JOIN count_deltas_year AS t3 ON
        t1.real_user_id = t3.real_user_id
        AND t1.week_join_msk = t3.week_join_msk
        AND t1.active_window_dt = t3.active_window_dt
),

segs AS (
    SELECT
        real_user_id,
        week_join_msk,
        active_window_dt,
        stat_3m,
        stat_6m,
        stat_12m,
        if(stat_12m >= 10 AND active_window_dt >= 12, "Power",
            if(stat_3m = 3 OR stat_6m >= 4, "Core Power",
                if(stat_3m = 0, "At Risk", "Core Regular"))) AS user_segment
    FROM
        segs_util
),

segs_finall AS (
    SELECT
        t1.os_type__first AS os_type,
        t1.user_id__first AS user_id,
        t1.country__first AS country,
        t1.user1yearold_flg,
        t1.real_user_id,
        t1.week_msk,
        t1.os_type AS os_type__week_msk,
        t1.country AS country__week_msk,
        t1.week_join_msk,
        t1.user_id AS user_id__week_msk,
        t1.cohort_month,
        t1.cohort_week,
        t2.user_segment,
        t1.num_orders,
        t1.num_active_days
    FROM
        activity_purch AS t1
    INNER JOIN segs AS t2 ON
        t1.real_user_id = t2.real_user_id
        AND t1.week_join_msk = t2.week_join_msk
        AND t1.cohort_month = t2.active_window_dt
)

select
    *
from
    segs_finall
