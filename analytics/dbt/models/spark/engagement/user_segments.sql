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
        round(datediff(week_msk, week_join_msk) / 28) AS cohort_month,
        round(datediff(week_msk, week_join_msk) / 7) AS cohort_week
    FROM
        activity
),

purchases AS (
    SELECT
        real_user_id,
        os_type,
        round(datediff(week_msk, week_join_msk) / 28) AS cohort_month,
        sum(num_orders) AS num_orders
    FROM (
        SELECT
            real_user_id,
            os_type,
            date(date_trunc('WEEK', real_user_join_ts_msk)) AS week_join_msk,
            date(date_trunc('WEEK', created_time_utc + INTERVAL 3 HOUR)) AS week_msk,
            count(*) AS num_orders
        FROM
            {{ source('mart', 'star_order_2020') }}
        WHERE
            date(created_time_utc + INTERVAL 3 HOUR) >= '2018-01-01'
        GROUP BY 1, 2, 3, 4
    )
    GROUP BY 1, 2, 3
),

activity_purch AS (
    SELECT
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
        activity2 AS t1
    LEFT JOIN purchases AS t2 ON
        t1.real_user_id = t2.real_user_id
        AND t1.cohort_month = t2.cohort_month
        AND t1.os_type = t2.os_type
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
        a.os_type,
        a.week_join_msk,
        a.num_orders,
        a.user1yearold_flg,
        u.time_unit,
        a.cohort_month AS active_dt,
        a.cohort_month + u.add_dt AS active_window_dt,
        if(a.num_orders = 1, u.segment * 1, 0) AS delta_action_cnt
    FROM (
        SELECT
            real_user_id,
            os_type,
            week_join_msk,
            cohort_month,
            user1yearold_flg,
            if(sum(num_orders) >= 1, 1, 0) AS num_orders
        FROM
            activity_purch
        GROUP BY 1, 2, 3, 4, 5
    ) AS a
    CROSS JOIN
        utility_table AS u
),

count_deltas AS (
    SELECT
        real_user_id,
        os_type,
        time_unit,
        week_join_msk,
        active_window_dt,
        user1yearold_flg,
        min(action_cnt) AS action_cnt
    FROM (
        SELECT
            real_user_id,
            os_type,
            week_join_msk,
            num_orders,
            user1yearold_flg,
            time_unit,
            active_dt,
            active_window_dt,
            delta_action_cnt,
            sum(delta_action_cnt) OVER (
                PARTITION BY real_user_id, os_type, time_unit
                ORDER BY active_window_dt
            ) AS action_cnt
        FROM
            activity_extended
    )
    GROUP BY 1, 2, 3, 4, 5, 6
),

count_deltas_quarter AS (
    SELECT
        real_user_id,
        os_type,
        week_join_msk,
        active_window_dt,
        action_cnt,
        user1yearold_flg
    FROM
        count_deltas
    WHERE
        time_unit = 'quarter'
),

count_deltas_half_year AS (
    SELECT
        real_user_id,
        os_type,
        week_join_msk,
        active_window_dt,
        action_cnt,
        user1yearold_flg
    FROM
        count_deltas
    WHERE
        time_unit = 'half_year'
),

count_deltas_year AS (
    SELECT
        real_user_id,
        os_type,
        week_join_msk,
        active_window_dt,
        action_cnt,
        user1yearold_flg
    FROM
        count_deltas
    WHERE
        time_unit = 'year'
),

segs_util AS (
    SELECT
        t1.real_user_id,
        t1.os_type,
        t1.week_join_msk,
        t1.active_window_dt,
        t1.action_cnt AS stat_3m,
        t1.user1yearold_flg,
        coalesce(t2.action_cnt, 0) AS stat_6m,
        coalesce(t3.action_cnt, 0) AS stat_12m
    FROM
        count_deltas_quarter AS t1
    LEFT JOIN count_deltas_half_year AS t2 ON
        t1.real_user_id = t2.real_user_id
        AND t1.os_type = t2.os_type
        AND t1.week_join_msk = t2.week_join_msk
        AND t1.active_window_dt = t2.active_window_dt
    LEFT JOIN count_deltas_year AS t3 ON
        t1.real_user_id = t3.real_user_id
        AND t1.os_type = t3.os_type
        AND t1.week_join_msk = t3.week_join_msk
        AND t1.active_window_dt = t3.active_window_dt
),

segs AS (
    SELECT
        real_user_id,
        os_type,
        week_join_msk,
        active_window_dt,
        stat_3m,
        stat_6m,
        stat_12m,
        if(stat_12m >= 10 AND user1yearold_flg, "Power",
            if(stat_3m = 3 OR stat_6m >= 4, "Core Power",
                if(stat_3m = 0, "At Risk", "Core Regular"))) AS user_segment
    FROM
        segs_util
),

segs_finall AS (
    SELECT
        t1.real_user_id,
        t1.week_msk,
        t1.os_type,
        t1.country,
        t1.week_join_msk,
        t1.user_id,
        t1.cohort_month,
        t1.cohort_week,
        t1.user1yearold_flg,
        t2.user_segment,
        t1.num_orders,
        t1.num_active_days
    FROM
        activity_purch AS t1
    INNER JOIN segs AS t2 ON
        t1.real_user_id = t2.real_user_id
        AND t1.os_type = t2.os_type
        AND t1.week_join_msk = t2.week_join_msk
        AND t1.cohort_month = t2.active_window_dt
)

SELECT * FROM segs_finall
