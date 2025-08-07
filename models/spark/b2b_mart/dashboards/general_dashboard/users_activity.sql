{{
  config(
    schema='b2b_mart',
    file_format='delta',
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    meta = {
      'model_owner' : '@tigran',
      'bigquery_load': 'true'
    },
  )
}}

WITH authentication AS (
    SELECT
        user_id,
        MIN(CASE WHEN autorisation = 1 THEN event_msk_date END) AS f_autorisation_date,
        MIN(CASE WHEN registration = 1 THEN event_msk_date END) AS f_registration_date
    FROM {{ ref('ss_events_authentication') }}
    GROUP BY 1
),

device_type_ AS (
    SELECT
        user_id,
        event_msk_date,
        CASE
            WHEN osType IN ('android', 'ios', 'tizen', 'harmonyos') THEN 'mobile'
            WHEN osType IN ('ubuntu', 'linux', 'mac os', 'windows', 'chromium os') THEN 'desktop'
            ELSE 'other'
        END AS devise_type,
        ROW_NUMBER() OVER (PARTITION BY user_id, event_msk_date ORDER BY event_ts_msk) AS rn,
        FIRST_VALUE(osType) OVER (PARTITION BY user_id, event_msk_date ORDER BY event_ts_msk) AS day_f_device,
        FIRST_VALUE(osType) OVER (PARTITION BY user_id, DATE_TRUNC('WEEK', event_msk_date) ORDER BY event_ts_msk) AS week_f_device,
        FIRST_VALUE(osType) OVER (PARTITION BY user_id, DATE_TRUNC('MONTH', event_msk_date) ORDER BY event_ts_msk) AS month_f_device
    FROM {{ ref('ss_events_startsession') }}
    WHERE
        event_msk_date >= '2024-04-06'
        AND osType IS NOT NULL
),

device_type AS (
    SELECT
        user_id,
        event_msk_date,
        devise_type,
        day_f_device,
        week_f_device,
        month_f_device
    FROM device_type_
    WHERE rn = 1
),

dau AS (
    SELECT
        user_id,
        event_msk_date,
        CASE
            WHEN d.day_f_device IN ('android', 'ios', 'tizen', 'harmonyos') THEN 'mobile'
            WHEN d.day_f_device IN ('ubuntu', 'linux', 'mac os', 'windows', 'chromium os') THEN 'desktop'
            ELSE 'other'
        END AS device_type,
        MAX(startsession.utm_source) AS utm_source,
        MAX(startsession.utm_medium) AS utm_medium,
        MAX(startsession.utm_campaign) AS utm_campaign,
        MAX(startsession.active_user) AS is_active_user,
        MAX(UPPER(SPLIT(startsession.landing, '-')[1])) AS country_code,
        MAX(CASE WHEN event_msk_date >= auth.f_autorisation_date THEN 1 ELSE 0 END) AS autorisation,
        MAX(CASE WHEN event_msk_date >= auth.f_registration_date THEN 1 ELSE 0 END) AS registration
    FROM {{ ref('ss_events_startsession') }} AS startsession
    LEFT JOIN authentication AS auth USING (user_id)
    LEFT JOIN device_type AS d USING (user_id, event_msk_date)
    WHERE startsession.landing IN ('pt-br', 'es-mx') AND startsession.bot_flag != 1 AND (startsession.type = 'sessionStart' OR (startsession.type = 'bounceCheck' AND event_msk_date >= '2025-02-13'))
    GROUP BY 1, 2, 3
),

dau_prep AS (
    SELECT
        *,
        CASE
            WHEN registration = 1 THEN 'registration'
            WHEN autorisation = 1 THEN 'authorisation'
            WHEN is_active_user = 1 THEN 'active_user'
            ELSE 'inactive'
        END AS user_type,
        CASE
            WHEN utm_source IS NULL THEN 'organic'
            WHEN utm_medium = 'extension' THEN 'extension'
            WHEN utm_source = 'pulse' THEN 'pulse'
            ELSE 'advertising'
        END AS traffic_type,
        MIN(event_msk_date) OVER (PARTITION BY user_id) AS first_visit_date,
        DATEDIFF(event_msk_date, MIN(event_msk_date) OVER (PARTITION BY user_id)) AS day_number
    FROM dau
),

dau_final AS (
    SELECT
        event_msk_date AS date_msk,
        device_type,
        user_type,
        country_code,
        traffic_type,
        'DAU' AS metric,
        COUNT(DISTINCT user_id) AS num_of_users
    FROM dau_prep
    GROUP BY 1, 2, 3, 4, 5
),


wau_base AS (
    SELECT
        user_id,
        DATE_TRUNC('WEEK', event_msk_date) AS week_start,
        device_type,
        user_type,
        traffic_type,
        country_code
    FROM dau_prep
),

device_agg AS (
    SELECT
        user_id,
        week_start,
        CASE
            WHEN COUNT(DISTINCT device_type) > 1 THEN 'cross'
            ELSE ANY_VALUE(device_type)
        END AS device_type
    FROM wau_base
    GROUP BY 1, 2
),

user_type_priority AS (
    SELECT
        user_id,
        week_start,
        MIN(
            CASE user_type
                WHEN 'not active' THEN 1
                WHEN 'active_user' THEN 2
                WHEN 'authorisation' THEN 3
                WHEN 'registration' THEN 4
                ELSE 5
            END
        ) AS user_type_priority_num
    FROM wau_base
    GROUP BY 1, 2
),

user_type_resolved AS (
    SELECT
        user_id,
        week_start,
        CASE user_type_priority_num
            WHEN 1 THEN 'not active'
            WHEN 2 THEN 'active_user'
            WHEN 3 THEN 'authorisation'
            WHEN 4 THEN 'registration'
        END AS user_type
    FROM user_type_priority
),

traffic_priority AS (
    SELECT
        user_id,
        week_start,
        MIN(
            CASE traffic_type
                WHEN 'pulse' THEN 1
                WHEN 'extension' THEN 2
                WHEN 'advertising' THEN 3
                WHEN 'organic' THEN 4
                ELSE 5
            END
        ) AS traffic_type_priority
    FROM wau_base
    GROUP BY 1, 2
),

traffic_resolved AS (
    SELECT
        user_id,
        week_start,
        CASE traffic_type_priority
            WHEN 1 THEN 'pulse'
            WHEN 2 THEN 'extension'
            WHEN 3 THEN 'advertising'
            WHEN 4 THEN 'organic'
        END AS traffic_type
    FROM traffic_priority
),

extra_fields AS (
    SELECT
        user_id,
        week_start,
        ANY_VALUE(country_code) AS country_code
    FROM wau_base
    GROUP BY 1, 2
),

wau_final_prep AS (
    SELECT
        d.user_id,
        d.week_start,
        d.device_type,
        ut.user_type,
        tt.traffic_type,
        e.country_code
    FROM device_agg AS d
    INNER JOIN user_type_resolved AS ut USING (user_id, week_start)
    INNER JOIN traffic_resolved AS tt USING (user_id, week_start)
    INNER JOIN extra_fields AS e USING (user_id, week_start)
),

wau_final AS (
    SELECT
        week_start AS date_msk,
        device_type,
        user_type,
        country_code,
        traffic_type,
        'WAU' AS metric,
        COUNT(DISTINCT user_id) AS num_of_users
    FROM wau_final_prep
    GROUP BY 1, 2, 3, 4, 5
),


-- MAU
mau_base AS (
    SELECT
        user_id,
        DATE_TRUNC('MONTH', event_msk_date) AS month_start,
        device_type,
        user_type,
        traffic_type,
        country_code
    FROM dau_prep
),

device_agg_m AS (
    SELECT
        user_id,
        month_start,
        CASE
            WHEN COUNT(DISTINCT device_type) > 1 THEN 'cross'
            ELSE ANY_VALUE(device_type)
        END AS device_type
    FROM mau_base
    GROUP BY 1, 2
),

user_type_priority_m AS (
    SELECT
        user_id,
        month_start,
        MIN(
            CASE user_type
                WHEN 'not active' THEN 1
                WHEN 'active_user' THEN 2
                WHEN 'authorisation' THEN 3
                WHEN 'registration' THEN 4
                ELSE 5
            END
        ) AS user_type_priority_num
    FROM mau_base
    GROUP BY 1, 2
),

user_type_resolved_m AS (
    SELECT
        user_id,
        month_start,
        CASE user_type_priority_num
            WHEN 1 THEN 'not active'
            WHEN 2 THEN 'active_user'
            WHEN 3 THEN 'authorisation'
            WHEN 4 THEN 'registration'
        END AS user_type
    FROM user_type_priority_m
),

traffic_priority_m AS (
    SELECT
        user_id,
        month_start,
        MIN(
            CASE traffic_type
                WHEN 'pulse' THEN 1
                WHEN 'extension' THEN 2
                WHEN 'advertising' THEN 3
                WHEN 'organic' THEN 4
                ELSE 5
            END
        ) AS traffic_type_priority
    FROM mau_base
    GROUP BY 1, 2
),

traffic_resolved_m AS (
    SELECT
        user_id,
        month_start,
        CASE traffic_type_priority
            WHEN 1 THEN 'pulse'
            WHEN 2 THEN 'extension'
            WHEN 3 THEN 'advertising'
            WHEN 4 THEN 'organic'
        END AS traffic_type
    FROM traffic_priority_m
),

-- Остальные поля (без приоритета)
extra_fields_m AS (
    SELECT
        user_id,
        month_start,
        ANY_VALUE(country_code) AS country_code
    FROM mau_base
    GROUP BY 1, 2
),

-- Финальный результат
mau_final_prep AS (
    SELECT
        d.user_id,
        d.month_start,
        d.device_type,
        ut.user_type,
        tt.traffic_type,
        e.country_code
    FROM device_agg_m AS d
    INNER JOIN user_type_resolved_m AS ut USING (user_id, month_start)
    INNER JOIN traffic_resolved_m AS tt USING (user_id, month_start)
    INNER JOIN extra_fields_m AS e USING (user_id, month_start)
),

mau_final AS (
    SELECT
        month_start AS date_msk,
        device_type,
        user_type,
        country_code,
        traffic_type,
        'MAU' AS metric,
        COUNT(DISTINCT user_id) AS num_of_users
    FROM mau_final_prep
    GROUP BY 1, 2, 3, 4, 5
),


-- QAU
qau_base AS (
    SELECT
        user_id,
        DATE_TRUNC('QUARTER', event_msk_date) AS quarter_start,
        device_type,
        user_type,
        traffic_type,
        country_code
    FROM dau_prep
),

device_agg_q AS (
    SELECT
        user_id,
        quarter_start,
        CASE
            WHEN COUNT(DISTINCT device_type) > 1 THEN 'cross'
            ELSE ANY_VALUE(device_type)
        END AS device_type
    FROM qau_base
    GROUP BY 1, 2
),

user_type_priority_q AS (
    SELECT
        user_id,
        quarter_start,
        MIN(
            CASE user_type
                WHEN 'not active' THEN 1
                WHEN 'active_user' THEN 2
                WHEN 'authorisation' THEN 3
                WHEN 'registration' THEN 4
                ELSE 5
            END
        ) AS user_type_priority_num
    FROM qau_base
    GROUP BY 1, 2
),

user_type_resolved_q AS (
    SELECT
        user_id,
        quarter_start,
        CASE user_type_priority_num
            WHEN 1 THEN 'not active'
            WHEN 2 THEN 'active_user'
            WHEN 3 THEN 'authorisation'
            WHEN 4 THEN 'registration'
        END AS user_type
    FROM user_type_priority_q
),

traffic_priority_q AS (
    SELECT
        user_id,
        quarter_start,
        MIN(
            CASE traffic_type
                WHEN 'pulse' THEN 1
                WHEN 'extension' THEN 2
                WHEN 'advertising' THEN 3
                WHEN 'organic' THEN 4
                ELSE 5
            END
        ) AS traffic_type_priority
    FROM qau_base
    GROUP BY 1, 2
),

traffic_resolved_q AS (
    SELECT
        user_id,
        quarter_start,
        CASE traffic_type_priority
            WHEN 1 THEN 'pulse'
            WHEN 2 THEN 'extension'
            WHEN 3 THEN 'advertising'
            WHEN 4 THEN 'organic'
        END AS traffic_type
    FROM traffic_priority_q
),

extra_fields_q AS (
    SELECT
        user_id,
        quarter_start,
        ANY_VALUE(country_code) AS country_code
    FROM qau_base
    GROUP BY 1, 2
),

qau_final_prep AS (
    SELECT
        d.user_id,
        d.quarter_start,
        d.device_type,
        ut.user_type,
        tt.traffic_type,
        e.country_code
    FROM device_agg_q AS d
    INNER JOIN user_type_resolved_q AS ut USING (user_id, quarter_start)
    INNER JOIN traffic_resolved_q AS tt USING (user_id, quarter_start)
    INNER JOIN extra_fields_q AS e USING (user_id, quarter_start)
),

qau_final AS (
    SELECT
        quarter_start AS date_msk,
        device_type,
        user_type,
        country_code,
        traffic_type,
        'QAU' AS metric,
        COUNT(DISTINCT user_id) AS num_of_users
    FROM qau_final_prep
    GROUP BY 1, 2, 3, 4, 5
),


-- AAU
aau_base AS (
    SELECT
        user_id,
        DATE_TRUNC('YEAR', event_msk_date) AS year_start,
        device_type,
        user_type,
        traffic_type,
        country_code
    FROM dau_prep
),

device_agg_y AS (
    SELECT
        user_id,
        year_start,
        CASE
            WHEN COUNT(DISTINCT device_type) > 1 THEN 'cross'
            ELSE ANY_VALUE(device_type)
        END AS device_type
    FROM aau_base
    GROUP BY 1, 2
),

user_type_priority_y AS (
    SELECT
        user_id,
        year_start,
        MIN(
            CASE user_type
                WHEN 'not active' THEN 1
                WHEN 'active_user' THEN 2
                WHEN 'authorisation' THEN 3
                WHEN 'registration' THEN 4
                ELSE 5
            END
        ) AS user_type_priority_num
    FROM aau_base
    GROUP BY 1, 2
),

user_type_resolved_y AS (
    SELECT
        user_id,
        year_start,
        CASE user_type_priority_num
            WHEN 1 THEN 'not active'
            WHEN 2 THEN 'active_user'
            WHEN 3 THEN 'authorisation'
            WHEN 4 THEN 'registration'
        END AS user_type
    FROM user_type_priority_y
),

traffic_priority_y AS (
    SELECT
        user_id,
        year_start,
        MIN(
            CASE traffic_type
                WHEN 'pulse' THEN 1
                WHEN 'extension' THEN 2
                WHEN 'advertising' THEN 3
                WHEN 'organic' THEN 4
                ELSE 5
            END
        ) AS traffic_type_priority
    FROM aau_base
    GROUP BY 1, 2
),

traffic_resolved_y AS (
    SELECT
        user_id,
        year_start,
        CASE traffic_type_priority
            WHEN 1 THEN 'pulse'
            WHEN 2 THEN 'extension'
            WHEN 3 THEN 'advertising'
            WHEN 4 THEN 'organic'
        END AS traffic_type
    FROM traffic_priority_y
),

extra_fields_y AS (
    SELECT
        user_id,
        year_start,
        ANY_VALUE(country_code) AS country_code
    FROM aau_base
    GROUP BY 1, 2
),

aau_final_prep AS (
    SELECT
        d.user_id,
        d.year_start,
        d.device_type,
        ut.user_type,
        tt.traffic_type,
        e.country_code
    FROM device_agg_y AS d
    INNER JOIN user_type_resolved_y AS ut USING (user_id, year_start)
    INNER JOIN traffic_resolved_y AS tt USING (user_id, year_start)
    INNER JOIN extra_fields_y AS e USING (user_id, year_start)
),

aau_final AS (
    SELECT
        year_start AS date_msk,
        device_type,
        user_type,
        country_code,
        traffic_type,
        'AAU' AS metric,
        COUNT(DISTINCT user_id) AS num_of_users
    FROM aau_final_prep
    GROUP BY 1, 2, 3, 4, 5
),

final AS (
    SELECT * FROM dau_final
    UNION ALL
    SELECT * FROM wau_final
    UNION ALL
    SELECT * FROM mau_final
    UNION ALL
    SELECT * FROM qau_final
    UNION ALL
    SELECT * FROM aau_final
)

SELECT *
FROM final
