{{ config(
    schema = 'search',
    materialized = 'table',
    file_format = 'parquet',
    partition_by = ['partition_date'],
    meta = {
        'model_owner': '@itangaev',
        'team': 'search',
        'bigquery_load': 'true'
    }
) }}

WITH params AS (
    SELECT CAST(1.96 AS DOUBLE) AS z
),

user_day_dim AS (
    SELECT
        search_date,
        device_id,
        search_type,
        device_country,
        os_type,
        COUNT(DISTINCT textQueryOrCategory) AS queries,
        COUNT(DISTINCT IF(relevance = 1 AND has_purchase = 1, textQueryOrCategory, NULL)) AS queries_with_success
    FROM {{ ref('search_success_result') }}
    WHERE
        search_date IS NOT NULL
        AND search_type <> "unknown_search"
        AND os_type IN ("android", "ios")
    GROUP BY 1, 2, 3, 4, 5
),

-- Дневные агрегаты по стране/OS + их тоталы через GROUPING SETS (без CI/PI)
daily_dim_raw AS (
    SELECT
        search_date,
        search_type,
        device_country,
        os_type,

        SUM(queries) AS sum_queries,
        SUM(queries_with_success) AS sum_queries_with_success,
        ROUND(sum_queries_with_success / sum_queries, 4) AS success_rate_by_query,

        COUNT_IF(queries > 0) AS sum_users,
        COUNT_IF(queries_with_success > 0) AS sum_users_with_success,
        ROUND(sum_users_with_success / sum_users, 4) AS success_rate_by_user
    FROM user_day_dim
    GROUP BY
        search_date,
        search_type,
        GROUPING SETS (
            (device_country, os_type),     -- полный срез
            (device_country),              -- тотал по OS
            (os_type),                     -- тотал по стране
            ()                             -- grand total
        )
),

-- 2a) Тотал на день (для «полосы»)
daily_total AS (
    SELECT
        search_date,
        search_type,
        sum_queries,
        sum_queries_with_success,
        success_rate_by_query,
        sum_users,
        sum_users_with_success,
        success_rate_by_user
    FROM daily_dim_raw
    WHERE device_country IS NULL AND os_type IS NULL   -- grand total
),

-- 3) Скользящие суммы за 28 дней (делаю только по тоталу, потому что остальные срезы совсем шумные)
roll AS (
    SELECT
        d.*,
        SUM(sum_users) OVER (
            PARTITION BY search_type
            ORDER BY CAST(search_date AS TIMESTAMP)
            RANGE BETWEEN INTERVAL 27 DAYS PRECEDING AND CURRENT ROW
        ) AS roll_n,
        SUM(sum_users_with_success) OVER (
            PARTITION BY search_type
            ORDER BY CAST(search_date AS TIMESTAMP)
            RANGE BETWEEN INTERVAL 27 DAYS PRECEDING AND CURRENT ROW
        ) AS roll_s
    FROM daily_total d
),

-- 4) Wilson CI + Prediction Interval (только для тотала)
band AS (
    SELECT
        r.*,
        CASE WHEN r.roll_n > 0 THEN r.roll_s * 1.0 / r.roll_n END AS p_roll,

        -- Wilson 95% CI для p_roll
        GREATEST(
            0.0,
            (
                (
                    (r.roll_s * 1.0 / r.roll_n)
                    + (POW(p.z, 2) / (2.0 * r.roll_n))
                )
                / (1.0 + (POW(p.z, 2) / r.roll_n))
                -
                (
                    p.z
                    * SQRT(
                        (
                            (r.roll_s * 1.0 / r.roll_n)
                            * (1.0 - (r.roll_s * 1.0 / r.roll_n))
                        ) / r.roll_n
                        + POW(p.z, 2) / (4.0 * r.roll_n * r.roll_n)
                    )
                )
                / (1.0 + (POW(p.z, 2) / r.roll_n))
            )
        ) AS ci_low,
        LEAST(
            1.0,
            (
                (
                    (r.roll_s * 1.0 / r.roll_n)
                    + (POW(p.z, 2) / (2.0 * r.roll_n))
                )
                / (1.0 + (POW(p.z, 2) / r.roll_n))
                +
                (
                    p.z
                    * SQRT(
                        (
                            (r.roll_s * 1.0 / r.roll_n)
                            * (1.0 - (r.roll_s * 1.0 / r.roll_n))
                        ) / r.roll_n
                        + POW(p.z, 2) / (4.0 * r.roll_n * r.roll_n)
                    )
                )
                / (1.0 + (POW(p.z, 2) / r.roll_n))
            )
        ) AS ci_high,

        -- Prediction Interval 95% для дня (шум по sum_users)
        CASE WHEN r.sum_users > 0 THEN GREATEST(
            0.0,
            (r.roll_s * 1.0 / r.roll_n)
            - p.z * SQRT(
                (r.roll_s * 1.0 / r.roll_n)
                * (1.0 - (r.roll_s * 1.0 / r.roll_n))
                / r.sum_users
            )
        ) END AS pi_low,
        CASE WHEN r.sum_users > 0 THEN LEAST(
            1.0,
            (r.roll_s * 1.0 / r.roll_n)
            + p.z * SQRT(
                (r.roll_s * 1.0 / r.roll_n)
                * (1.0 - (r.roll_s * 1.0 / r.roll_n))
                / r.sum_users
            )
        ) END AS pi_high

    FROM roll r
    CROSS JOIN params p
    WHERE r.roll_n >= 100
)

-- 5) Финал: total-полоса (с интервалами) + детализация по стране/OS (включая их тоталы), без дубля grand total
SELECT
    b.search_date,
    b.search_type,
    "total" AS device_country,
    "total" AS os_type,

    b.sum_queries,
    b.sum_queries_with_success,
    b.success_rate_by_query,
    b.sum_users,
    b.sum_users_with_success,
    b.success_rate_by_user,

    DATEDIFF(CURRENT_DATE(), CAST(d.search_date AS DATE)) <= 13 AS is_incomplete_data,
    b.pi_low,
    b.pi_high,
    CASE
        WHEN b.success_rate_by_user < b.pi_low OR b.success_rate_by_user > b.pi_high THEN TRUE
        ELSE FALSE
    END AS is_outlier
FROM band b

UNION ALL

SELECT
    d.search_date,
    d.search_type,
    COALESCE(d.device_country, "total") AS device_country,
    COALESCE(d.os_type, "total") AS os_type,

    d.sum_queries,
    d.sum_queries_with_success,
    d.success_rate_by_query,
    d.sum_users,
    d.sum_users_with_success,
    d.success_rate_by_user,

    DATEDIFF(CURRENT_DATE(), CAST(d.search_date AS DATE)) <= 13 AS is_incomplete_data,

    CAST(NULL AS DOUBLE) AS pi_low,
    CAST(NULL AS DOUBLE) AS pi_high,
    CAST(NULL AS BOOLEAN) AS is_outlier
FROM daily_dim_raw d
WHERE NOT (d.device_country IS NULL AND d.os_type IS NULL)  -- выкинуть grand total, чтобы не дублировать «полосу»

ORDER BY
    search_date,
    search_type,
    device_country,
    os_type;