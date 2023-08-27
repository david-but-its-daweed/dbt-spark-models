{{ config(
    schema='customer_routing',
    incremental_strategy='insert_overwrite',
    materialized='incremental',
    file_format='parquet',
    partition_by=['partition_date_msk'],
    meta = {
      'team': 'clan',
      'model_owner': '@ekutynina',
      'bigquery_load': 'true',
      'bigquery_partitioning_date_column': 'partition_date_msk',
      'bigquery_fail_on_missing_partitions': 'false'
    }
) }}

WITH orders AS (
    SELECT
        day,
        platform,
        country,
        gmv_initial,
        gmv_final
    FROM
        (
            SELECT
                partition_date AS day,
                order_id,
                UPPER(shipping_country) AS country,
                LOWER(os_type) AS platform,
                ROUND(gmv_initial, 3) AS gmv_initial,
                ROUND(gmv_final, 3) AS gmv_final
            FROM {{ source('mart', 'star_order_2020') }}
            WHERE
                partition_date < CURRENT_DATE()
                AND partition_date >= CURRENT_DATE() - INTERVAL 240 DAY
        )
),

sales_calendar AS (
    SELECT DISTINCT
        promo_start_date,
        promo_end_date,
        sale_period
    FROM (
        SELECT
            CASE
                WHEN INSTR(LOWER(promo_title), ":") > 0 THEN SPLIT_PART(LOWER(promo_title), ":", 1)
                WHEN INSTR(LOWER(promo_title), " — ") > 0 THEN SPLIT_PART(LOWER(promo_title), " — ", 1)
                WHEN INSTR(LOWER(promo_title), "sale") > 0 THEN SUBSTRING(LOWER(promo_title), 1, INSTR(LOWER(promo_title), "sale") + 4)
                ELSE LOWER(promo_title)
            END AS promo_title,
            DATE(promo_start_time_utc) AS promo_start_date,
            DATE(promo_end_time_utc) AS promo_end_date,
            DATEDIFF(DATE(promo_end_time_utc), DATE(promo_start_time_utc)) AS sale_period,
            COUNT(*) AS cnt
        FROM {{ source('mart', 'promotions') }}
        WHERE
            DATE(promo_start_time_utc) >= "2023-01-01"
            AND (INSTR(LOWER(promo_title), LOWER("Weekly Promotion")) = 0 OR INSTR(LOWER(promo_title), LOWER("Weekly Promotion")) IS NULL)
            AND DATEDIFF(DATE(promo_end_time_utc), DATE(promo_start_time_utc)) < 12
            AND INSTR(LOWER(promo_title), LOWER("sale")) > 0
            AND DATEDIFF(DATE(promo_end_time_utc), DATE(promo_start_time_utc)) > 2
            AND SPLIT_PART(LOWER(promo_title), "sale", 1) != ""
        GROUP BY 1, 2, 3, 4
    )
)

SELECT
    o.day AS partition_date_msk,
    platform,
    CASE
        WHEN a.promo_start_date IS NOT NULL THEN CONCAT(CONCAT(a.promo_start_date, "-"), a.promo_end_date)
        ELSE "no_sales"
    END AS sale_type,
    a.promo_start_date AS start_of_sale,
    a.promo_end_date AS end_of_sale,
    a.sale_period,
    SUM(o.gmv_initial) AS gmv_initial
FROM orders AS o
LEFT JOIN sales_calendar AS a ON o.day >= a.promo_start_date AND o.day < a.promo_end_date
WHERE
    1 = 1
    {% if is_incremental() %}
        AND o.day >= DATE'{{ var("start_date_ymd") }}'
        AND o.day < DATE'{{ var("end_date_ymd") }}'
    {% else %}
        AND o.day   >= DATE'2022-12-15'
    {% endif %}
GROUP BY 1, 2, 3, 4, 5, 6