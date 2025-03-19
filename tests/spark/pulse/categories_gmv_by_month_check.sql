SELECT gmv_total
FROM (
    SELECT
        SUM(gmv_1m) / COUNT(DISTINCT partition_date) AS gmv_total,
        partition_date
    FROM {{ source('joompro_analytics', 'mercadolibre_categories_cube_js_monthly') }}
    WHERE
        l2_id IS NULL
        AND partition_date >= '2024-07-01'
        AND partition_date != '2024-10-28'
    GROUP BY partition_date
)
WHERE (gmv_total > 3645000000 * 4 * 1.25 * 1.7 OR gmv_total < 3645000000 * 4 / 1.25 / 1.5) AND dayofmonth(current_date()) <= 4
