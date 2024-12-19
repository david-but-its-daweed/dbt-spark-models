SELECT gmv_total
FROM (
    SELECT
        SUM(gmv_1w) / COUNT(DISTINCT partition_date) AS gmv_total,
        partition_date
    FROM {{ source('joompro_analytics', 'mercadolibre_categories_reviews_cube_js_weekly') }}
    WHERE
        l2_id IS NULL
        AND partition_date >= '2024-07-01'
        AND partition_date != '2024-10-28'
    GROUP BY partition_date
)
WHERE gmv_total > 2700000000 * 1.25 * 1.5 * 4 OR gmv_total < 2700000000 / 1.25 / 1.5 * 4
