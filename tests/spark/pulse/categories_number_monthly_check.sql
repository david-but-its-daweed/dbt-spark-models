WITH cub AS (
    SELECT DISTINCT category_id
    FROM {{ source('joompro_analytics', 'mercadolibre_categories_cube_js_monthly') }} AS c
    WHERE c.is_current
),


dates AS (
    SELECT MAX(DATE(partition_date)) as date
    FROM {{ source('joompro_analytics', 'mercadolibre_categories_cube_js_monthly') }} AS c
    WHERE c.is_current
)

SELECT cat.category_id
FROM {{ source('joompro_mart', 'mercadolibre_categories_view') }} AS cat
LEFT JOIN cub ON cat.category_id = cub.category_id
CROSS JOIN dates
WHERE cub.category_id IS NULL
AND DATE(cat.date_created) < dates.date AND dayofmonth(current_date()) <= 4
