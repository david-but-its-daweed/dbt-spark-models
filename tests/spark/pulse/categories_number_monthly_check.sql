WITH cub AS (
    SELECT DISTINCT category_id AS cube_count
    FROM {{ source('joompro_analytics', 'mercadolibre_categories_cube_js_monthly') }} AS c
    WHERE c.is_current
)

SELECT cat.category_id
FROM {{ source('joompro_mart', 'mercadolibre_categories_view') }} AS cat
WHERE cat.category_id NOT IN cub
AND DATE(cat.date_created) < (SELECT MAX(partition_date) + interval 1 MONTH from {{ source('joompro_analytics', 'mercadolibre_categories_cube_js_monthly') }})
