WITH cub AS (
    SELECT COUNT(DISTINCT p.id) AS items_cube
    FROM {{ source('joompro_analytics_mart', 'cube_mlb_products') }} AS p
)

SELECT 1
FROM (
    SELECT SUM(c.total_items) AS items_categories
    FROM {{ source('joompro_mart', 'mercadolibre_categories') }} AS c
    WHERE c.level = 1
) AS categories
CROSS JOIN cub
WHERE cub.items_cube / categories.items_categories < 0.65
