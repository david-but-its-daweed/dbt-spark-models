WITH cub AS (
    SELECT COUNT(*) AS cube_count
    FROM {{ source('joompro_analytics', 'mercadolibre_categories_reviews_cube_js_weekly') }} AS c
    WHERE c.is_current
),

cat AS (
    SELECT COUNT(*) AS cat_count
    FROM {{ source('joompro_mart', 'mercadolibre_categories_view') }}
)

SELECT
    cub.cube_count,
    cat.cat_count
FROM cub
CROSS JOIN cat
WHERE cub.cube_count != cat.cat_count
