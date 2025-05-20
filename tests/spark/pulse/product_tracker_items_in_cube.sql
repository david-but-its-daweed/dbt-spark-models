WITH cub AS (
    SELECT id, 1 AS flag
    FROM {{ source('joompro_analytics_mart', 'cube_mlb_products') }}
)

SELECT DISTINCT tr.id
FROM {{ source('joompro_analytics', 'product_tracker_items') }} AS tr
    LEFT JOIN cub ON tr.id = cub.id
WHERE cub.flag IS NULL
