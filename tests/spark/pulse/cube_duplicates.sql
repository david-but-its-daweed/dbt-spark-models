SELECT 1 FROM
(
SELECT COUNT(DISTINCT p.id) as unique_items, COUNT(p.id) as n_rows
FROM {{ source('joompro_analytics_mart', 'cube_mlb_products') }} AS p
)
WHERE unique_items != n_rows
