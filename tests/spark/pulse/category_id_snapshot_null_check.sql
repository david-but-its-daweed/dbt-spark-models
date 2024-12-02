SELECT id
FROM {{ source('joompro_analytics_mart', 'mercadolibre_products_snapshot') }}
WHERE category_id IS NULL
