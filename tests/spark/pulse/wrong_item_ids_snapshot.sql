SELECT id
FROM {{ source('joompro_analytics_mart', 'mercadolibre_products_snapshot') }}
WHERE id IS NULL OR id = product_id
