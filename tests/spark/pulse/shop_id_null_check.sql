SELECT shop_id
FROM {{ source('joompro_analytics_mart', 'mercadolibre_products_snapshot') }}
WHERE shop_id IS NULL OR shop_id <= 0
