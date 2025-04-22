SELECT DISTINCT shop_id
FROM {{ source('joompro_analytics_mart', 'mercadolibre_products_snapshot') }}
WHERE shop_name IS NULL OR shop_url IS NULL