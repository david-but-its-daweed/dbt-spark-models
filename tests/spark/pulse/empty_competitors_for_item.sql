SELECT id
FROM {{ source('joompro_analytics_mart', 'cube_mlb_connected_stores_listings') }}
WHERE competing_products_count = 0
