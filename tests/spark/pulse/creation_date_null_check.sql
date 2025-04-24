SELECT id
FROM {{ source('joompro_analytics_mart', 'mercadolibre_products_snapshot') }}
WHERE listing_creation_date IS NULL
     AND effective_ts >= DATE_SUB(CURRENT_DATE(), 7)
     AND effective_ts < DATE_SUB(CURRENT_DATE(), 2)
