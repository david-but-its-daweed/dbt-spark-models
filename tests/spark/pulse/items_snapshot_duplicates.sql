SELECT id
FROM {{ source('joompro_analytics_mart', 'mercadolibre_products_snapshot') }}
GROUP BY id
HAVING COUNT("*") > 1
