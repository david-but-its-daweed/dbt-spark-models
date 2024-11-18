SELECT null_count
FROM (
    SELECT COUNT(*) AS null_count
    FROM {{ source('joompro_analytics_mart', 'mercadolibre_products_snapshot') }}
    WHERE
        brand_name IS NULL
        AND activity_status = "active"
) AS T
WHERE null_count >= 10000000
