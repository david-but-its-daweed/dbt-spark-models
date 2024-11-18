SELECT null_fraction
FROM (
    SELECT COUNT(CASE WHEN brand_name IS NULL THEN 1 END) / COUNT(*) AS null_fraction
    FROM {{ source('joompro_analytics_mart', 'mercadolibre_products_snapshot') }}
    WHERE activity_status = "active"
) AS T
WHERE null_fraction >= 0.07
