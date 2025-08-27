WITH products_sat AS (
    SELECT spcd.product_id
    FROM {{ source('joompro_analytics', 'sat_product_creation_date') }} AS spcd
)

SELECT products_snapshot.product_id
FROM (
    SELECT DISTINCT mps.product_id
    FROM {{ source('joompro_analytics_mart', 'mercadolibre_products_snapshot') }} AS mps
    WHERE mps.product_id NOT LIKE 'MLB-%'
) AS products_snapshot
LEFT ANTI JOIN products_sat ON products_snapshot.product_id = products_sat.product_id
