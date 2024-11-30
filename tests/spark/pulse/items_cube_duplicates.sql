SELECT id
FROM {{ source('joompro_analytics_mart', 'cube_mlb_products') }}
GROUP BY id
HAVING COUNT("*") > 1
