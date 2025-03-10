SELECT
    APPROX_COUNT_DISTINCT(id) AS items,
    partition_date
FROM {{ source('joompro_analytics', 'ods_mlb_mobile_api_product_previews') }}
WHERE
    partition_date >= DATE_SUB(CURRENT_DATE(), 2)
GROUP BY partition_date
HAVING APPROX_COUNT_DISTINCT(id) < 100000000
