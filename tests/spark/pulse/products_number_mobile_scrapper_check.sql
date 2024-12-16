SELECT items_count
FROM (
    SELECT APPROX_COUNT_DISTINCT(id) AS items_count
    FROM {{ source('joompro_analytics', 'ods_mlb_mobile_api_product_previews') }}
    WHERE partition_date >= DATE_SUB(CURRENT_DATE(), 3)
)
WHERE items_count < 10000000
