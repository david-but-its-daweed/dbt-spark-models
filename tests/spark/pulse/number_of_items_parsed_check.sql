SELECT 1
FROM
    (
        SELECT
            *,
            MAX(partition_date) OVER () AS max_pt
        FROM {{ source('joompro_analytics_mart', 'items_parsed_number') }}
    )
WHERE partition_date = max_pt
HAVING SUM(products_parsed) < SUM(products_in_api) * 0.8
