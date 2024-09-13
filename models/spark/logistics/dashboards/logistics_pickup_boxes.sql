{{
  config(
    materialized='table',
    meta = {
        'model_owner' : '@general_analytics',
        'bigquery_load': 'true'
    },
  )
}}


WITH regions_by_zip AS (
  SELECT
    COALESCE(seed.chinese_region_name, 'N/A') AS region_name,
    EXPLODE(zip.zipcodeRange) AS zip_code,
    SIZE(zip.zipcodeRange) AS zip_code_size
  FROM {{ source('mongo', 'logistics_zipcode_range_snapshots_daily_snapshot') }} AS zip
  INNER JOIN {{ ref('china_zip_codes_regions') }} AS seed ON seed.zip_code_range_id = zip._id
),

min_region AS (-- Если к одному zip_code подходит 2 региона, то берем регион с наименьшим количеством zip_code -> так регионы должны быть точнее
  SELECT
    zip_code,
    MIN_BY(region_name, zip_code_size) AS region_name
  FROM regions_by_zip
  GROUP BY 1
),

pickup_orders AS (
    SELECT
        orders._id AS pickup_order_id,
        EXPLODE(actualBoxes) AS boxes,
        SPLIT(orders.channelId, '-')[0] AS shipper,
        COALESCE(min_region.region_name, 'N/A') AS region_name,
        orders.merchantId AS merchant_id
    FROM {{ source('mongo', 'logistics_pickup_orders_daily_snapshot') }} AS orders
    LEFT JOIN min_region ON orders.pickupAddress.zipCode = min_region.zip_code
    WHERE orders.createdTime > '2023-11-19' -- более ранних данных нет в таблице core_pickup_request_boxes_daily_snapshot
)

SELECT
    sh.b AS box_barcode,
    pickup_orders.shipper AS shipper,
    pickup_orders.region_name AS region_name,
    pickup_orders.merchant_id AS merchant_id,
    IF(FILTER(sh, x -> x.s = 1) IS NULL, NULL, FILTER(sh, x -> x.s = 1)[0].ut) AS shipped_datetime_utc,
    IF(FILTER(sh, x -> x.s = 2) IS NULL, NULL, FILTER(sh, x -> x.s = 2)[0].ut) AS checked_in_datetime_utc,
    IF(FILTER(sh, x -> x.s = 3) IS NULL, NULL, FILTER(sh, x -> x.s = 3)[0].ut) AS delivered_datetime_utc,
    IF(FILTER(sh, x -> x.s = 4) IS NULL, NULL, FILTER(sh, x -> x.s = 4)[0].ut) AS cancelled_datetime_utc
FROM {{ source('mongo', 'core_pickup_request_boxes_daily_snapshot') }} AS sh
LEFT JOIN pickup_orders ON pickup_orders.boxes.barcode = sh.b
WHERE
  sh.st >= '1900-01-01'