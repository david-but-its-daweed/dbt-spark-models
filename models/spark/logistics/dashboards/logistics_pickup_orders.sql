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
  LEFT JOIN {{ ref('china_zip_codes_regions') }} AS seed ON seed.zip_code_range_id = zip._id
),

min_region AS (-- Если к одному zip_code подходит 2 региона, то берем регион с наименьшим количеством zip_code -> так регионы должны быть точнее
  SELECT
    zip_code,
    MIN_BY(region_name, zip_code_size) AS region_name
  FROM regions_by_zip
  GROUP BY 1
)

SELECT
  orders._id AS pickup_order_id,
  SPLIT(orders.channelId, '-')[0] AS shipper,
  COALESCE(min_region.region_name, 'N/A') AS region_name,
  orders.merchantId AS merchant_id,
  DATE(IF(FILTER(statusHistory, x -> x.status = 1) IS NULL, NULL, FILTER(statusHistory, x -> x.status = 1)[0].updatedTime)) AS created_date_utc,
  IF(FILTER(statusHistory, x -> x.status = 1) IS NULL, NULL, FILTER(statusHistory, x -> x.status = 1)[0].updatedTime) AS created_datetime_utc,
  IF(FILTER(statusHistory, x -> x.status = 2) IS NULL, NULL, FILTER(statusHistory, x -> x.status = 2)[0].updatedTime) AS shipped_datetime_utc,
  IF(FILTER(statusHistory, x -> x.status = 3) IS NULL, NULL, FILTER(statusHistory, x -> x.status = 3)[0].updatedTime) AS checked_in_datetime_utc,
  IF(FILTER(statusHistory, x -> x.status = 4) IS NULL, NULL, FILTER(statusHistory, x -> x.status = 4)[0].updatedTime) AS delivered_datetime_utc,
  IF(FILTER(statusHistory, x -> x.status = 5) IS NULL, NULL, FILTER(statusHistory, x -> x.status = 5)[0].updatedTime) AS cancelled_datetime_utc
FROM {{ source('mongo', 'logistics_pickup_orders_daily_snapshot') }} AS orders
LEFT JOIN min_region ON orders.pickupAddress.zipCode = min_region.zip_code

