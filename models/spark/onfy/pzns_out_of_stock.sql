{{ config(
    schema='onfy',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@helbuk',
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': '#onfy-etl-monitoring'
    }
) }}

---------------------------------------------------------------------

-- RU: Собираем данные по товарам, бывшим в out-of-stock в аптеках в посление 30 дней
-- ENG: Collecting data on out-of-stock items in pharmacies in the last 30 days

---------------------------------------------------------------------

WITH Parameters AS (
    SELECT
        90 AS statWindow,
        CURRENT_DATE - INTERVAL '1' DAY AS end_of_window,
        CURRENT_DATE - INTERVAL '91' DAY AS start_of_window, -- statWindow + 1
        10 AS min_active_days
),

filtered_orders AS (
    SELECT
        oi.*,  -- Select all columns FROM the orders_info
        opi.id AS opi_id,
        opi.product_id,
        opi.order_parcel_id AS parcel_id,
        opi.price AS opi_price,
        pi.before_price,
        pi.after_price
    FROM onfy.orders_info AS oi
    INNER JOIN pharmacy_landing.order_parcel_item AS opi
        ON
            oi.product_id = opi.product_id
            AND oi.parcel_id = opi.order_parcel_id
    LEFT JOIN pharmacy_landing.price_applier_applied_for_object AS pi
        ON
            opi.id = pi.object_id
            AND pi.object_type = 'ORDER_PARCEL_ITEM'
            AND pi.applier_type = 'DISCOUNT_BY_TOKEN'
    WHERE
        oi.order_created_time_cet >= (SELECT start_of_window FROM Parameters)
        AND oi.order_created_time_cet <= (SELECT end_of_window FROM Parameters)
),

totalGmv AS (
    SELECT
        pzn,  -- Grouping by 'pzn'
        store_name,
        SUM(quantity * COALESCE(before_price, item_price)) AS gmv,
        AVG(COALESCE(before_price, item_price)) AS avg_price
    FROM filtered_orders
    GROUP BY pzn, store_name
),

stocks AS (
    SELECT
        store.store_name,
        medicine.pzn,
        ps.effective_ts,
        ps.next_effective_ts,
        CASE WHEN ps.quantity > 0 THEN TRUE ELSE FALSE END AS in_stock
    FROM pharmacy.product_store_fast_scd2 AS ps
    INNER JOIN (
        SELECT id AS store_id, name AS store_name
        FROM pharmacy_landing.store
    ) AS store
    ON
        ps.store_id = store.store_id
    INNER JOIN (
        SELECT id AS product_id, country_local_id AS pzn 
        FROM pharmacy_landing.medicine
    ) AS medicine
    ON
      ps.product_id = medicine.product_id
    INNER JOIN totalGmv
    ON 
      medicine.pzn = totalGmv.pzn
      AND store.store_name == totalGmv.store_name
),
    
AdjustedStocks AS (
    SELECT
        pzn,
        store_name,
        GREATEST(effective_ts, (SELECT start_of_window FROM Parameters)) AS adjusted_effective_ts,
        LEAST(next_effective_ts, (SELECT end_of_window FROM Parameters)) AS adjusted_next_effective_ts,
        in_stock
    FROM stocks
    WHERE
        effective_ts <= (SELECT end_of_window FROM Parameters)
        AND next_effective_ts >= (SELECT start_of_window FROM Parameters)
),

AggregatedStocks AS (
    SELECT
        pzn,
        store_name,
        SUM(CASE WHEN in_stock THEN
            ((UNIX_TIMESTAMP(adjusted_next_effective_ts) - UNIX_TIMESTAMP(adjusted_effective_ts)) / 86400.0) 
        ELSE 0 END) AS active,
        SUM(CASE WHEN NOT in_stock THEN 
            ((UNIX_TIMESTAMP(adjusted_next_effective_ts) - UNIX_TIMESTAMP(adjusted_effective_ts)) / 86400.0) 
        ELSE 0 END) AS not_active
    FROM AdjustedStocks
    GROUP BY pzn, store_name
),

FilteredStocks AS (
    SELECT *
    FROM AggregatedStocks
    WHERE 
        (active + not_active) >= ((SELECT statWindow FROM Parameters) - 1) 
        AND active >= (SELECT min_active_days FROM Parameters)
),

ActiveDailyGmv AS (
    SELECT 
        f.pzn, 
        f.store_name,
        (t.gmv / f.active) AS gmv_per_active_day,
        t.avg_price as avg_item_price
    FROM FilteredStocks f
    INNER JOIN totalGmv t 
    ON
      f.pzn = t.pzn
      AND f.store_name = t.store_name
),

EndTime AS (
    SELECT MAX(effective_ts) AS value FROM stocks
),

StartTime AS (
    SELECT value - INTERVAL '30' DAY AS start_time FROM EndTime
),

pznTitle AS (
    SELECT 
        medicine.country_local_id AS pzn,
        product.name AS title
    FROM pharmacy_landing.medicine AS medicine
    JOIN pharmacy_landing.product AS product
    ON 
      medicine.id = product.id
),

AdjustedStocks2 AS (
    SELECT 
        pzn,
        store_name,
        GREATEST(effective_ts, (SELECT start_time FROM StartTime)) AS adjusted_effective_ts,
        LEAST(next_effective_ts, (SELECT value FROM EndTime)) AS adjusted_next_effective_ts,
        in_stock,
        MAX(CAST(in_stock AS INT)) OVER (
            PARTITION BY pzn, store_name 
            ORDER BY effective_ts DESC
        ) AS rnk
    FROM stocks
    WHERE 
        effective_ts <= (SELECT value FROM EndTime)
        AND next_effective_ts >= (SELECT start_time FROM StartTime)
),

AggregatedData2 AS (
    SELECT 
        pzn,
        store_name,
        SUM(CASE 
            WHEN in_stock = FALSE 
            THEN ((UNIX_TIMESTAMP(adjusted_next_effective_ts) - UNIX_TIMESTAMP(adjusted_effective_ts)) / 86400.0) 
            ELSE 0 
        END) AS days_not_active_total,
        SUM(CASE 
            WHEN rnk = 0 
            THEN ((UNIX_TIMESTAMP(adjusted_next_effective_ts) - UNIX_TIMESTAMP(adjusted_effective_ts)) / 86400.0) 
            ELSE 0 
        END) AS days_not_active_now
    FROM AdjustedStocks2
    GROUP BY pzn, store_name
),

FilteredData AS (
    SELECT
        a.pzn,
        a.store_name,
        a.days_not_active_total,
        a.days_not_active_now,
        g.gmv_per_active_day,
        g.avg_item_price
    FROM AggregatedData2 AS a
    INNER JOIN ActiveDailyGmv AS g
        ON (
            a.pzn = g.pzn
            AND a.store_name = g.store_name
        )
    WHERE a.days_not_active_now >= 0.01
)

SELECT
    f.store_name,
    f.pzn,
    p.title,
    ROUND(f.days_not_active_total, 0) AS days_not_active_total, -- Total days when unavailable in last 30
    ROUND(f.days_not_active_now, 0) AS days_not_active_now, -- Num days since last availability 
    ROUND(f.gmv_per_active_day, 2) AS gmv_per_active_day, 
    ROUND(f.avg_item_price, 2) AS avg_item_price
FROM FilteredData AS f
INNER JOIN pznTitle AS p
    ON f.pzn = p.pzn
ORDER BY f.gmv_per_active_day DESC