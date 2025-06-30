{{ config(
    schema='onfy',
    materialized='table',
    file_format='delta',
    meta = {
      'model_owner' : '@helbuk',
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': '#onfy-etl-monitoring',
      'bigquery_overwrite': 'true'
    }
) }}

-- --------------------------------------------------------------------------------
-- Competitor Pharmacy Pricing Statistics Table
-- --------------------------------------------------------------------------------

-- Purpose:
--     This query computes detailed summary statistics for pharmacy competitors
--     listed on price comparison channels such as 'idealo', 'billiger',
--     'medizinfuchs-api' and others

-- Description:
--     - Builds a base dataset of product prices per pharmacy, channel.
--     - Computes rank of each pharmacy's price (lower = cheaper) within each
--       product, channel, and date group.
--     - Aggregates key statistics per pharmacy and channel.

-- Key Metrics:
--     - num_products: Number of unique products sold by this pharmacy on this channel.
--     - num_products_onfy_intersections: Number of products shared with Onfy.
--     - avg_rank: Average price rank of this pharmacy (1 = lowest price).
--     - mean_price, min_price, max_price: Basic price level statistics.
--     - median_price (25%, 50%, 75% percentiles): Price distribution shape.
--     - std_price: Price volatility (standard deviation).
--     - onfy_price_comparison: Average relative price difference to Onfy.
--     - num_dates: Number of unique dates the pharmacy had offers for these products.

-- Notes:
--     - Only active product listings (status = 'ACTIVE') are included.
--     - Pharmacies with fewer than 1,000 distinct products are excluded.
--     - Only products where Onfy had > 100 orders in the past year
--     - Onfy pharmacies and channels which had > 10 product intersection with Onfy 

-- Usage:
--     Used to evaluate how competitive each pharmacy is on different channels,
--     in terms of price positioning and overlap with Onfy.de.


WITH orders_precalc AS (
    SELECT
        "key" AS _key,
        product_id,
        pzn,
        store_name,
        SUM(before_products_price) AS before_products_price,
        SUM(quantity) AS quantity,
        COUNT(DISTINCT order_id) AS orders
    FROM {{ source('onfy', 'orders_info') }}
    WHERE
        partition_date >= CURRENT_DATE() - INTERVAL 366 DAY
    GROUP BY
        GROUPING SETS (
            (product_id, pzn, store_name),
            (product_id, pzn)
        )
),

orders AS (
    SELECT
        orders_precalc.*,
        totals.before_products_price_total,
        totals.quantity_total,
        totals.orders_total
    FROM orders_precalc
    INNER JOIN (
        SELECT
            "key" AS _key,
            SUM(before_products_price) AS before_products_price_total,
            SUM(orders) AS orders_total,
            SUM(quantity) AS quantity_total
        FROM orders_precalc
        WHERE store_name IS NULL
    ) AS totals
    ON totals._key = orders_precalc._key
),

base AS (
    SELECT
        product_id,
        channel,
        DATE(effective_ts) AS date,
        DATE_FORMAT(DATE(effective_ts), "EEEE") AS day_of_week,
        DATE_FORMAT(DATE(effective_ts), "MMMM") AS month_name,
        pharmacy_name,
        product_price,
        MIN(CASE WHEN LOWER(pharmacy_name) = "onfy.de" THEN product_price END) OVER (PARTITION BY product_id, channel, effective_ts) AS price_onfy
    FROM {{ source('pharmacy', 'marketing_channel_price_fast_scd2') }}
    WHERE
        effective_ts >= CURRENT_DATE() - INTERVAL 91 DAY
        AND effective_ts < CURRENT_DATE() - 1
        --AND channel IN ('idealo', 'billiger', 'medizinfuchs-api')
        AND status = "ACTIVE"
        AND product_id IN (
            SELECT DISTINCT product_id
            FROM orders
            WHERE store_name IS NULL AND orders >= 100
        )
),

ranked_base AS (
    SELECT
        *,
        DENSE_RANK() OVER (
            PARTITION BY product_id, channel, date
            ORDER BY product_price ASC
        ) AS price_rank
    FROM base
)

SELECT
    channel,
    pharmacy_name,
    COUNT(DISTINCT product_id) AS num_products,
    COUNT(DISTINCT CASE WHEN price_onfy IS NOT NULL THEN product_id END) AS num_products_onfy_intersections,
    AVG(price_rank) AS mean_rank,
    AVG(product_price) AS mean_price,
    MIN(product_price) AS min_price,
    MAX(product_price) AS max_price,
    PERCENTILE(product_price, 0.25) AS percentile_25_price,
    PERCENTILE(product_price, 0.5) AS median_price,
    PERCENTILE(product_price, 0.75) AS percentile_75_price,
    STDDEV(product_price) AS std_price,
    AVG(CASE WHEN price_onfy IS NOT NULL THEN (product_price - price_onfy) / price_onfy END) AS onfy_price_comparison,
    COUNT(DISTINCT date) AS num_dates_stats
FROM ranked_base
GROUP BY
    channel,
    pharmacy_name
HAVING
    COUNT(DISTINCT product_id) >= 1000
    AND num_products_onfy_intersections > 10