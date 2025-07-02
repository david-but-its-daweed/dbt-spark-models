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

WITH orders_precalc AS (
    SELECT
        "key" AS _key,
        product_id,
        pzn,
        store_name,
        SUM(before_products_price) AS before_products_price,
        SUM(quantity) AS quantity,
        COUNT(DISTINCT order_id) AS orders
    FROM onfy.orders_info
    WHERE partition_date >= CURRENT_DATE() - INTERVAL 366 DAY
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
    WHERE store_name IS NULL AND orders >= 100
),

ads_dashboard_predata AS (
    SELECT
        session_dt,
        partition_date,
        landing_page,
        landing_pzn,
        session_source,
        order_id,
        device_id,
        total_spend,
        gross_profit_initial,
        promocode_discount,
        session_spend,
        source,
        REGEXP_EXTRACT(landing_page, '/artikel/([^/?]+)', 1) AS medicine_id -- is there a better way ??
    FROM onfy.ads_dashboard ad
    WHERE partition_date >= "2025-04-01"
        AND source IN ('billiger', 'idealo', 'medizinfuchs')
        AND landing_page LIKE '/artikel/%'
        AND session_dt >= current_date() - interval 31 day
        AND session_dt < current_date() - 1
),

ads_dashboard_data AS (
    SELECT
        t.*,
        FROM_UNIXTIME(UNIX_TIMESTAMP(t.session_dt), 'yyyy-MM-dd HH:00:00') AS session_hour,
        DATE(session_dt) AS date,
        m.id AS product_id,
        m.country_local_id AS pzn
    FROM ads_dashboard_predata t
    INNER JOIN pharmacy_landing.medicine m
        ON (m.id = t.medicine_id OR m.country_local_id = t.medicine_id)
),

groupped_ads_dashboard AS (
    SELECT
        date,
        CASE WHEN source = "medizinfuchs" THEN "medizinfuchs-api" ELSE source END AS source,
        product_id,
        SUM(gross_profit_initial) AS sum_gross_profit_initial_onfy,
        COUNT(*) AS visits_from_pa,
        COUNT(DISTINCT order_id) AS orders
    FROM ads_dashboard_data
    GROUP BY
        date,
        source,
        product_id
),

base_data AS (
    SELECT
        product_id,
        channel,
        effective_ts,
        pharmacy_name,
        product_price
    FROM pharmacy.marketing_channel_price_fast_scd2
    WHERE
        effective_ts >= CURRENT_DATE() - INTERVAL 31 DAY
        AND effective_ts < CURRENT_DATE() - 1
        AND status = "ACTIVE"
        AND product_id IN (
            SELECT DISTINCT product_id
            FROM orders
        )
        AND LOWER(pharmacy_name) NOT LIKE "%amazon%"
        AND LOWER(pharmacy_name) NOT LIKE "%ebay%"
        AND LOWER(pharmacy_name) != "tablettenbote.de"
),

calendar AS (
    SELECT DISTINCT
        channel,
        effective_ts
    FROM base_data
),

product_list AS (
    SELECT DISTINCT
        channel,
        product_id,
        pharmacy_name
    FROM base_data
),

full_calendar AS (
    SELECT
        p.product_id,
        p.channel,
        p.pharmacy_name,
        t.effective_ts
    FROM product_list p
    INNER JOIN calendar t
        ON p.channel = t.channel
),

joined_data AS (
    SELECT
        fc.product_id,
        fc.channel,
        fc.pharmacy_name,
        fc.effective_ts,
        bd.product_price
    FROM full_calendar AS fc
    LEFT JOIN base_data AS bd
    ON 
        fc.product_id = bd.product_id
        AND fc.channel = bd.channel
        AND fc.pharmacy_name = bd.pharmacy_name
        AND fc.effective_ts = bd.effective_ts
),

filled_data AS ( --As we don't parse all goods every hour, we keep last available price (but no longer than 1 day)
    SELECT
        *,
        LAST(
            CASE WHEN product_price IS NOT NULL THEN effective_ts END,
            TRUE
        ) OVER (
            PARTITION BY product_id, channel, pharmacy_name
            ORDER BY effective_ts
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS last_seen_ts,
        LAST(
            product_price,
            TRUE
        ) OVER (
            PARTITION BY product_id, channel, pharmacy_name
            ORDER BY effective_ts
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS raw_filled_price
    FROM joined_data
),

filtered_filled_data AS (
    SELECT
        product_id,
        channel,
        pharmacy_name,
        effective_ts,
        UNIX_TIMESTAMP(effective_ts) - UNIX_TIMESTAMP(last_seen_ts) AS age_seconds,
        last_seen_ts,
        CASE
            WHEN UNIX_TIMESTAMP(effective_ts) - UNIX_TIMESTAMP(last_seen_ts) <= 93600 THEN raw_filled_price -- day + 2 hours
        END AS filled_price
    FROM filled_data
),

ranked_prices AS (
    SELECT
        product_id,
        channel,
        effective_ts,
        last_seen_ts,
        age_seconds,
        pharmacy_name,
        filled_price,
        DENSE_RANK() OVER (
            PARTITION BY product_id, channel, effective_ts
            ORDER BY filled_price ASC
        ) AS price_rank
    FROM filtered_filled_data
    WHERE filled_price IS NOT NULL
),

onfy_ranked AS (
    SELECT
        product_id,
        channel,
        effective_ts,
        price_rank AS onfy_rank
    FROM ranked_prices
    WHERE LOWER(pharmacy_name) = "onfy.de"
),

cheapest_pharmacy AS (
    SELECT
        product_id,
        channel,
        effective_ts,
        pharmacy_name AS cheapest_pharmacy
    FROM ranked_prices
    WHERE price_rank = 1
),

aggregated AS (
    SELECT
        product_id,
        channel,
        effective_ts,
        MIN(last_seen_ts) AS min_last_seen_ts,
        MIN(filled_price) AS min_price,
        MAX(filled_price) AS max_price,
        AVG(filled_price) AS mean_price,
        PERCENTILE(filled_price, 0.25) AS percentile_25_price,
        PERCENTILE(filled_price, 0.5) AS median_price,
        PERCENTILE(filled_price, 0.75) AS percentile_75_price,
        STDDEV(filled_price) AS std_price,
        COLLECT_LIST(pharmacy_name) AS pharmacy_name_list,
        MIN(CASE WHEN LOWER(pharmacy_name) = "onfy.de" THEN filled_price END) AS price_onfy
    FROM ranked_prices
    GROUP BY
        product_id,
        channel,
        effective_ts
),

goods_in_base AS (
    SELECT
        medicine.id AS product_id,
        medicine.country_local_id AS pzn,
        product.name AS product_name,
        product_store.price AS base_price
    FROM pharmacy_landing.medicine
    LEFT JOIN pharmacy_landing.product AS product
        ON medicine.id = product.id
    LEFT JOIN pharmacy_landing.product_store
        ON medicine.id = product_store.product_id
),

united AS (
    SELECT
        a.*,
        DATE(a.effective_ts) AS date,
        DATE_FORMAT(DATE(a.effective_ts), "EEEE") AS day_of_week,
        DATE_FORMAT(DATE(a.effective_ts), "MMMM") AS month_name,
        o.onfy_rank,
        c.cheapest_pharmacy,
        gnb.pzn,
        gnb.product_name,
        gnb.base_price,
        CASE
            WHEN a.price_onfy IS NOT NULL AND gnb.base_price IS NOT NULL
                THEN (gnb.base_price - a.price_onfy) / gnb.base_price
        END AS discount,
        orders.orders
    FROM aggregated AS a
    LEFT JOIN onfy_ranked AS o
        ON a.product_id = o.product_id AND a.channel = o.channel AND a.effective_ts = o.effective_ts
    LEFT JOIN cheapest_pharmacy AS c
        ON a.product_id = c.product_id AND a.channel = c.channel AND a.effective_ts = c.effective_ts
    LEFT JOIN goods_in_base AS gnb
        ON a.product_id = gnb.product_id
    INNER JOIN orders
        ON a.product_id = orders.product_id
),

united_groupped AS (
    SELECT
        date,
        channel,
        pzn,
        product_id,
        FIRST(orders) AS num_total_orders,
        FIRST(day_of_week) AS day_of_week,
        FIRST(month_name) AS month_name,
        FIRST(product_name) AS product_name,
        ROUND(AVG(price_onfy), 2) AS mean_onfy_price,
        ROUND(AVG(onfy_rank), 2) AS mean_onfy_rank,
        ROUND(AVG(discount), 2) AS mean_onfy_discount,
        ROUND(FIRST(base_price), 2) AS onfy_base_price,
        ROUND(MIN(min_price), 2) AS min_competitors_price_of_the_day,
        ROUND(AVG(mean_price), 2) AS mean_competitors_price,
        ROUND(AVG(percentile_25_price), 2) AS mean_competitors_price_25_percentile,
        ROUND(AVG(median_price), 2) AS mean_competitors_price_median,
        ROUND(AVG(percentile_75_price), 2) AS mean_competitors_price_75_percentile,
        ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(pharmacy_name_list))) AS pharmacy_name_set
    FROM united
    WHERE
        price_onfy IS NOT NULL
    GROUP BY date, channel, pzn, product_id
)


SELECT
    united_groupped.*,
    groupped_ads_dashboard.sum_gross_profit_initial_onfy,
    groupped_ads_dashboard.visits_from_pa,
    groupped_ads_dashboard.orders
FROM united_groupped
LEFT JOIN groupped_ads_dashboard
    ON
        united_groupped.product_id = groupped_ads_dashboard.product_id
        AND united_groupped.channel = groupped_ads_dashboard.source
        AND united_groupped.date = groupped_ads_dashboard.date