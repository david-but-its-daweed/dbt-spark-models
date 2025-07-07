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
    FROM {{ source('onfy', 'orders_info') }}
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
    FROM {{ source('onfy', 'ads_dashboard') }} ad
    WHERE 
        source IN ('billiger', 'idealo', 'medizinfuchs')
        AND landing_page LIKE '/artikel/%'
        AND session_dt >= CURRENT_DATE() - interval 91 day
        AND session_dt < CURRENT_DATE() - 1
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
    FROM {{ source('pharmacy', 'marketing_channel_price_fast_scd2') }} 
    WHERE
        effective_ts >= CURRENT_DATE() - INTERVAL 91 DAY
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

ranked_prices AS (
    SELECT
        product_id,
        channel,
        effective_ts,
        pharmacy_name,
        product_price,
        DENSE_RANK() OVER (
            PARTITION BY product_id, channel, effective_ts
            ORDER BY product_price ASC
        ) AS price_rank
    FROM base_data
    WHERE product_price IS NOT NULL
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

first_rank AS (
    SELECT
        product_id,
        channel,
        effective_ts,
        product_price AS first_rank_price
    FROM ranked_prices
    WHERE price_rank = 1
),

second_rank AS (
    SELECT
        product_id,
        channel,
        effective_ts,
        product_price AS second_rank_price
    FROM ranked_prices
    WHERE price_rank = 2
),

third_rank AS (
    SELECT
        product_id,
        channel,
        effective_ts,
        product_price AS third_rank_price
    FROM ranked_prices
    WHERE price_rank = 3
),

aggregated AS (
    SELECT
        product_id,
        channel,
        effective_ts,
        MIN(product_price) AS min_price,
        MAX(product_price) AS max_price,
        AVG(product_price) AS mean_price,
        PERCENTILE(product_price, 0.25) AS percentile_25_price,
        PERCENTILE(product_price, 0.5) AS median_price,
        PERCENTILE(product_price, 0.75) AS percentile_75_price,
        STDDEV(product_price) AS std_price,
        COLLECT_LIST(pharmacy_name) AS pharmacy_name_list,
        MIN(CASE WHEN LOWER(pharmacy_name) = "onfy.de" THEN product_price END) AS price_onfy
    FROM ranked_prices
    GROUP BY
        product_id,
        channel,
        effective_ts
),

united AS (
    SELECT
        a.*,
        DATE(a.effective_ts) AS date,
        DATE_FORMAT(DATE(a.effective_ts), "EEEE") AS day_of_week,
        DATE_FORMAT(DATE(a.effective_ts), "MMMM") AS month_name,
        o.onfy_rank,
        c.cheapest_pharmacy,
        first_rank.first_rank_price,
        second_rank.second_rank_price,
        third_rank.third_rank_price,
        orders.orders,
        product.name AS product_name
    FROM aggregated AS a
    LEFT JOIN onfy_ranked AS o
        ON a.product_id = o.product_id AND a.channel = o.channel AND a.effective_ts = o.effective_ts
    LEFT JOIN cheapest_pharmacy AS c
        ON a.product_id = c.product_id AND a.channel = c.channel AND a.effective_ts = c.effective_ts
    LEFT JOIN first_rank
        ON a.product_id = first_rank.product_id AND a.channel = first_rank.channel AND a.effective_ts = first_rank.effective_ts
    LEFT JOIN second_rank
        ON a.product_id = second_rank.product_id AND a.channel = second_rank.channel AND a.effective_ts = second_rank.effective_ts
    LEFT JOIN third_rank
        ON a.product_id = third_rank.product_id AND a.channel = third_rank.channel AND a.effective_ts = third_rank.effective_ts
    INNER JOIN orders
        ON a.product_id = orders.product_id
    LEFT JOIN {{ source('pharmacy_landing', 'product') }}
        ON a.product_id = product.id

),

united_groupped AS (
    SELECT
        date,
        channel,
        product_id,
        FIRST(orders) AS num_total_orders,
        FIRST(day_of_week) AS day_of_week,
        FIRST(month_name) AS month_name,
        FIRST(product_name) AS product_name,
        ROUND(AVG(price_onfy), 2) AS mean_onfy_price,
        ROUND(AVG(onfy_rank), 2) AS mean_onfy_rank,
        ROUND(MIN(min_price), 2) AS min_competitors_price_of_the_day,
        ROUND(AVG(mean_price), 2) AS mean_competitors_price,
        ROUND(AVG(percentile_25_price), 2) AS mean_competitors_price_25_percentile,
        ROUND(AVG(median_price), 2) AS mean_competitors_price_median,
        ROUND(AVG(percentile_75_price), 2) AS mean_competitors_price_75_percentile,
        ROUND(MIN(first_rank_price), 2) AS mean_first_rank_price,
        ROUND(MIN(second_rank_price), 2) AS mean_second_rank_price,
        ROUND(MIN(third_rank_price), 2) AS mean_third_rank_price,
        ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(pharmacy_name_list))) AS pharmacy_name_set
    FROM united
    WHERE
        price_onfy IS NOT NULL
    GROUP BY date, channel, product_id
)


SELECT
    united_groupped.*,
    ROUND(groupped_ads_dashboard.sum_gross_profit_initial_onfy, 2) AS sum_gross_profit_initial_onfy,
    ROUND(groupped_ads_dashboard.visits_from_pa, 2) AS visits_from_pa,
    ROUND(groupped_ads_dashboard.orders, 2) AS orders,
    SIZE(united_groupped.pharmacy_name_set) AS num_competitors
FROM united_groupped
LEFT JOIN groupped_ads_dashboard
    ON
        united_groupped.product_id = groupped_ads_dashboard.product_id
        AND united_groupped.channel = groupped_ads_dashboard.source
        AND united_groupped.date = groupped_ads_dashboard.date
