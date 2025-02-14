{{
  config(
    enabled=false,
    materialized='table',
    alias='js_prices_log',
    file_format='parquet',
    schema='category_management',
    meta = {
        'model_owner' : '@vasiukova_mn',
        'bigquery_load': 'false'
    }
  )
}} 

----- Firstly, we create an artificial calendar, which consists of a list of dates in a row.
with calendar_dt AS ( 
    SELECT col AS partition_date
    FROM (
        SELECT EXPLODE(sequence(to_date(CURRENT_DATE() - INTERVAL 365 DAY), to_date(CURRENT_DATE()), interval 1 day))
    )
),

----- Then we are detecting for each product_id in promo table the list of dates when it had product discounts
----- And we take maximum discount for each product at a particular day
promo_calendar AS (
    SELECT
        partition_date,
        product_id,
        MAX(discount) / 100 as discount
    FROM calendar_dt AS c
    LEFT JOIN {{ source('mart', 'promotions') }} AS p -- can be optimized using inner join. Left join is a relic from the last algorithm idea.
        ON partition_date >= promo_start_time_utc
        AND partition_date < promo_end_time_utc
    WHERE promo_start_time_utc >= CURRENT_DATE() - INTERVAL 365 DAY
    GROUP BY 1, 2
),

--------------------------------------------------------------------------    
---- As far as dim_published_variant_with_merchant contains 
---- many lines for each variant with the same price,
---- we shoud define effective period of a particular price for each variant
variants_stg_ex1 AS (
    SELECT
        variant_id,
        product_id,
        price,
        currency,
        value_partition,
        MAX(current_price) AS current_price,
        -- Forming new effective periods of each price
        MIN(effective_ts) AS effective_ts, -- define when the price starts to be valid
        MAX(next_effective_ts) AS next_effective_ts -- define when the price ends to be valid
    FROM (
        SELECT
            variant_id,
            product_id,
            price,
            currency,
            effective_ts,
            next_effective_ts,
            SUM(value_partition) over (PARTITION BY variant_id, product_id ORDER BY effective_ts ) AS value_partition, -- So here ve derive unique marks for periods with a constant price
            FIRST(price) OVER (PARTITION BY variant_id, product_id ORDER BY effective_ts DESC) AS current_price -- here we get the last (actual) price
        FROM (
            SELECT
                variant_id,
                product_id,
                price,
                currency,
                effective_ts AS effective_ts,
                next_effective_ts AS next_effective_ts,
                CASE WHEN IF(price_lag IS NULL OR price_lag!= price, effective_ts, NULL) IS NULL THEN 0 ELSE 1 END AS value_partition  ---- The idea is to mark сonsecutive time periods where price didn't change. It's the preliminary step, where mark only raws with price changing. 
            FROM (
                SELECT
                    variant_id,
                    product_id,
                    price / 1000000 AS price,
                    currency,
                    effective_ts,
                    next_effective_ts,
                    LAG(p.price / 1000000) OVER (PARTITION BY variant_id, product_id ORDER BY next_effective_ts) AS price_lag --- Firstly we define price in previous effective period
                FROM {{ source('mart', 'dim_published_variant_with_merchant') }} AS p
                )
            )
        )
    GROUP BY 1, 2, 3, 4, 5
),

--- As far as dim_published_variant_with_merchant contains info about price in a merchant currency, we need to convert it into usd for comparing with other prices.
--- So, here we can find currency rate for each day for any currency. 
currency_rates AS (
    SELECT
        currency_code,
        rate / 1000000 AS rate,
        effective_date,
        next_effective_date
    FROM {{ source('mart', 'dim_currency_rate') }}
)

--- Here we are merging data on prices with information on promotional discounts and calculating aggregates to determine the average price during a promotion.
SELECT 
    v.effective_ts,
    v.next_effective_ts,
    v.product_id,
    v.variant_id,
    v.price,
    v.currency,
    v.current_price,
    SUM(v.price * COALESCE((1 - p.discount), 1)) AS sum_price_with_discount,
    COUNT(p.partition_date) AS days_with_promo
FROM variants_stg_ex1 AS v
-- MB we need to change left join to inner, because variants_stg_ex1 is unlimited, but promo_calendar is reduced to last year
LEFT JOIN promo_calendar AS p 
    ON
        v.product_id = p.product_id
        AND p.partition_date >= v.effective_ts
        AND p.partition_date < v.next_effective_ts
GROUP BY
    v.effective_ts,
    v.next_effective_ts,
    v.product_id,
    v.variant_id,
    v.price,
    v.current_price,
    v.currency