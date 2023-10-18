{{
  config(
    meta = {
      'model_owner' : '@yushkov',
      'bigquery_load': 'true'
    },
    materialized='table',
    schema='jl_models',
  )
}}

WITH bucket_data AS (
    SELECT
        partition_date,
        is_last_week,
        weight,
        dangerous_kinds,
        country,
        ROUND(merchant_sale_price, 2) AS merchant_sale_price,
        XXHASH64(weight, dangerous_kinds, country, ROUND(merchant_sale_price, 2)) AS bucket_id,
        COUNT(*) AS bucket_jl_order_count,
        SUM(gmv_initial) AS bucket_gmv_initial
    FROM {{ ref('parcels_for_price_comparison') }}
    GROUP BY 1, 2, 3, 4, 5, 6, 7
),

bounds AS (
    SELECT
        COALESCE(starttime, TIMESTAMP(0)) AS start_time_utc,
        COALESCE(endtime, TIMESTAMP('9999-01-01')) AS end_time_utc,
        destinationcountry AS destination_country,
        -- TODO Хорошо бы начать использовать валюту, а то в будущем границы могут быть в разных валютах (может и сейчас для каких-то особенных стран) или выпилить поле
        shippingtypebounds.currency AS currency_code,
        EXPLODE(shippingtypebounds.bounds) AS bound
    FROM {{ source ('mongo', 'logistics_jl_obligatory_channels_daily_snapshot') }}
    WHERE sourcecountry = 'CN'
),

price_buck AS (
    SELECT
        start_time_utc,
        end_time_utc,
        destination_country,
        bound.shippingtype AS shipping_type,
        COALESCE(IF(bound.shippingtype != 'NRM', 0, bound.min / 1e6), 0) AS min_price,
        COALESCE(IF(bound.max = 0, NULL, bound.max / 1e6), 9999) AS max_price
    FROM bounds

    UNION -- добавлено, так как в MD нет RM
    SELECT
        TIMESTAMP(0) AS start_time_utc,
        TIMESTAMP('9999-01-01') AS end_time_utc,
        'MD' AS destination_country,
        'RM' AS shipping_type,
        0 AS min_price,
        9999 AS max_price
),

tariffs_jl AS (
    SELECT
        tar.partition_date,
        tar.country,
        tar.channel_id,
        tar.min_weight,
        tar.max_weight,
        tar.dangerous_kinds,
        tar.per_item,
        tar.per_kg,
        pb.min_price,
        pb.max_price
    FROM {{ ref('jl_tariffs') }} AS tar
    LEFT JOIN price_buck AS pb
        ON
            tar.country = pb.destination_country
            AND tar.shipping_type = pb.shipping_type
            AND tar.partition_date BETWEEN pb.start_time_utc AND pb.end_time_utc
),

add_channel_price AS (
    SELECT
        bd.*,
        ali.per_item + ali.per_kg * bd.weight AS ali_price,
        ROW_NUMBER() OVER (PARTITION BY bd.partition_date, bd.bucket_id ORDER BY ali.per_item + ali.per_kg * bd.weight) AS ali_row_num,
        jtb.per_item + jtb.per_kg * bd.weight AS jtb_price,
        ROW_NUMBER() OVER (PARTITION BY bd.partition_date, bd.bucket_id ORDER BY jtb.per_item + jtb.per_kg * bd.weight) AS jtb_row_num
    FROM bucket_data AS bd
    LEFT JOIN {{ ref('ali_tariffs') }} AS ali
        ON
            GREATEST(bd.weight, 0.01) >= ali.min_weight -- тут не надо greatest, но @savinov хотел симметрию, убрать как починят тарифы али 0-0.01
            AND GREATEST(bd.weight, 0.01) < ali.max_weight
            AND IF(CARDINALITY(ARRAY_EXCEPT(bd.dangerous_kinds, ARRAY(0))) != 0, 'dangerous', 'non_dangerous') = ali.dangerous_type
            AND bd.country = ali.country
            AND bd.merchant_sale_price >= ali.min_price
            AND bd.merchant_sale_price < ali.max_price
            AND bd.partition_date = ali.partition_date
    LEFT JOIN tariffs_jl AS jtb
        ON
            bd.weight >= jtb.min_weight
            AND bd.weight < jtb.max_weight
            AND CARDINALITY(ARRAY_EXCEPT(bd.dangerous_kinds, jtb.dangerous_kinds)) = 0
            AND bd.country = jtb.country
            AND bd.merchant_sale_price >= jtb.min_price
            AND bd.merchant_sale_price < jtb.max_price
            AND bd.partition_date = jtb.partition_date
),

price_by_bucket AS (
    SELECT
        partition_date,
        is_last_week,
        bucket_id,
        country,
        MIN(bucket_gmv_initial) AS bucket_gmv_initial,
        MIN(ali_price) AS ali_price,
        MIN(ali_price) * MIN(bucket_jl_order_count) AS ali_price_bucket,
        MIN(jtb_price) AS jtb_price,
        MIN(jtb_price) * MIN(bucket_jl_order_count) AS jtb_price_bucket
    FROM add_channel_price
    WHERE
        jtb_row_num = 1
        OR ali_row_num = 1
    GROUP BY 1, 2, 3, 4
)

SELECT
    pr.partition_date,
    pr.is_last_week,
    pr.country,
    SUM(pr.jtb_price_bucket) AS jl_costs,
    SUM(pr.ali_price_bucket) AS ali_costs,
    SUM(pr.bucket_gmv_initial) AS gmv_initial,
    ROUND(SUM(pr.jtb_price_bucket) / SUM(pr.bucket_gmv_initial), 3) AS jl_costs_share,
    ROUND(SUM(pr.ali_price_bucket) / SUM(pr.bucket_gmv_initial), 3) AS ali_costs_share,
    ROUND(SUM(pr.jtb_price_bucket) / SUM(pr.ali_price_bucket), 3) AS jl_ali_costs_share
FROM price_by_bucket AS pr
WHERE
    pr.jtb_price IS NOT NULL
    AND pr.ali_price IS NOT NULL
GROUP BY 1, 2, 3
