{{
  config(
    meta = {
      'model_owner' : '@e.kotsegubov'
    },
    materialized='table',
    schema='jl_models',
  )
}}

WITH weeks AS (
    SELECT DISTINCT year_week AS partition_date
    FROM {{ source ('mart', 'dim_date') }}
    WHERE
        1 = 1
        AND id >= DATE '2023-01-01'
        AND id < CURRENT_DATE
)

SELECT
    w.partition_date,
    t.country,
    t.shipping_type,
    t.channel_id,
    t.min_weight,
    t.max_weight,
    t.dangerous_kinds,
    ROUND(COALESCE(t.pickup_per_item, 0) + COALESCE(t.firstmile_per_item, 0) + COALESCE(t.linehaul_per_item, 0) + COALESCE(t.lastmile_per_item, 0) + COALESCE(t.customs_per_item, 0), 2) AS per_item,
    ROUND(COALESCE(t.pickup_per_kg, 0) + COALESCE(t.firstmile_per_kg, 0) + COALESCE(t.linehaul_per_kg, 0) + COALESCE(t.lastmile_per_kg, 0) + COALESCE(t.customs_per_kg, 0), 2) AS per_kg
FROM {{ source ('logistics_mart', 'tariffs') }} AS t
INNER JOIN weeks AS w
    ON t.effective_date = w.partition_date
WHERE
    t.origin_country = 'CN'
    AND t.effective_start_time <= w.partition_date
    AND t.effective_end_time > w.partition_date
    AND t.is_channel_active
