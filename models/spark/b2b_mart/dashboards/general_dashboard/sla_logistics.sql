{{ config(
    schema='b2b_mart',
    file_format='delta',
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    meta = {
      'model_owner' : '@tigran',
      'bigquery_load': 'true'
    }
) }}

WITH deals AS (
    SELECT
        deal_id,
        country,
        procurement_order_id,
        is_small_batch,
        created_ts,
        CASE
            WHEN is_small_batch = 1 THEN sub_status_confirmed_by_procurement_ts
            WHEN is_small_batch = 0 THEN sub_status_preparing_order_ts
        END AS confirmed_ts,
        CASE
            WHEN is_small_batch = 1 THEN sub_status_ready_for_shipment_ts
            WHEN is_small_batch = 0 THEN sub_status_final_payment_acquired_ts
        END AS ready_to_ship_ts,
        CASE
            WHEN is_small_batch = 1 THEN sub_status_shipped_by_3pl_ts
            WHEN is_small_batch = 0 THEN sub_status_pick_up_payment_picked_up_ts
        END AS outbound_ts
    FROM {{ ref('purchasing_and_production_report') }}
    WHERE current_status <> 'cancelled'
),

deals_with_metrics AS (
    SELECT
        deal_id,
        is_small_batch,
        country,
        DATE(MIN(created_ts)) AS order_created_date,
        COUNT(DISTINCT procurement_order_id) AS num_of_orders_in_deal,
        SUM(CASE WHEN confirmed_ts IS NOT NULL THEN 1 ELSE 0 END) AS num_of_confirmed_in_deal,
        SUM(CASE WHEN ready_to_ship_ts IS NOT NULL THEN 1 ELSE 0 END) AS num_of_ready_to_ship_in_deal,
        SUM(CASE WHEN outbound_ts IS NOT NULL THEN 1 ELSE 0 END) AS num_of_outbound_in_deal,
        MIN(created_ts) AS order_placed_ts,
        MAX(confirmed_ts) AS confirmed_ts,
        MAX(ready_to_ship_ts) AS ready_to_ship_ts,
        MAX(outbound_ts) AS outbound_ts,
        FLOOR((UNIX_TIMESTAMP(MAX(confirmed_ts)) - UNIX_TIMESTAMP(MIN(created_ts))) / 3600) / 24 AS datediff_created_confirmed,
        FLOOR((UNIX_TIMESTAMP(MAX(ready_to_ship_ts)) - UNIX_TIMESTAMP(MAX(confirmed_ts))) / 3600) / 24 AS datediff_confirmed_ready_to_ship,
        FLOOR((UNIX_TIMESTAMP(MAX(outbound_ts)) - UNIX_TIMESTAMP(MIN(created_ts))) / 3600) / 24 AS datediff_created_outbound,
        FLOOR((UNIX_TIMESTAMP(MAX(outbound_ts)) - UNIX_TIMESTAMP(MAX(ready_to_ship_ts))) / 3600) / 24 AS datediff_ready_to_ship_outbound
    FROM deals
    GROUP BY 1, 2, 3
),

union_metrics AS (
    SELECT
        order_created_date,
        is_small_batch,
        country,
        'placed - confirmed' AS metric_name,
        datediff_created_confirmed AS metric
    FROM deals_with_metrics
    WHERE num_of_orders_in_deal = num_of_confirmed_in_deal
    UNION ALL
    SELECT
        order_created_date,
        is_small_batch,
        country,
        'confirmed - ready to ship' AS metric_name,
        datediff_confirmed_ready_to_ship AS metric
    FROM deals_with_metrics
    WHERE num_of_orders_in_deal = num_of_confirmed_in_deal AND num_of_confirmed_in_deal = num_of_ready_to_ship_in_deal
    UNION ALL
    SELECT
        order_created_date,
        is_small_batch,
        country,
        'ready to ship - outbound' AS metric_name,
        datediff_ready_to_ship_outbound AS metric
    FROM deals_with_metrics
    WHERE num_of_orders_in_deal = num_of_confirmed_in_deal AND num_of_confirmed_in_deal = num_of_ready_to_ship_in_deal AND num_of_ready_to_ship_in_deal = num_of_outbound_in_deal
    UNION ALL
    SELECT
        order_created_date,
        is_small_batch,
        country,
        'placed - outbound' AS metric_name,
        datediff_created_outbound AS metric
    FROM deals_with_metrics
    WHERE num_of_orders_in_deal = num_of_confirmed_in_deal AND num_of_confirmed_in_deal = num_of_ready_to_ship_in_deal AND num_of_ready_to_ship_in_deal = num_of_outbound_in_deal
)

SELECT DISTINCT
    order_created_date,
    is_small_batch,
    country,
    metric_name,
    PERCENTILE_APPROX(metric, 0.5) OVER (all_dimensions) AS perc_50,
    PERCENTILE_APPROX(metric, 0.8) OVER (all_dimensions) AS perc_80,
    PERCENTILE_APPROX(metric, 0.95) OVER (all_dimensions) AS perc_95
FROM union_metrics
WINDOW all_dimensions AS (PARTITION BY order_created_date, metric_name, country, is_small_batch)
