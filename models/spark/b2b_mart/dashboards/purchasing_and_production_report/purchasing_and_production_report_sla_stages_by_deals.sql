{{ config(
    schema='b2b_mart',
    file_format='delta',
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    meta = {
      'model_owner' : '@abadoyan',
      'bigquery_load': 'true'
    }
) }}


WITH small_batch_deals_raw AS (
    SELECT
        *,
        COUNT(procurement_order_id) OVER (PARTITION BY deal_friendly_id, stage) AS po_in_deal
    FROM {{ ref('purchasing_and_production_report_sla_stages') }}
    WHERE is_small_batch = 1
),

small_batch_deals AS (
    SELECT
        deal_friendly_id,
        is_small_batch,
        '1.Confirmation' AS stage,
        'day' AS sla_granularity,
        4 AS sla_value,
        (
            COUNT(DISTINCT CASE WHEN stage = '0.Assigned' AND start_ts IS NOT NULL THEN procurement_order_id END) = MAX(po_in_deal)
        AND
            COUNT(DISTINCT CASE WHEN stage = '15.Waiting Payment' AND end_ts IS NOT NULL THEN procurement_order_id END) = MAX(po_in_deal)
        ) AS is_deal_ready,
        MIN(CASE WHEN stage = '0.Assigned' THEN start_ts END) AS start_ts,
        MAX(CASE WHEN stage = '15.Waiting Payment' THEN end_ts END) AS end_ts
    FROM small_batch_deals_raw
    WHERE stage IN (
        '0.Assigned',
        '15.Waiting Payment'
    )
    GROUP BY 1,2
    UNION ALL
    SELECT
        deal_friendly_id,
        is_small_batch,
        '2.China Operations' AS stage,
        'day' AS sla_granularity,
        14 AS sla_value,
        (
            COUNT(DISTINCT CASE WHEN stage = '2.Payment to Merchant' AND start_ts IS NOT NULL THEN procurement_order_id END) = MAX(po_in_deal)
        AND
            COUNT(DISTINCT CASE WHEN stage = '5.Ready for Shipment' AND end_ts IS NOT NULL THEN procurement_order_id END) = MAX(po_in_deal)
        ) AS is_deal_ready,
        MIN(CASE WHEN stage = '2.Payment to Merchant' THEN start_ts END) AS start_ts,
        MAX(CASE WHEN stage = '5.Ready for Shipment' THEN end_ts END) AS end_ts
    FROM small_batch_deals_raw
    WHERE stage IN (
        '2.Payment to Merchant',
        '5.Ready for Shipment'
    )
    GROUP BY 1,2
    UNION ALL
    SELECT
        deal_friendly_id,
        is_small_batch,
        '3.Shipped' AS stage,
        'day' AS sla_granularity,
        7 AS sla_value,
        (
            COUNT(DISTINCT CASE WHEN stage = '5.Ready for Shipment' AND start_ts IS NOT NULL THEN procurement_order_id END) = MAX(po_in_deal)
        AND
            COUNT(DISTINCT CASE WHEN stage = '6.Shipped' AND end_ts IS NOT NULL THEN procurement_order_id END) = MAX(po_in_deal)
        ) AS is_deal_ready,
        MIN(CASE WHEN stage = '5.Ready for Shipment' THEN start_ts END) AS start_ts,
        MAX(CASE WHEN stage = '6.Shipped' THEN end_ts END) AS end_ts
    FROM small_batch_deals_raw
    WHERE is_small_batch = 1
      AND stage IN (
        '5.Ready for Shipment',
        '6.Shipped'
    )
    GROUP BY 1,2
),

main AS (
    SELECT
        deal_friendly_id,
        is_small_batch,
        stage,
        sla_granularity,
        sla_value,
        is_deal_ready,
        start_ts,
        CASE WHEN is_deal_ready THEN end_ts END AS end_ts
    FROM small_batch_deals
),

calendar_hours AS (
    SELECT
        sequence.ts AS hour_ts,
        dayofweek(sequence.ts) AS dow,
        CASE WHEN dayofweek(sequence.ts) IN (1, 7) THEN 1 ELSE 0 END AS is_weekend
    FROM (
        SELECT explode(
            sequence(
                to_timestamp('2023-01-01 00:00:00'),
                to_timestamp('2026-12-31 23:00:00'),
                interval 1 hour
            )
        ) AS ts
    ) AS sequence
),

weekend_hours AS (
    SELECT
        deal_friendly_id,
        stage,
        COUNT(*) AS weekend_hours
    FROM main AS ao
    JOIN calendar_hours AS ch
      ON ch.hour_ts >= date_trunc('hour', ao.start_ts) + INTERVAL 5 HOURS
     AND ch.hour_ts <  date_trunc('hour', ao.end_ts)   + INTERVAL 5 HOURS
     AND ch.is_weekend = 1
    GROUP BY 1,2
)


SELECT
    m.deal_friendly_id,
    is_small_batch,
    m.stage,
    sla_granularity,
    sla_value,
    start_ts,
    end_ts,
    (unix_timestamp(m.end_ts) - unix_timestamp(m.start_ts)) / 60 / 60 / 24 AS fact_value_with_weekends,
    CASE
        WHEN m.start_ts IS NOT NULL AND m.end_ts IS NOT NULL THEN GREATEST((
            (unix_timestamp(m.end_ts) - unix_timestamp(m.start_ts)) / 60 / 60 - COALESCE(wh.weekend_hours, 0)
        ) / 24, 0)
    END AS fact_value_without_weekends
FROM main AS m
LEFT JOIN weekend_hours AS wh
    ON m.deal_friendly_id = wh.deal_friendly_id
    AND m.stage = wh.stage
