{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'model_owner' : '@abadoyan',
      'bigquery_load': 'true'
    }
) }}


WITH big_batch_raw AS (
    SELECT *
    FROM {{ ref('purchasing_and_production_report') }}
    WHERE is_small_batch = 0
      AND current_status != 'cancelled'
),
     small_batch_raw AS (
    SELECT *
    FROM {{ ref('purchasing_and_production_report') }}
    WHERE is_small_batch = 1
      AND current_status != 'cancelled'
),
     big_batch AS (
    SELECT
        deal_friendly_id,
        is_small_batch,
        procurement_order_id,
        created_ts,
        '1.Queue' AS stage,
        'day' AS sla_granularity,
        1 AS sla_value,
        sub_status_forming_order_unassigned_ts AS start_ts,
        sub_status_filling_in_information_ts AS end_ts
    FROM big_batch_raw

    UNION ALL

    SELECT
        deal_friendly_id,
        is_small_batch,
        procurement_order_id,
        created_ts,
        '2.Confirmation' AS stage,
        'day' AS sla_granularity,
        1 AS sla_value,
        sub_status_filling_in_information_ts AS start_ts,
        sub_status_preparing_order_ts AS end_ts
    FROM big_batch_raw

    UNION ALL

    SELECT
        deal_friendly_id,
        is_small_batch,
        procurement_order_id,
        created_ts,
        '3.First Payment' AS stage,
        'day' AS sla_granularity,
        4 AS sla_value,
        sub_status_client_payment_received_ts AS start_ts,
        sub_status_manufacturing_ts AS end_ts
    FROM big_batch_raw

    UNION ALL

    SELECT
        deal_friendly_id,
        is_small_batch,
        procurement_order_id,
        created_ts,
        '4.Manufacturing' AS stage,
        'day' AS sla_granularity,
        COALESCE(manufacturing_days, manufacturing_days_from_merchant_order) AS sla_value,
        sub_status_manufacturing_ts AS start_ts,
        COALESCE(psi_being_conducted_ts, sub_status_psi_being_conducted_ts) AS end_ts
    FROM big_batch_raw

    UNION ALL

    SELECT
        deal_friendly_id,
        is_small_batch,
        procurement_order_id,
        created_ts,
        '5.PSI Execution' AS stage,
        'day' AS sla_granularity,
        3 AS sla_value,
        COALESCE(psi_being_conducted_ts, sub_status_psi_being_conducted_ts) AS start_ts,
        COALESCE(psi_waiting_for_confirmation_ts, sub_status_psi_waiting_for_confirmation_ts) AS end_ts
    FROM big_batch_raw

    UNION ALL

    SELECT
        deal_friendly_id,
        is_small_batch,
        procurement_order_id,
        created_ts,
        '6.PSI Confirmation' AS stage,
        'day' AS sla_granularity,
        3 AS sla_value,
        COALESCE(psi_waiting_for_confirmation_ts, sub_status_psi_waiting_for_confirmation_ts) AS start_ts,
        COALESCE(psi_problems_are_to_be_fixed_ts, sub_status_psi_problems_are_to_be_fixed_ts, psi_results_accepted_ts, sub_status_psi_results_accepted_ts) AS end_ts
    FROM big_batch_raw

    UNION ALL

    SELECT
        deal_friendly_id,
        is_small_batch,
        procurement_order_id,
        created_ts,
        '7.Final Payment' AS stage,
        'day' AS sla_granularity,
        4 AS sla_value,
        coalesce(psi_results_accepted_ts, sub_status_psi_results_accepted_ts) AS start_ts,
        sub_status_final_payment_acquired_ts AS end_ts
    FROM big_batch_raw
),
     small_batch AS (
    SELECT
        deal_friendly_id,
        is_small_batch,
        procurement_order_id,
        created_ts,
        '0.Assigned' AS stage,
        'day' AS sla_granularity,
        null AS sla_value,
        sub_status_forming_order_unassigned_ts AS start_ts,
        sub_status_filling_in_information_ts AS end_ts
    FROM small_batch_raw

    UNION ALL

    SELECT
        deal_friendly_id,
        is_small_batch,
        procurement_order_id,
        created_ts,
        '05.Confirmed by Procurement' AS stage,
        'day' AS sla_granularity,
        null AS sla_value,
        sub_status_filling_in_information_ts AS start_ts,
        sub_status_confirmed_by_procurement_ts AS end_ts
    FROM small_batch_raw

    UNION ALL

    SELECT
        deal_friendly_id,
        is_small_batch,
        procurement_order_id,
        created_ts,
        '1.Confirmation' AS stage,
        'day' AS sla_granularity,
        1 AS sla_value,
        sub_status_forming_order_unassigned_ts AS start_ts,
        sub_status_confirmed_by_procurement_ts AS end_ts
    FROM small_batch_raw

    UNION ALL

    SELECT
        deal_friendly_id,
        is_small_batch,
        procurement_order_id,
        created_ts,
        '15.Waiting Payment' AS stage,
        'day' AS sla_granularity,
        null AS sla_value,
        sub_status_confirmed_by_procurement_ts AS start_ts,
        sub_status_waiting_for_payment_ts AS end_ts
    FROM small_batch_raw

    UNION ALL

    SELECT
        deal_friendly_id,
        is_small_batch,
        procurement_order_id,
        created_ts,
        '2.Payment to Merchant' AS stage,
        'day' AS sla_granularity,
        1 AS sla_value,
        sub_status_waiting_for_payment_ts AS start_ts,
        sub_status_merchant_preparing_order_ts AS end_ts
    FROM small_batch_raw

    UNION ALL

    SELECT
        deal_friendly_id,
        is_small_batch,
        procurement_order_id,
        created_ts,
        '3.Merchant Shipped' AS stage,
        'day' AS sla_granularity,
        1 AS sla_value,
        sub_status_merchant_preparing_order_ts AS start_ts,
        sub_status_merchant_shipped_the_goods_ts AS end_ts
    FROM small_batch_raw

    UNION ALL

    SELECT
        deal_friendly_id,
        is_small_batch,
        procurement_order_id,
        created_ts,
        '4.Warehouse Received' AS stage,
        'day' AS sla_granularity,
        3 AS sla_value,
        sub_status_merchant_shipped_the_goods_ts AS start_ts,
        sub_status_psi_being_conducted_ts AS end_ts
    FROM small_batch_raw

    UNION ALL

    SELECT
        deal_friendly_id,
        is_small_batch,
        procurement_order_id,
        created_ts,
        '5.Ready for Shipment' AS stage,
        'day' AS sla_granularity,
        /*
        Если были проблемы с PSI, то на решение проблем дается 7 дней.
        Если проблем не было, то SLA на PSI 1 день.
        */
        CASE
            WHEN sub_status_psi_problems_are_to_be_fixed_ts IS NOT NULL THEN 7
            ELSE 1
        END AS sla_value,
        sub_status_psi_being_conducted_ts AS start_ts,
        sub_status_ready_for_shipment_ts AS end_ts
    FROM small_batch_raw

    UNION ALL

    SELECT
        deal_friendly_id,
        is_small_batch,
        procurement_order_id,
        created_ts,
        '6.Shipped' AS stage,
        'day' AS sla_granularity,
        1 AS sla_value,
        sub_status_ready_for_shipment_ts AS start_ts,
        sub_status_shipped_by_3pl_ts AS end_ts
    FROM small_batch_raw
),
     all_orders AS (
    SELECT *
    FROM big_batch
    UNION ALL
    SELECT *
    FROM small_batch
),
     total_stage AS (
    SELECT
        deal_friendly_id,
        is_small_batch,
        procurement_order_id,
        created_ts,
        'Total Production' AS stage,
        'day' AS sla_granularity,
        SUM(sla_value) AS sla_value,
        MIN(
            CASE
                WHEN is_small_batch = 0 AND stage = '3.First Payment' THEN start_ts
                WHEN is_small_batch = 1 AND stage = '2.Payment to Merchant' THEN start_ts
            END
        ) AS start_ts,
        MAX(
            CASE
                WHEN is_small_batch = 0 AND stage = '7.Final Payment' THEN end_ts
                WHEN is_small_batch = 1 AND stage = '5.Ready for Shipment' THEN end_ts
            END
        ) AS end_ts
    FROM all_orders
    WHERE
        CASE
            WHEN is_small_batch = 0 THEN stage IN (
                '3.First Payment',
                '4.Manufacturing',
                '5.PSI Execution',
                '6.PSI Confirmation',
                '7.Final Payment'
            )
            WHEN is_small_batch = 1 THEN stage IN (
                '2.Payment to Merchant',
                '3.Merchant Shipped',
                '4.Warehouse Received',
                '5.Ready for Shipment'
            )
            ELSE FALSE
        END
    GROUP BY 1,2,3,4
),
     calendar_hours AS (
    SELECT
        sequence.ts AS hour_ts,
        dayofweek(sequence.ts) AS dow,
        CASE WHEN dayofweek(sequence.ts) IN (1, 7) THEN 1 ELSE 0 END AS is_weekend
    FROM (
        SELECT explode(sequence(to_timestamp('2023-01-01 00:00:00'), to_timestamp('2026-12-31 23:59:59'), interval 1 hour)) AS ts
    ) AS sequence
),
     main AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY procurement_order_id
            ORDER BY GREATEST(
                COALESCE(start_ts, to_timestamp('0001-01-01')),
                COALESCE(end_ts, to_timestamp('0001-01-01'))
            ) DESC, stage DESC
        ) AS rn
    FROM all_orders
    UNION ALL
    SELECT
        *,
        NULL AS rn
    FROM total_stage
),
     weekend_hours AS (
    SELECT
        procurement_order_id,
        stage,
        COUNT(*) AS weekend_hours
    FROM main AS ao
    JOIN calendar_hours AS ch
      ON ch.hour_ts >= date_trunc('hour', ao.start_ts) + INTERVAL 5 HOURS
     AND ch.hour_ts < date_trunc('hour', ao.end_ts) + INTERVAL 5 HOURS
     AND ch.is_weekend = 1
    GROUP BY 1,2
)


SELECT
    m.deal_friendly_id,
    m.is_small_batch,
    m.procurement_order_id,
    m.stage,
    m.sla_granularity,
    m.sla_value,
    m.start_ts,
    m.end_ts,
    (unix_timestamp(m.end_ts) - unix_timestamp(m.start_ts)) / 60 / 60 / 24 AS fact_value_with_weekends,
    GREATEST(
        CASE
            WHEN m.start_ts IS NOT NULL AND m.end_ts IS NOT NULL THEN (
                (unix_timestamp(m.end_ts) - unix_timestamp(m.start_ts)) / 60 / 60 - COALESCE(wh.weekend_hours, 0)
            ) / 24
        END,
    0) AS fact_value_without_weekends,
    CASE WHEN m.rn = 1 AND m.end_ts IS NULL THEN 1 ELSE 0 END AS is_current_stage,
    COALESCE(
        MAX(CASE WHEN m.rn = 1 AND m.end_ts IS NULL THEN m.stage END) OVER (PARTITION BY m.procurement_order_id),
        'No active stage'
    ) AS current_stage,
    CASE
        WHEN m.sla_value IS NOT NULL AND (m.start_ts IS NULL OR m.end_ts IS NULL)
         AND (
            FIRST_VALUE(CASE WHEN m.stage != 'Total Production' THEN m.start_ts END) OVER (
                PARTITION BY m.procurement_order_id
                ORDER BY m.stage
                ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
            ) IS NOT NULL
            OR
            FIRST_VALUE(CASE WHEN m.stage != 'Total Production' THEN m.end_ts END) OVER (
                PARTITION BY m.procurement_order_id
                ORDER BY m.stage
                ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
            ) IS NOT NULL
         ) THEN 1
        WHEN m.end_ts IS NOT NULL AND m.start_ts IS NULL THEN 1
        ELSE 0
    END AS is_stage_skipped,
    MAX(CASE WHEN m.stage = 'Total Production' AND m.start_ts IS NOT NULL AND m.end_ts IS NULL THEN 1 ELSE 0 END)
        OVER (PARTITION BY m.procurement_order_id) AS is_order_in_production
FROM main AS m
LEFT JOIN weekend_hours AS wh
    ON m.procurement_order_id = wh.procurement_order_id
    AND m.stage = wh.stage
ORDER BY procurement_order_id, stage
