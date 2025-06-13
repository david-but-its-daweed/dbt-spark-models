{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'model_owner' : '@abadoyan',
      'bigquery_load': 'true'
    }
) }}


WITH deals AS (
    SELECT
        deal_id,
        issue_friendly_id AS deal_friendly_id,
        owner_email AS deal_assignee_email,
        CONCAT(
            '<a target="_blank" href="https://admin.joompro.io/users/',
            user_id,
            '/deal/',
            deal_id,
            '">',
            issue_friendly_id,
            '</a>'
        ) AS deal_link
    FROM {{ ref('fact_deals') }}
    WHERE next_effective_ts_msk IS NULL

),

main AS (
    SELECT
        pp.*,

        CONCAT(
            '<a target="_blank" href="https://admin.joompro.io/procurementOrders/',
            pp.procurement_order_id,
            '">',
            pp.procurement_order_friendly_id,
            '</a>'
        ) AS procurement_order_link,

        d.deal_assignee_email,
        d.deal_friendly_id,
        d.deal_link,

        sub_status_packing_and_labeling_ts AS psi_results_accepted_small_batch,

        CASE
            WHEN sub_status_psi_problems_are_to_be_fixed_ts IS NOT NULL THEN 1
            ELSE 0
        END AS is_psi_with_problems_small_batch,

        COALESCE(manufacturing_days, manufacturing_days_from_merchant_order) AS manufacturing_days_plan,

        DATE_ADD(sub_status_manufacturing_ts, INT(COALESCE(manufacturing_days, manufacturing_days_from_merchant_order))) AS production_deadline_to_plan,

        CASE
            WHEN COALESCE(psi_being_conducted_ts, CASE WHEN current_sub_status != 'cancelled' THEN CURRENT_TIMESTAMP() END)
                 > DATE_ADD(sub_status_manufacturing_ts, INT(COALESCE(manufacturing_days, manufacturing_days_from_merchant_order)))
            THEN 1
            ELSE 0
        END AS is_production_deadline_missed,

        DATEDIFF(
            COALESCE(psi_being_conducted_ts, CASE WHEN current_sub_status != 'cancelled' THEN CURRENT_TIMESTAMP() END),
            DATE_ADD(sub_status_manufacturing_ts, INT(COALESCE(manufacturing_days, manufacturing_days_from_merchant_order)))
        ) AS production_delay_days,

        COUNT(CASE WHEN current_status != 'cancelled' THEN procurement_order_id END) OVER (PARTITION BY pp.deal_id) AS orders_in_deal,

        COUNT(CASE WHEN current_status != 'cancelled' AND sub_status_ready_for_shipment_ts IS NOT NULL THEN procurement_order_id END)
            OVER (PARTITION BY pp.deal_id) AS orders_ready_for_shipment_in_deal,

        MIN(sub_status_ready_for_shipment_ts) OVER (PARTITION BY pp.deal_id) AS first_ready_for_shipment_in_deal_ts,

        MIN(sub_status_shipped_by_3pl_ts) OVER (PARTITION BY pp.deal_id) AS first_shipped_in_deal_ts

    FROM {{ ref('procurement_orders') }} AS pp
    LEFT JOIN deals AS d ON pp.deal_id = d.deal_id
    WHERE is_for_purchasing_and_production_report = 1

),

main_with_last AS (
    SELECT
        *,
        MAX(
            CASE
                WHEN orders_ready_for_shipment_in_deal = orders_in_deal THEN sub_status_ready_for_shipment_ts
            END
        ) OVER (PARTITION BY deal_id) AS last_ready_for_shipment_in_deal_ts
    FROM main
),

gmv AS (
    SELECT
        procurement_order_id,
        total_price_usd_raw AS gmv_usd
    FROM {{ ref('purchasing_and_production_report_boxes') }}
)


SELECT
    main.*,
    gmv.gmv_usd,
    (UNIX_TIMESTAMP(sub_status_ready_for_shipment_ts) - UNIX_TIMESTAMP(first_ready_for_shipment_in_deal_ts)) / 3600 / 24
        AS waiting_ready_for_shipment_from_first_order_in_deal,
    (UNIX_TIMESTAMP(last_ready_for_shipment_in_deal_ts) - UNIX_TIMESTAMP(first_ready_for_shipment_in_deal_ts)) / 3600 / 24
        AS waiting_last_ready_for_shipment_from_first_order_in_deal,
    (UNIX_TIMESTAMP(first_shipped_in_deal_ts) - UNIX_TIMESTAMP(last_ready_for_shipment_in_deal_ts)) / 3600 / 24
        AS waiting_shipped_from_last_ready_for_shipment_in_deal
FROM main_with_last AS main
LEFT JOIN gmv ON main.procurement_order_id = gmv.procurement_order_id
