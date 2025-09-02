{{
  config(
    file_format='delta',
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by=['order_date_utc'],
    on_schema_change='sync_all_columns',
    meta = {
        'model_owner' : '@leonid.enov',
        'bigquery_load': 'true',
        'bigquery_partitioning_date_column': 'order_date_utc',
        'bigquery_overwrite': 'true'
    }
  )
}}

WITH currency_rate AS (
    SELECT
        currency_code,
        rate,
        effective_date,
        next_effective_date
    FROM {{ source('mart','dim_pair_currency_rate') }}
    WHERE currency_code_to = 'USD'
),

merchant_data AS (
    SELECT
        a.order_date_utc,
        a.marketplace_name,
        SUM(a.merchant_revenue * cr.rate) AS merchant_revenue_initial,
        SUM(IF(a.order_status NOT IN ('cancelledByJL', 'cancelledByMerchant', 'refunded'), a.merchant_revenue * cr.rate, NULL)) AS merchant_revenue_final
    FROM {{ ref('jms_orders') }} AS a
    LEFT JOIN currency_rate AS cr
        ON
            a.merchant_price_currency = cr.currency_code
            AND a.order_date_utc > cr.effective_date
            AND a.order_date_utc <= cr.next_effective_date
    GROUP BY 1, 2
),

gmv_and_logistics AS (
    SELECT
        a.order_date_utc,
        a.marketplace_name,
        -- GMV with and without VAT, refunds
        SUM(a.gmv_initial) AS gmv_w_vat,
        SUM(a.gmv_wo_vat_usd) AS gmv_wo_vat,
        SUM(a.customer_vat_initial_usd) AS vat,
        SUM(IF(a.detailed_refund_reason IS NOT NULL, a.gmv_initial, 0)) AS gmv_refunded_w_vat,
        SUM(IF(a.detailed_refund_reason IS NOT NULL, a.gmv_wo_vat_usd, 0)) AS gmv_refunded_wo_vat,
        SUM(a.gmv_wo_vat_usd) - SUM(IF(a.detailed_refund_reason IS NOT NULL, a.gmv_wo_vat_usd, 0)) AS gmv_final, -- Check if wo_vat is OK

        --Logistics costs
        SUM(l.jms_logistics_revenue_initial) AS jms_logistics_revenue_initial,
        SUM(l.jms_logistics_revenue_final) AS jms_logistics_revenue_final,
        SUM(l.jl_cost) AS jl_cost_logistics,
        -- комиссии
        SUM(
            IF(
                a.detailed_refund_reason IS NULL AND (a.order_status IN ('fulfilledOnline', 'shipped')), a.take_rate_jms_commission_share * a.gmv_wo_vat_usd, 0
            )
        ) AS money_take_rate_jms,
        SUM(
            IF(
                a.detailed_refund_reason IS NULL AND (a.order_status IN ('fulfilledOnline', 'shipped')), a.take_rate_partner_share * a.gmv_wo_vat_usd, 0
            )
        ) AS money_take_rate_partner
    FROM {{ ref('jms_orders') }} AS a
    LEFT JOIN {{ ref('source_jms_pnl_logistics') }} AS l ON a.friendly_order_id = l.friendly_order_id
    GROUP BY 1, 2
),

final_stats AS (
    SELECT
        a.order_date_utc,
        a.marketplace_name,
        SUM(a.gmv_w_vat) AS gmv_w_vat,
        SUM(a.gmv_wo_vat) AS gmv_wo_vat,
        SUM(a.vat) AS vat,
        SUM(a.gmv_w_vat) - SUM(a.gmv_wo_vat) - SUM(a.vat) AS marketplace_commission,
        SUM(a.gmv_refunded_w_vat) AS gmv_refunded_w_vat,
        SUM(a.gmv_refunded_wo_vat) AS gmv_refunded_wo_vat,
        SUM(a.gmv_final) AS gmv_final,
        SUM(b.merchant_revenue_initial) AS merchant_revenue_initial,
        SUM(b.merchant_revenue_final) AS merchant_revenue_final,
        SUM(a.jl_cost_logistics) AS jl_cost_logistics,
        SUM(a.jms_logistics_revenue_initial) AS jms_logistics_revenue_initial,
        SUM(a.jms_logistics_revenue_final) AS jms_logistics_revenue_final,
        SUM(a.money_take_rate_jms) AS money_take_rate_jms,
        SUM(a.money_take_rate_partner) AS money_take_rate_partner,
        SUM(a.gmv_final) - SUM(b.merchant_revenue_final) - SUM(a.jms_logistics_revenue_final) AS commission_revenue,
        SUM(a.gmv_final) - SUM(b.merchant_revenue_final) - SUM(a.jms_logistics_revenue_final) + SUM(a.jms_logistics_revenue_final) AS total_jms_revenue,
        SUM(a.money_take_rate_partner) + SUM(a.jl_cost_logistics) AS total_costs_commission_revenue,
        ((SUM(a.gmv_final) - SUM(b.merchant_revenue_final) - SUM(a.jms_logistics_revenue_final)) + SUM(a.jms_logistics_revenue_final))
        - (SUM(a.money_take_rate_partner) + SUM(a.jl_cost_logistics)) AS gross_profit
    FROM gmv_and_logistics AS a
    LEFT JOIN merchant_data AS b USING (order_date_utc, marketplace_name)
    WHERE a.order_date_utc >= '2025-01-01'
    GROUP BY 1, 2
)

SELECT *
FROM final_stats