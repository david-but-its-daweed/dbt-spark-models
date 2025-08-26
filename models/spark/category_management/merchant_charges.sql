{{
  config(
    materialized='incremental',
    alias='merchant_charges',
    file_format='delta',
    schema='category_management',
    incremental_strategy='insert_overwrite',
    partition_by=['partition_date'],
    meta = {
      'model_owner' : '@catman-analytics.duty',
      'bigquery_load': 'true'
    },
    on_schema_change='append_new_columns'
  )
}}

WITH quality_orders AS (
    SELECT order_id
    FROM {{ ref('gold_orders') }} --gold.orders 
    WHERE
        1 = 1
        AND (refund_reason = 'quality' OR is_quality_refund)
        AND DATE(order_datetime_utc) > '2025-01-01'
),

data_1 AS (
    SELECT
        DATE(inv.ct) AS partition_date,
        `inv`.`_id` AS invoice_id,
        `inv`.`ct` AS created_at,
        `inv`.`to`.`id` AS inv_to_id,
        `inv`.`fr`.`id` AS inv_fr_id,
        inv.t,
        `inv`.`s`.`tp`.`amount` AS inv_s_tp_amount,
        `inv`.`s`.`tp`.`ccy` AS inv_s_tp_ccy,
        ELEMENT_AT(inv.sh, 1) AS inv_sh_0, --достаем нулевой элемент аналогично inv.sh[0]
        `inv`.`p`.`b` AS inv_p_b,
        `inv`.`p`.`e` AS inv_p_e,
        inv.ct,
        inv.baseId
    FROM {{ source('mongo','finance_invoices_v3_daily_snapshot') }} AS inv --mongo.finance_invoices_v3_daily_snapshot AS inv
    WHERE
        1 = 1
        AND (
            inv.t = 58 -- joomSIAJoomMerchantOrderCancellationCharge, iprojJoomMerchantOrderCancellationCharge
            OR
            inv.t = 2 --  // счет Joom мерчантам по рефандам
        )
        AND DATE(inv.ct) >= DATE('2025-01-01')
)

SELECT DISTINCT
    inv.partition_date,
    inv.invoice_id,
    inv.created_at,
    CASE
        WHEN inv.t = 2 THEN inv.inv_to_id
        WHEN inv.t = 58 THEN inv.inv_fr_id
    END AS merchant_id_corrected,
    CASE
        WHEN inv.t = 2 THEN inv.inv_fr_id
        WHEN inv.t = 58 THEN inv.inv_to_id
    END AS beneficiary_corrected,
    CASE
        WHEN inv.t = 2 THEN inv.inv_s_tp_amount / 1000000.0 * (-1)
        WHEN inv.t = 58 THEN inv.inv_s_tp_amount / 1000000.0
    END AS invoice_amount_corrected,
    inv.inv_s_tp_ccy AS invoice_ccy,
    inv.t AS inv_type,
    CASE
        WHEN inv.t = 2 THEN 'quality refund'
        WHEN inv.t = 58 THEN 'merchant cancellation'
    END AS inv_type_text,
    CASE
        WHEN inv.inv_sh_0.s = 1 THEN 'Created'
        WHEN inv.inv_sh_0.s = 2 THEN 'Unnumbered'
        WHEN inv.inv_sh_0.s = 3 THEN 'Ready'
        WHEN inv.inv_sh_0.s = 4 THEN 'PayoutASsigned'
        WHEN inv.inv_sh_0.s = 5 THEN 'Paid'
        WHEN inv.inv_sh_0.s = 6 THEN 'PayoutNotExpected'
    END AS invoice_status,
    TO_DATE(inv.inv_p_b, 'yyyyMMdd') AS period_begin_dt,
    TO_DATE(inv.inv_p_e, 'yyyyMMdd') AS period_end_dt,
    CASE
        WHEN inv.t = 2 THEN inv.inv_s_tp_amount / 1000000.0 * (-1)
        WHEN inv.t = 58 THEN inv.inv_s_tp_amount / 1000000.0
    END * cr.rate AS invoice_amount_usd
FROM data_1 AS inv
LEFT JOIN {{ ref('dim_pair_currency_rate') }} AS cr --models.dim_pair_currency_rate  AS cr
    ON
        1 = 1
        AND inv.inv_s_tp_ccy = cr.currency_code
        AND DATE(inv.ct) = cr.effective_date
        AND cr.currency_code_to = 'USD'
LEFT JOIN {{ source('mongo','cashflow_invoices_daily_snapshot') }} AS cashflowInv --mongo.cashflow_invoices_daily_snapshot AS cashflowInv 
    ON
        1 = 1
        AND (
            CASE
                WHEN inv.baseId IS NOT NULL AND inv.baseId != '' THEN inv.baseId
                ELSE inv.invoice_id
            END
        ) = cashflowInv.clientInvoiceId
LEFT JOIN {{ source('mongo','cashflow_daily_operations_daily_snapshot') }} AS dailyOps -- mongo.cashflow_daily_operations_daily_snapshot as dailyOps 
    ON
        1 = 1
        AND cashflowInv._id = dailyOps.invoice
LEFT JOIN {{ source('mongo','cashflow_operations_daily_snapshot') }} AS ops --mongo.cashflow_operations_daily_snapshot as ops 
    ON
        1 = 1
        AND dailyOps._id = ops.daily
WHERE
    1 = 1
    AND (
        (inv.t = 2 AND ops.ref.id IN (SELECT qo.order_id FROM quality_orders AS qo))
        OR
        inv.t = 58
    )