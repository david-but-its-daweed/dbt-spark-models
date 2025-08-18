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

SELECT
    DATE(inv.ct) AS partition_date,
    inv._id AS invoice_id,
    inv.ct AS created_at,
    CASE
        WHEN inv.t = 2 THEN `inv.to.id`
        WHEN inv.t = 58 THEN `inv.fr.id`
    END AS merchant_id_corrected,
    CASE
        WHEN inv.t = 2 THEN `inv.fr.id`
        WHEN inv.t = 58 THEN `inv.to.id`
    END AS beneficiary_corrected,
    CASE
        WHEN inv.t = 2 THEN inv.s.tp.amount / 1000000.0 * (-1)
        WHEN inv.t = 58 THEN inv.s.tp.amount / 1000000.0
    END AS invoice_amount_corrected,
    inv.s.tp.ccy AS invoice_ccy,
    inv.t AS inv_type,
    CASE
        WHEN inv.t = 2 THEN 'quality refund'
        WHEN inv.t = 58 THEN 'merchant cancellation'
    END AS inv_type_text,
    CASE
        WHEN `inv.sh[0].s` = 1 THEN 'Created'
        WHEN `inv.sh[0].s` = 2 THEN 'Unnumbered'
        WHEN `inv.sh[0].s` = 3 THEN 'Ready'
        WHEN `inv.sh[0].s` = 4 THEN 'PayoutASsigned'
        WHEN `inv.sh[0].s` = 5 THEN 'Paid'
        WHEN `inv.sh[0].s` = 6 THEN 'PayoutNotExpected'
    END AS invoice_status,
    TO_DATE(inv.p.b, 'yyyyMMdd') AS period_begin_dt,
    TO_DATE(inv.p.e, 'yyyyMMdd') AS period_end_dt,
    CASE
        WHEN inv.t = 2 THEN inv.s.tp.amount / 1000000.0 * (-1)
        WHEN inv.t = 58 THEN inv.s.tp.amount / 1000000.0
    END * cr.rate AS invoice_amount_usd
FROM {{ source('mongo','finance_invoices_v3_daily_snapshot') }} AS inv --mongo.finance_invoices_v3_daily_snapshot AS inv
LEFT JOIN {{ ref('dim_pair_currency_rate') }} AS cr --models.dim_pair_currency_rate  AS cr
    ON
        1 = 1
        AND inv.s.tp.ccy = cr.currency_code
        AND DATE(inv.ct) = cr.effective_date
        AND cr.currency_code_to = 'USD'
WHERE
    1 = 1
    AND (
        inv.t = 58 -- joomSIAJoomMerchantOrderCancellationCharge, iprojJoomMerchantOrderCancellationCharge
        OR
        inv.t = 2 --  // счет Joom мерчантам по рефандам
    )
    AND DATE(inv.ct) >= DATE('2025-01-01')
