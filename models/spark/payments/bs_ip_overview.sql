{{
  config(
    meta = {
      'model_owner' : '@operational.analytics.duty',
      'bigquery_load': 'true',
      'bigquery_overwrite': 'true',
    },
    materialized='table',
    file_format='parquet',
  )
}}

WITH pmts_and_refunds AS (
    SELECT
        pmt.order_group_id,
        pmt.payment_id,
        pmt.payment_order_id,
        IF(pmt.provider = "idbank", FROM_UNIXTIME(TO_UNIX_TIMESTAMP(pmt.created_time) + 60 * 60), pmt.created_time) AS pmt_time_bank, -- UTC+4 если idbank, UTC+3 для остальных
        FROM_UNIXTIME(TO_UNIX_TIMESTAMP(pmt.created_time) - 3 * 60 * 60) AS pmt_time_utc,
        pmt.currency AS pmt_ccy,
        pmt.amount_currency AS pmt_amount,
        pmt.amount_usd AS pmt_amount_usd,
        pmt.is_success AS pmt_is_success,
        pmt.provider AS pmt_provider,
        pmt.payment_type,
        ref.currency AS ref_ccy,
        ref.amount_currency AS ref_amount,
        ref.amount_usd AS ref_amount_usd,
        IF(pmt.provider = "idbank", FROM_UNIXTIME(TO_UNIX_TIMESTAMP(ref.ts_msk) + 60 * 60), ref.ts_msk) AS ref_time_bank,
        FROM_UNIXTIME(TO_UNIX_TIMESTAMP(ref.ts_msk) - 3 * 60 * 60) AS ref_time_utc,
        ref.is_success AS ref_is_success,
        ref.provider_id AS ref_provider
    FROM {{ ref('payment') }} AS pmt
    LEFT JOIN {{ source('payments','fact_refund') }} AS ref
        ON pmt.payment_id = ref.payment_id
    WHERE
        pmt.date >= DATE '2024-01-01'
        AND pmt.provider IN ("paygine_bs", "paygine_ip", "raifpay_bs", "raifpay_ip", "idbank", "yandexpay_bs")
        AND pmt.payment_type IN ("card", "sbp", "yandexPay", "yandexSplit")
        AND pmt.order_group_id IS NOT null
),

fee_data_by_order_group AS (
    SELECT
        orderGroupId,
        SUM(costs.pspChargeFeeInitial.amount) AS psp_from_cost_initial,
        ANY_VALUE(costs.pspChargeFeeInitial.ccy) AS psp_ccy_from_costs_initial,
        SUM(costs.pspChargeFeeInitial.usd) AS psp_usd_from_costs_initial,

        SUM(costs.pspFinal.amount) AS psp_from_cost_final,
        ANY_VALUE(costs.pspFinal.ccy) AS psp_ccy_from_costs_final,
        SUM(costs.pspFinal.usd) AS psp_usd_from_costs_final
    FROM {{ source('mongo','finance_order_costs_daily_snapshot') }}
    GROUP BY 1
),

pmt_data_by_order_group AS (
    SELECT
        order_group_id,
        MIN(pmt_time_bank) AS pmt_time_bank,
        MIN_BY(pmt_ccy, pmt_time_bank) AS pmt_ccy,
        SUM(pmt_amount) AS pmt_amount,
        SUM(pmt_amount_usd) AS pmt_amount_usd,
        MIN_BY(pmt_provider, pmt_time_bank) AS pmt_provider,
        MIN_BY(payment_type, pmt_time_bank) AS payment_type,
        COUNT(*) AS cnt_pmts
    FROM pmts_and_refunds
    WHERE pmt_is_success = 1
    GROUP BY 1
),

refund_data_by_order_group AS (
    SELECT
        order_group_id AS ref_order_group_id,
        MIN(ref_time_bank) AS ref_time_bank,
        MIN_BY(ref_ccy, pmt_time_bank) AS ref_ccy,
        SUM(ref_amount) AS ref_amount,
        SUM(ref_amount_usd) AS ref_amount_usd,
        MIN_BY(ref_provider, pmt_time_bank) AS ref_provider,
        COUNT(*) AS cnt_refunds
    FROM pmts_and_refunds
    WHERE ref_is_success = 1
    GROUP BY 1
),

pmt_fee_ref_data_by_order_group AS (
    SELECT
        t1.order_group_id,
        t1.pmt_time_bank,
        t1.pmt_ccy,
        t1.pmt_amount,
        t1.pmt_amount_usd,
        t1.pmt_provider,
        t1.payment_type,
        t1.cnt_pmts,

        t2.psp_from_cost_initial,
        t2.psp_ccy_from_costs_initial,
        t2.psp_usd_from_costs_initial,
        t2.psp_from_cost_final,
        t2.psp_ccy_from_costs_final,
        t2.psp_usd_from_costs_final,

        t3.ref_time_bank,
        t3.ref_ccy,
        t3.ref_amount,
        t3.ref_amount_usd,
        t3.ref_provider,
        t3.cnt_refunds,
        (t2.psp_usd_from_costs_final = 0 AND t1.payment_type = "card" AND t1.pmt_provider != "idbank") AS zero_psp_card_ru_flag
    FROM pmt_data_by_order_group AS t1
    LEFT JOIN fee_data_by_order_group AS t2
        ON t1.order_group_id = t2.orderGroupId
    LEFT JOIN refund_data_by_order_group AS t3
        ON t1.order_group_id = t3.ref_order_group_id
),

nonzeropsp_refunds_by_refund_dt AS (
    SELECT
        DATE(DATE_TRUNC("day", ref_time_bank)) AS dt,
        pmt_provider,
        payment_type,
        pmt_ccy,
        SUM(IF(zero_psp_card_ru_flag, 0, cnt_refunds)) AS nonzeropsp_cnt_refunds,
        SUM(IF(zero_psp_card_ru_flag, 0, ref_amount)) AS nonzeropsp_ref_amount,
        SUM(IF(zero_psp_card_ru_flag, 0, ref_amount_usd)) AS nonzeropsp_ref_amount_usd
    FROM pmt_fee_ref_data_by_order_group
    WHERE ref_time_bank IS NOT null
    GROUP BY 1, 2, 3, 4
),

pmt_agg_data AS (
    SELECT
        DATE(DATE_TRUNC("day", pmt_time_bank)) AS dt,
        pmt_provider,
        payment_type,
        pmt_ccy,

        SUM(psp_from_cost_initial) AS psp_initial,
        SUM(psp_usd_from_costs_initial) AS psp_initial_usd,
        SUM(psp_from_cost_final) AS psp_final,
        SUM(psp_usd_from_costs_final) AS psp_final_usd,

        SUM(IF(zero_psp_card_ru_flag, 0, cnt_pmts)) AS nonzeropsp_cnt_pmts,
        SUM(IF(zero_psp_card_ru_flag, 0, pmt_amount)) AS nonzeropsp_pmt_amount,
        SUM(IF(zero_psp_card_ru_flag, 0, pmt_amount_usd)) AS nonzeropsp_pmt_amount_usd
    FROM pmt_fee_ref_data_by_order_group
    GROUP BY 1, 2, 3, 4
)

SELECT
    t1.dt,
    t1.pmt_provider,
    t1.payment_type,
    t1.pmt_ccy,

    t1.psp_initial,
    t1.psp_initial_usd,
    t1.psp_final,
    t1.psp_final_usd,

    t1.nonzeropsp_cnt_pmts,
    t1.nonzeropsp_pmt_amount,
    t1.nonzeropsp_pmt_amount_usd,
    COALESCE(t2.nonzeropsp_cnt_refunds, 0) AS nonzeropsp_cnt_refunds,
    COALESCE(t2.nonzeropsp_ref_amount, 0) AS nonzeropsp_ref_amount,
    COALESCE(t2.nonzeropsp_ref_amount_usd, 0) AS nonzeropsp_ref_amount_usd
FROM pmt_agg_data AS t1
LEFT JOIN nonzeropsp_refunds_by_refund_dt AS t2
    USING (dt, pmt_provider, payment_type, pmt_ccy)