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

WITH pmt_fee_ref_data_by_order_group AS (
    SELECT *
    FROM {{ ref('bs_ip_overview_raw_data') }}
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