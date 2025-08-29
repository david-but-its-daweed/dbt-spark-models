{{
  config(
    meta = {
      'model_owner' : '@operational.analytics.duty',
    },
    materialized='table',
    file_format='parquet',
  )
}}
WITH collect_refund_data AS (
    SELECT
        payment_id,
        refund_id,
        currency AS ref_ccy,
        amount_currency AS ref_amount,
        amount_usd AS ref_amount_usd,
        IF(provider_id = "idbank", FROM_UNIXTIME(TO_UNIX_TIMESTAMP(ts_msk) + 60 * 60), ts_msk) AS ref_time_bank,
        FROM_UNIXTIME(TO_UNIX_TIMESTAMP(ts_msk) - 3 * 60 * 60) AS ref_time_utc,
        is_success AS ref_is_success,
        provider_id AS ref_provider
    FROM {{ source('payments','fact_refund') }}
    WHERE is_success = 1
),

agg_refund_data AS (
    SELECT
        payment_id,
        ref_ccy,
        ref_provider,
        SUM(ref_amount) AS ref_amount,
        SUM(ref_amount_usd) AS ref_amount_usd,
        MIN(ref_time_bank) AS first_ref_time_bank,
        MIN(ref_time_utc) AS first_ref_time_utc,
        ARRAY_AGG(refund_id) AS refund_id_list
    FROM collect_refund_data
    GROUP BY 1, 2, 3
),

pmts_and_refunds AS (
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
        ref.ref_ccy,
        ref.ref_amount,
        ref.ref_amount_usd,
        ref.first_ref_time_bank,
        ref.first_ref_time_utc,
        ref.ref_provider,
        ref.refund_id_list
    FROM {{ ref('payment') }} AS pmt
    LEFT JOIN agg_refund_data AS ref
        ON
            pmt.payment_id = ref.payment_id
            AND pmt.currency = ref.ref_ccy
            AND pmt.provider = ref.ref_provider
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
        ARRAY_AGG(payment_id) AS payment_id_list,
        COUNT(*) AS cnt_pmts
    FROM pmts_and_refunds
    WHERE pmt_is_success = 1
    GROUP BY 1
),

refund_data_by_order_group AS (
    SELECT
        order_group_id AS ref_order_group_id,
        MIN(first_ref_time_bank) AS ref_time_bank,
        MIN_BY(ref_ccy, first_ref_time_bank) AS ref_ccy,
        SUM(ref_amount) AS ref_amount,
        SUM(ref_amount_usd) AS ref_amount_usd,
        MIN_BY(ref_provider, first_ref_time_bank) AS ref_provider,
        ARRAY_SIZE(FLATTEN(ARRAY_AGG(refund_id_list))) AS cnt_refunds,
        FLATTEN(ARRAY_AGG(refund_id_list)) AS refund_id_list
    FROM pmts_and_refunds
    WHERE ref_amount_usd IS NOT null -- берем только ордер группы с рефандами 
    GROUP BY 1
)

SELECT
    t1.order_group_id,
    t1.pmt_time_bank,
    t1.pmt_ccy,
    t1.pmt_amount,
    t1.pmt_amount_usd,
    t1.pmt_provider,
    t1.payment_type,
    t1.cnt_pmts,
    t1.payment_id_list,

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
    t3.refund_id_list,
    (COALESCE(t2.psp_usd_from_costs_final, 0) = 0 AND t1.payment_type = "card" AND t1.pmt_provider != "idbank") AS zero_psp_card_ru_flag
FROM pmt_data_by_order_group AS t1
LEFT JOIN fee_data_by_order_group AS t2
    ON t1.order_group_id = t2.orderGroupId
LEFT JOIN refund_data_by_order_group AS t3
    ON t1.order_group_id = t3.ref_order_group_id