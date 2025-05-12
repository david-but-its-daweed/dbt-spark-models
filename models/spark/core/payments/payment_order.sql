{{
  config(
    meta = {
      'model_owner' : '@operational.analytics.duty',
      'bigquery_load': 'true',
      'bigquery_partitioning_date_column': 'date',
      'priority_weight': '1000',
      'bigquery_overwrite': 'true',
    },
    materialized='table',
    file_format='delta',
  )
}}


WITH device_day_payments AS (
    SELECT
        device_id,
        date,
        IF(COUNT(DISTINCT CONCAT(device_id, provider)) = 1, 1, 0)
        AS is_only_one_provider
    FROM
        {{ ref('payment') }}
    WHERE
        DATEDIFF(TO_DATE('{{ var("start_date_ymd") }}'), date) < 181
    GROUP BY
        1, 2
),

chargebacks AS (
    SELECT
        payment_id,
        MAX(date_msk) AS chb_date,
        MAX(reason_code) AS chb_reason_code,
        MAX(reason_category) AS chb_reason_category
    FROM
        {{ source('payments','chargeback') }}
    GROUP BY
        payment_id
),

refunds AS (
    SELECT
        payment_id,
        MAX(1) AS has_refund_att,
        MAX(is_success) AS has_success_refund,
        SUM(amount_usd * is_success) AS success_refund_usd
    FROM
        {{ source('payments','fact_refund') }}
    GROUP BY
        payment_id
),

slo AS (
    SELECT
        country AS pref_country,
        MAX(IF(country_filter_name = 'SLO_card', value, NULL)) AS slo_card,
        MAX(IF(country_filter_name = 'SLO_all', value, NULL)) AS slo_all,
        MAX(IF(country_filter_name = 'top20gmv', rank, NULL)) AS country_rank
    FROM
        {{ source('payments','country_filter') }}
    GROUP BY
        1
),

categories AS (
    SELECT DISTINCT
        o.order_group_id,
        FIRST_VALUE(
            category_id) OVER (
            PARTITION BY o.order_group_id
            ORDER BY o.gmv_initial DESC, o.order_id ASC
        ) AS category_id,
        FIRST_VALUE(
            c.l1_category_name) OVER (
            PARTITION BY o.order_group_id
            ORDER BY o.gmv_initial DESC, o.order_id ASC
        ) AS category_name
    FROM
        {{ ref('orders') }} AS o
    LEFT JOIN
        {{ ref('categories') }} AS c USING (category_id)
),

orders_info AS (
    SELECT
        o.order_group_id,
        SUM(o.psp_initial) AS psp_initial,
        SUM(o.psp_refund_fee) AS psp_refund_fee,
        SUM(o.psp_chargeback_fee) AS psp_chargeback_fee,
        SUM(o.order_gross_profit_final_estimated) AS order_gross_profit_final_estimated,
        SUM(o.gmv_initial) AS gmv_initial,
        MAX(o.country) AS shipping_country
    FROM
        {{ ref('orders') }} AS o
    GROUP BY
        1
),

ads_info AS (
    SELECT
        device_id,
        source AS ads_source,
        partner_id AS ads_partner_id
    FROM
        {{ source('ads','ads_install') }}
    WHERE
        join_date >= DATE_SUB(CURRENT_DATE(), 365)
)

SELECT
    po.device_id,
    po.payment_id,
    po.payment_order_id,
    po.user_id,
    po.created_time,
    po.is_success,
    po.target_type,
    po.target_id,
    po.order_group_id,
    po.payment_type,
    po.payment_origin,
    po.is_new_card,
    po.is_new_card_int,
    po.card_id,
    po.card_pan_id,
    po.currency,
    po.amount_usd,
    po.amount_currency,
    po.request3ds,
    po.antifraud_status,
    po.blacklist_status,
    po.is_potentially_forbidden_zip_int,
    po.is_skipped_address_provider_check_int,
    po.cancel_reason_type,
    po.cancel_reason_message,
    po.device_day,
    po.price_range_id,
    po.price_range,
    po.fail_reason_type,
    po.provider,
    po.is_3ds_int,
    po.is_3ds,
    po.version_3ds,
    po.method_3ds,
    po.time_3ds,
    po.external_ids,
    po.currency_pmt,
    po.card_bin,
    po.fail_reason_message,
    po.fail_reason_type_string,
    po.pref_country,
    po.antifraud_status_from_event,
    po.jaf_status,
    po.os_type,
    po.riskified_status,
    po.antifraud_status_reason,
    po.cancel_reason_type_string,
    po.rule_name,
    po.plan_id,
    po.time_to_fail_sec,
    po.time_to_success_sec,
    po.double_og,
    po.is_new_device,
    po.is_new_device_24h,
    po.is_request_cvv,
    po.is_get_cvv,
    po.new_user_by_purchase,
    po.new_user_by_purchase_string,
    po.has_cascade,
    po.chb_reason,
    po.chb_status,
    po.is_chb_reverted_int,
    po.has_chb,
    po.new_device_by_purchase,
    po.new_device_by_purchase_string,
    po.number_attempt_device,
    po.is_first_success_device,
    po.is_started_second_device,
    po.is_second_plus_success_device,
    po.cnt_attempts_device,
    po.time_success,
    po.time_ses_success,
    po.numb_attempt_og,
    po.riskified_fees,
    po.reason_3ds,
    po.date,
    po.has_linked_card,
    po.klarna_real_type,
    po.klarna_desctription,
    po.klarna_type,
    po.is_visa_discount_applicable,

    ROW_NUMBER() OVER (PARTITION BY po.device_day, po.provider, po.payment_type, po.is_new_card_int ORDER BY po.created_time)
    AS number_attempt_provider_payment_type,
    MAX(po.is_success) OVER (PARTITION BY po.device_day, po.provider, po.payment_type, po.is_new_card_int)
    AS has_success_provider_payment_type,
    CONCAT(TRUNC(po.date, 'MM'), po.provider, po.payment_type, po.is_new_card_int) AS pp_id,

    IF(po.payment_type = 'card', IF(
        po.is_new_card_int = 1,
        'new_card', 'linked_card'
    ), po.payment_type) AS payment_type_extended,
    MAX(IF(po.cancel_reason_message = 'blacklist , reason sanctionList', 1, 0))
        OVER
        (PARTITION BY po.user_id ORDER BY po.created_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    AS has_sanction_block_user,
    cb.card_bank,
    cb.card_brand,
    cb.card_country,
    cb.card_level,
    cb.card_type,
    ddp.is_only_one_provider,
    DATEDIFF(chb.chb_date, po.date) AS chb_days_dif,
    chb.chb_date,
    chb.chb_reason_code,
    chb.chb_reason_category,
    CONCAT(po.device_id, STRING(po.date)) AS dev_date,
    COALESCE(r.has_refund_att, 0) AS has_refund_att,
    COALESCE(r.has_success_refund, 0) AS has_success_refund,
    COALESCE(r.success_refund_usd, 0) AS success_refund_usd,
    po.amount_currency * cur.rate AS amount_eur,
    '<a href="https://admin.joom.it/payments/' || po.payment_id
    || '" target="_blank">' || po.payment_id || '</a>' AS p_id_to_admin_link,
    s.slo_card,
    s.slo_all,
    s.country_rank,
    CASE
        WHEN po.amount_usd < 10 THEN '0. less 10$'
        WHEN po.amount_usd < 20 THEN '1. 10-20$'
        WHEN po.amount_usd < 30 THEN '2. 20-30$'
        WHEN po.amount_usd < 40 THEN '3. 30-40$'
        WHEN po.amount_usd < 50 THEN '4. 40-50$'
        WHEN po.amount_usd < 60 THEN '5. 50-60$'
        WHEN po.amount_usd < 70 THEN '6. 60-70$'
        WHEN po.amount_usd < 80 THEN '7. 70-80$'
        WHEN po.amount_usd < 100 THEN '8. 80-100$'
        ELSE '9. more 100$'
    END AS price_segment_wide,
    IF(po.currency = po.currency_pmt, 0, 1) AS is_currency_diff_int,
    IF(po.currency = po.currency_pmt, 'coinciding', 'different')
    AS is_currency_diff,
    IF(
        po.has_chb = 1 AND chb.chb_reason_category
        != 'consumer', 'has_fraud_chb', 'no_chb'
    ) AS has_fraud_chb,
    c.category_id,
    c.category_name,
    oi.psp_initial,
    oi.psp_refund_fee,
    oi.psp_chargeback_fee,
    oi.order_gross_profit_final_estimated,
    oi.gmv_initial,
    COALESCE (po.is_success = 1
    AND ROW_NUMBER() OVER (
        PARTITION BY po.order_group_id
        ORDER BY CASE WHEN po.is_success = 1 THEN po.created_time END
    ) = 1, FALSE) AS is_first_success_po_over_order_group,

    COALESCE(oi.shipping_country, 'unknown') AS shipping_country,
    COALESCE(ai.ads_source, '-') AS ads_source,
    COALESCE(ai.ads_partner_id, '-') AS ads_partner_id,
    og.discount_amount_ccy,
    og.discount_amount_ccy * cur.rate AS discount_amount_eur,
    IF(og.discount_amount_ccy > 0, 1, 0) AS is_discount_applied,
    IF(
        po.pref_country = 'MD' AND cb.card_brand = 'VISA' AND po.amount_usd >= 17
        AND po.currency IN ('MDL', 'USD', 'EUR', 'UAH', 'RUB', 'RON', 'GBP'), 1, 0
    ) AS is_discount_proper
FROM
    {{ source('payments','payment_order') }} AS po
LEFT JOIN
    {{ ref('card_bins') }} AS cb USING (card_bin)
LEFT JOIN
    device_day_payments AS ddp USING (device_id, date)
LEFT JOIN
    chargebacks AS chb USING (payment_id)
LEFT JOIN
    refunds AS r USING (payment_id)
LEFT JOIN
    {{ ref('dim_pair_currency_rate') }} AS cur
    ON
        po.date = cur.effective_date
        AND po.currency = cur.currency_code AND cur.currency_code_to = 'EUR'
LEFT JOIN
    slo AS s USING (pref_country)
LEFT JOIN
    categories AS c
    ON po.order_group_id = c.order_group_id AND po.is_success = 1
LEFT JOIN
    orders_info AS oi
    ON po.order_group_id = oi.order_group_id AND po.is_success = 1
LEFT JOIN
    ads_info AS ai USING (device_id)
LEFT JOIN
    {{ source('payments','order_groups') }} AS og USING (payment_order_id)
WHERE
    po.date >= DATE_SUB(CURRENT_DATE(), 750)
    AND po.payment_type != 'points'