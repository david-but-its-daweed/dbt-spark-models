{{
  config(
    materialized='table',
    file_format='delta',
    partition_by=['date'],
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
        DATEDIFF(CURRENT_DATE(), date) < 181
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
        MAX(1) AS has_refund,
        SUM(amount_usd) AS refund_usd
    FROM
        {{ source('payments','fact_refund') }}
    WHERE
        is_success = 1
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
        o.order_group_id AS target_id,
        FIRST_VALUE(
            category_id) OVER (
            PARTITION BY o.order_group_id
            ORDER BY o.gmv_initial DESC
        ) AS category_id,
        FIRST_VALUE(
            c.l1_category_name) OVER (
            PARTITION BY o.order_group_id
            ORDER BY o.gmv_initial DESC
        ) AS category_name
    FROM
        {{ ref('orders') }} AS o
    LEFT JOIN
        {{ ref('categories') }} AS c USING (category_id)
),

orders_info AS (
    SELECT
        og.payment_order_id,
        SUM(o.psp_initial) AS psp_initial,
        SUM(o.psp_refund_fee) AS psp_refund_fee,
        SUM(o.psp_chargeback_fee) AS psp_chargeback_fee,
        MAX(o.country) AS shipping_country
    FROM
        {{ ref('orders') }} AS o
    INNER JOIN
        {{ source('payments','order_groups') }} AS og USING (order_group_id)
    GROUP BY
        1
)

SELECT
    po.*,
    po.target_id AS order_group_id,
    MAX(po.is_success) OVER (PARTITION BY po.device_day, po.provider) AS has_success_provider,
    ROW_NUMBER() OVER (PARTITION BY po.device_day, po.provider ORDER BY po.created_time) AS number_att_provider,
    MAX(po.is_success) OVER (PARTITION BY po.device_day, po.payment_type, po.is_new_card) AS has_success_pmt_type,
    ROW_NUMBER() OVER (PARTITION BY po.device_day, po.payment_type, po.is_new_card ORDER BY po.created_time) AS number_att_pmt_type,
    IF(po.payment_type = 'card', IF(
        po.is_new_card_int = 1,
        'new_card', 'linked_card'
    ), po.payment_type) AS payment_type_extended,
    MAX(IF(po.cancel_reason_message = 'blacklist , reason sanctionList', 1, 0)) OVER
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
    r.has_refund,
    r.refund_usd,
    po.amount_currency * cur.rate AS amount_eur,
    '<a href="https://admin.joom.it/payments/' || payment_id
    || '" target="_blank">' || payment_id || '</a>' AS p_id_to_admin_link,
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
    COALESCE(oi.shipping_country, 'unknown') AS shipping_country
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
    categories AS c USING (target_id)
LEFT JOIN
    orders_info AS oi USING (payment_order_id)
WHERE
    po.payment_type != 'points'
    AND (YEAR(CURRENT_DATE()) - YEAR(po.date)) < 2