{{
  config(
    meta = {
      'model_owner' : '@operational.analytics.duty',
      'bigquery_load': 'true',
      'bigquery_partitioning_date_column': 'date',
      'bigquery_overwrite': 'true',
      'priority_weight': '1000',
    },
    materialized='table',
    file_format='delta',
  )
}}

SELECT
    p.payment_id,
    p.payment_order_id,
    p.order_group_id,
    p.is_success,
    p.payment_type,
    p.fail_reason_message,
    p.is_3ds_int,
    p.is_3ds,
    p.provider,
    p.created_time,
    p.date,
    IF(p.card_id IS NULL, 1, 0) AS is_new_card_int,
    p.card_pan_id,
    p.card_bin,
    p.force3ds,
    p.amount_usd,
    p.amount_currency,
    p.currency,
    p.user_id,
    p.device_id,
    p.version_3ds,
    p.method_3ds,
    p.fee_amount,
    p.fee_currency,
    p.price_range,
    p.pref_country,
    p.antifraud_status_from_event,
    p.antifraud_status_reason,
    p.os_type,
    cb.card_bank,
    cb.card_brand,
    cb.card_country,
    cb.card_level,
    cb.card_type
FROM
    {{ source('payments','payment') }} AS p
LEFT JOIN
    {{ ref('card_bins') }} AS cb USING (card_bin)
WHERE
    p.payment_type != 'points'
    AND (YEAR(TO_DATE('{{ var("start_date_ymd") }}')) - YEAR(p.date)) < 2
