{{
  config(
    materialized='view',
    alias='payment_orders',
    schema='gold',
    file_format='parquet',
    meta = {
        'model_owner' : '@gusev',
        'bigquery_load': 'true',
        'bigquery_overwrite': 'true',
        'bigquery_override_dataset_id': 'gold_migration',
        'priority_weight': '1000',
    }
  )
}}

SELECT
    payment_order_id,
    order_group_id,

    device_id,
    user_id,
    UPPER(pref_country) AS device_country_code,
    LOWER(os_type) AS platform,
    COALESCE(new_device_by_purchase = 1, FALSE) AS is_first_successful_payment_on_date_for_device,

    TO_DATE(created_time) AS payment_date_msk,

    COALESCE(is_success = 1, FALSE) AS is_success,
    COALESCE(has_cascade = 1, FALSE) AS is_with_cascade_of_payment_attempts,
    COALESCE(has_chb = 1, FALSE) AS is_with_chargeback,
    COALESCE(has_refund = 1, FALSE) AS is_refunded,

    payment_type,
    payment_origin,
    provider AS provider_name,
    is_new_card AS card_linking_status,
    card_brand,
    card_country AS card_country_code,
    card_bin,
    COALESCE(is_3ds_int = 1, FALSE) AS is_3ds,
    reason_3ds AS initiator_3ds,

    UPPER(currency_pmt) AS payment_currency,
    UPPER(currency) AS user_currency,
    amount_currency AS payment_amount_user_currency,
    amount_usd AS payment_amount,
    refund_usd AS refund_amount,

    antifraud_status,

    cancel_reason_type_string AS cancel_reason_type,
    cancel_reason_message
FROM {{ ref('payment_order') }}