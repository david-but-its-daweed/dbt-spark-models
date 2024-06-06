{{
    config(
        materialized='view',
        alias='active_devices',
        schema='gold',
        meta = {
            'model_owner' : '@general_analytics',
            'bigquery_load': 'true',
            'bigquery_partitioning_date_column': 'date_msk',
            'bigquery_check_counts_max_diff_fraction': '0.01',
            'bigquery_upload_horizon_days': '230',
            'priority_weight': '1000',
        }
    )
}}

SELECT
    date_msk,
    device_id,
    real_user_id,
    country_code,
    top_country_code,
    country_priority_type,
    region_name,
    app_language,
    platform,
    legal_entity,
    app_entity,
    join_date_msk,
    real_user_segment,
    is_new_device,
    device_lifetime,
    previous_activity_device_group,
    prev_date_msk_lag,
    next_date_msk_lag,
    gmv_per_day_initial,
    gmv_per_day_final,
    order_gross_profit_per_day_final_estimated,
    order_gross_profit_per_day_final,
    ecgp_per_day_initial,
    ecgp_per_day_final,
    number_of_orders,
    is_payer,
    is_converted,
    is_rd1,
    is_rd3,
    is_rd7,
    is_rd14,
    is_rw1,
    is_rw2,
    is_rw3,
    is_rw4,
    is_churned_14,
    is_churned_28
FROM {{ ref('gold_active_devices_with_ephemeral') }}
WHERE is_ephemeral_device = FALSE
