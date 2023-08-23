{{
    config(
        materialized='table',
        alias='active_users',
        schema='gold',
        partition_by=['date_msk'],
        file_format='delta',
        meta = {
            'model_owner' : '@gusev'
        }
    )
}}

SELECT
    date_msk,
    user_id,
    real_user_id,
    country_code,
    top_country_code,
    region_name,
    app_language,
    platform,
    legal_entity,
    join_date_msk,
    real_user_segment,
    is_new_user,
    user_lifetime,
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
FROM {{ ref('gold_active_users_with_ephemeral') }}
WHERE is_ephemeral_user = FALSE
