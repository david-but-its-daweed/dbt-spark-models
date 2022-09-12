{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'team': 'general_analytics',
      'bigquery_load': 'true'

    }
) }}
SELECT customer_id,
       company_name,
       year_of_establishment,
       estimated_purchase_volume_min,
       estimated_purchase_volume_max,
       legal_entity,
       monthly_turnover_from,
       monthly_turnover_to,
       own_brand,
       purchase_volume_per_month,
       years_of_experience,
       TIMESTAMP(dbt_valid_from) as effective_ts_msk,
       TIMESTAMP(dbt_valid_to) as next_effective_ts_msk
FROM {{ ref('scd2_mongo_customer_main_info') }} t



