{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'model_owner' : '@amitiushkina',
      'team': 'general_analytics',
      'bigquery_load': 'true'

    }
) }}
SELECT customer_id,
       company_name,
       year_of_establishment,
       CASE WHEN estimated_purchase_volume = 10 THEN "PurchaseVolume_1_150"
            WHEN estimated_purchase_volume = 20 THEN "PurchaseVolume_150_300"
            WHEN estimated_purchase_volume = 30 THEN "PurchaseVolume_300_500"
            WHEN estimated_purchase_volume = 40 THEN "PurchaseVolume_500_700"
            WHEN estimated_purchase_volume = 50 THEN "PurchaseVolume_700_1000"
            WHEN estimated_purchase_volume = 60 THEN "PurchaseVolume_1000_1500"
            WHEN estimated_purchase_volume = 70 THEN "PurchaseVolume_1500_5000"
            WHEN estimated_purchase_volume = 80 THEN "PurchaseVolume_5000_15000"
            WHEN estimated_purchase_volume = 90 THEN "PurchaseVolume_15000"
       END AS estimated_purchase_volume,
       legal_entity,
       monthly_turnover_from,
       monthly_turnover_to,
       own_brand,
       purchase_volume_per_month,
       years_of_experience,
       invited_by_promo,
       invited_time,
       is_partner,
       grade,
       grade_probability,
       TIMESTAMP(dbt_valid_from) as effective_ts_msk,
       TIMESTAMP(dbt_valid_to) as next_effective_ts_msk
FROM {{ ref('scd2_mongo_customer_main_info') }} t



