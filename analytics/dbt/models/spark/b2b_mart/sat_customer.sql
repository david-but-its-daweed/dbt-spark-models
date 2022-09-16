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
       CASE WHEN purchaseVolumePerMonth = 10 THEN "PurchaseVolume_1_150"
       CASE WHEN purchaseVolumePerMonth = 20 THEN "PurchaseVolume_150_300"
       CASE WHEN purchaseVolumePerMonth = 30 THEN "PurchaseVolume_300_500"
       CASE WHEN purchaseVolumePerMonth = 40 THEN "PurchaseVolume_500_700"
       CASE WHEN purchaseVolumePerMonth = 50 THEN "PurchaseVolume_700_1000"
       CASE WHEN purchaseVolumePerMonth = 60 THEN "PurchaseVolume_1000_1500"
       CASE WHEN purchaseVolumePerMonth = 70 THEN "PurchaseVolume_1500_5000"
       CASE WHEN purchaseVolumePerMonth = 80 THEN "PurchaseVolume_5000_15000"
       CASE WHEN purchaseVolumePerMonth = 90 THEN "PurchaseVolume_15000"
       END AS purchase_volume_per_month,
       legal_entity,
       monthly_turnover_from,
       monthly_turnover_to,
       own_brand,
       purchase_volume_per_month,
       years_of_experience,
       TIMESTAMP(dbt_valid_from) as effective_ts_msk,
       TIMESTAMP(dbt_valid_to) as next_effective_ts_msk
FROM {{ ref('scd2_mongo_customer_main_info') }} t



