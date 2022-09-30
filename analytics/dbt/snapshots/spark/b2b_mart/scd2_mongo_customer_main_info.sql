{% snapshot scd2_mongo_customer_main_info %}

{{
    config(
      target_schema='b2b_mart',
      unique_key='customer_id',

      strategy='timestamp',
      updated_at='update_ts_msk',
      file_format='delta'
    )
}}

SELECT _id AS                              customer_id,
       millis_to_ts_msk(utms)          AS update_ts_msk,
       companyName                     AS company_name,
       est                             AS year_of_establishment,
       purchaseVolumePerMonth          AS estimated_purchase_volume,
       hasLegalEntity                  AS legal_entity,
       monthlyTurnover.from            AS monthly_turnover_from,
       monthlyTurnover.to              AS monthly_turnover_to,
       ownBrand                        AS own_brand,
       purchaseVolumePerMonth          AS purchase_volume_per_month,
       yearsOfExperience               AS years_of_experience
FROM {{ source('mongo', 'b2b_core_customers_daily_snapshot') }}
{% endsnapshot %}
