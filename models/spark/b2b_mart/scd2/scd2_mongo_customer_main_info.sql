{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'bigquery_load': 'false'
    }
) }}


SELECT
       _id AS                              customer_id,
       millis_to_ts_msk(utms)          AS update_ts_msk,
       companyName                     AS company_name,
       est                             AS year_of_establishment,
       purchaseVolumePerMonth          AS estimated_purchase_volume,
       hasLegalEntity                  AS legal_entity,
       monthlyTurnover.from            AS monthly_turnover_from,
       monthlyTurnover.to              AS monthly_turnover_to,
       ownBrand                        AS own_brand,
       purchaseVolumePerMonth          AS purchase_volume_per_month,
       yearsOfExperience               AS years_of_experience,
       invitedByPromo                  AS invited_by_promo,
       invitedTime                     AS invited_time,
       isPartner                       AS is_partner,
       gradeInfo.grade                 AS grade,
       gradeInfo.prob                  AS grade_probability,
       firstDealPlanningVolume.amount  AS first_deal_planning_volume,
       firstDealPlanningVolume.ccy     AS first_deal_planning_currency,
       firstDealPlanningVolumeUsd      AS first_deal_planning_volume_usd,
       tracked,
       dbt_scd_id,
       dbt_updated_at,
       dbt_valid_from,
       dbt_valid_to
    FROM {{ ref('scd2_customers_snapshot') }}
