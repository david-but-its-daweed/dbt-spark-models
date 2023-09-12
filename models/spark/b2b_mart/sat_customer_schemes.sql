{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'model_owner' : '@ekutynina',
      'team': 'general_analytics',
      'bigquery_load': 'true'

    }
) }}
SELECT id,
       customer_id,
       schema_id,
       TIMESTAMP(dbt_valid_from) as effective_ts_msk,
       TIMESTAMP(dbt_valid_to) as next_effective_ts_msk
FROM {{ ref('scd2_mongo_customer_schemes') }} t