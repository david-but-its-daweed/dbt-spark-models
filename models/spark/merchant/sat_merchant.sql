{{ config(
    schema='merchant',
    materialized='view',
    meta = {
      'model_owner' : '@gburg',
          'bigquery_load': 'true',
    }
) }}

select
    merchant_id,
       created_time,
       updated_time,
       activation_time,
       name,
       origin,
       enabled,
       disablingReason,
       disablingNote,
       dbt_valid_from as effective_ts,
       coalesce(dbt_valid_to, cast('9999-12-31 23:59:59' as timestamp)) as next_effective_ts
FROM {{ ref('scd2_mongo_merchant')}}