{{ config(
    schema='example',
    materialized='view',
    meta = {
      'team': 'platform'
    }
) }}

select
  merchant_id,
  name,
  origin,
  create_ts_msk,
  dbt_valid_from as effective_ts_msk,
  dbt_valid_to as next_effective_ts_msk
from {{ ref('scd2_mongo_merchant') }} t
