{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@mkirusha',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}


SELECT
    *,
    TIMESTAMP(d.dbt_valid_from) AS effective_ts_msk,
    TIMESTAMP(d.dbt_valid_to) AS next_effective_ts_msk
FROM {{ ref('scd2_merchants_snapshot') }} AS d
