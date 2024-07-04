{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@kirill_melnikov',
      'bigquery_load': 'true',
    }
) }}

SELECT
pid as product_id,
jId as mercado_product_id,
d as score,
dt,
millis_to_ts_msk(uber_loader_eventTimeMs) as update_time
FROM  {{ ref('scd2_mongo_product_jpa_matches_daily_snapshot')}}
