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
    pid AS product_id,
    jid AS mercado_product_id,
    d AS score,
    dt,
    MILLIS_TO_TS_MSK(uber_loader_eventtimems) AS update_time
FROM {{ source('mongo', 'b2b_core_product_jpa_matches_daily_snapshot') }}