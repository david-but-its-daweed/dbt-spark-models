{{ config(
    schema='b2b_mart',
    file_format='delta',
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    meta = {
      'model_owner' : '@kirill_melnikov',
      'bigquery_load': 'true'
    }
) }}


SELECT
    pid AS product_id,
    jid AS mercado_product_id,
    d AS score,
    dt,
    MILLIS_TO_TS_MSK(uber_loader_eventtimems) AS update_time
FROM {{ source('mongo', 'b2b_product_product_jpa_matches_daily_snapshot') }}
