{{ config(
    meta = {
        'model_owner' : '@msafonov'
    },
    schema='junk2',
    materialized='table',
    file_format='parquet',
) }}

SELECT * FROM {{ ref("gold_orders") }} LIMIT 1000