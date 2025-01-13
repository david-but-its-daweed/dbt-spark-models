{{ config(
    schema='coolbe',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@product-analytics.duty',
      'team': 'clan',
      'bigquery_load': 'true'
    }
) }}

SELECT
    product_id,
    label,
    CAST(partition_date AS DATE) AS partition_date_msk
FROM {{ source('goods', 'coolbe_product_labels') }}