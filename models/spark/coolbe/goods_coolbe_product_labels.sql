{{ config(
    schema='coolbe',
    incremental_strategy='insert_overwrite',
    materialized='incremental',
    file_format='parquet',
    partition_by=['partition_date_msk'],
    meta = {
      'model_owner' : '@ekutynina',
      'team': 'clan',
      'bigquery_load': 'true',
      'bigquery_partitioning_date_column': 'partition_date_msk',
      'bigquery_fail_on_missing_partitions': 'false'
    }
) }}

SELECT
    product_id,
    label,
    DATE(partition_date) AS partition_date_msk
FROM {{ source('goods', 'coolbe_product_labels') }}
