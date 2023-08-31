{{ config(
    schema='mart',
    materialized='view',
    meta = {
      'bigquery_load': 'true',
      'bigquery_clustering_columns': ['context_name'],
      'bigquery_partitioning_date_column': 'partition_date',
      'bigquery_upload_horizon_days': '3',
      'priority_weight': '150'
    }
) }}

select *
FROM {{ source('platform', 'context_product_counters_v5') }}
