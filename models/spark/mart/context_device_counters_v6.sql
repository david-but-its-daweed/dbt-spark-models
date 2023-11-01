{{ config(
    schema='mart',
    materialized='view',
    meta = {
      'model_owner' : '@gburg',
      'bigquery_load': 'true',
      'bigquery_overwrite': 'true',
      'priority_weight' : '150',
      'bigquery_clustering_columns' : ['type', 'context_name'],
      'bigquery_partitioning_date_column' : 'partition_date',
      'bigquery_upload_horizon_days' : '3'
    }
) }}

select device_id,
       ephemeral,
       context_name,
       is_adtech_promoted,
       is_buying_user,
       is_last_context,
       type,
       count,
       CAST(partition_date as DATE) partition_date
FROM {{ source('recom', 'context_device_counters_v6') }}
