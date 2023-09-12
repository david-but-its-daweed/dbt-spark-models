{{ config(
    schema='b2b_mart',
    materialized='incremental',
    partition_by=['partition_date_msk'],
    incremental_strategy='insert_overwrite',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'bigquery_partitioning_date_column': 'partition_date_msk'
    }
) }}

SELECT DISTINCT
    DATE(CONCAT(CAST(SUBSTR(tp, 0, 4) AS INT), '-', CAST(SUBSTR(tp, 7, 7) AS INT) * 3 - 2, '-01')) AS quarter,
    egmv.amount / 1000000 AS plan,
    mid AS moderator_id,
    DATE('{{ var("start_date_ymd") }}') AS partition_date_msk
FROM {{ ref('scd2_admin_user_plans_snapshot') }}
WHERE dbt_valid_to IS NULL
