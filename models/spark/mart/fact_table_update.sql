{{ config(
    meta = {
      'model_owner' : '@analytics.duty'
    },
    schema='mart',
    materialized='incremental',
    file_format='parquet',
    incremental_strategy='append',
    partition_by=['table_name','partition_date'],
    tags=['manual']
) }}
-- Why we need this one
-- We need to mark dables to fact_table_update in dbt DAG
-- Right now it is done by separate spark app which is lond and uses pool slot

SELECT NOW() as update_ts, '{{var("table_name")}}' as table_name, DATE('{{ var("start_date_ymd") }}') as partition_date
