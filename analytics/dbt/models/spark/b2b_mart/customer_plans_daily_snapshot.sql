{{ config(
    schema='b2b_mart',
    materialized='incremental',
    partition_by=['partition_date_msk'],
    incremental_strategy='insert_overwrite',
    file_format='parquet',
    meta = {
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'bigquery_partitioning_date_column': 'partition_date_msk'
    }
) }}

select 
distinct 
            date(concat(cast(substr(tp, 0, 4) as int),'-',cast(substr(tp, 7, 7) as int)*3 - 2,'-01')) as quarter,
            egmv.amount/1000000 as plan,
            uid as user_id,
            date('{{ var("start_date_ymd") }}') as partition_date_msk
from {{ source('mongo', 'b2b_core_customer_plans_daily_snapshot') }}
