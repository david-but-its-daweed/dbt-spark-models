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
      'bigquery_partitioning_date_column': 'partition_date_msk',
    }
) }}

select 
distinct 
          date(concat(cast(substr(tp, 0, 4) as int),'-',cast(substr(tp, 7, 7) as int)*3 - 2,'-01')) as quarter,
          mid as admin_id,
          hn as hypothesis_name,
          egmv.amount/1000000 as plan_gmv,
          lc as plan_leads,
          clc as plan_converted_leads,
          date('{{ var("start_date_ymd") }}') as partition_date_msk
from {{ ref('scd2_sdr_plans_snapshot') }}
where dbt_valid_to is null
