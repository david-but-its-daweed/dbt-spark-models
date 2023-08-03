{{ config(
    schema='ads',
    materialized='view',
    meta = {
      'team': 'ads',
      'bigquery_load': 'true',
      'bigquery_table_name': 'ads.web_analytics_cohorts_v4',
      'bigquery_partitioning_date_column': 'partition_date',
      'bigquery_overwrite': 'true'
    }
) }}
select *
from {{ source('ads', 'web_analytics_cohorts_v4') }}
