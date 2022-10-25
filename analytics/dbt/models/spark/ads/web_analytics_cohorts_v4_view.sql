{{ config(
    schema='ads',
    materialized='view',
    partition_by=['partition_date'],
    meta = {
      'team': 'ads',
      'bigquery_load': 'true',
      'bigquery_table_name': 'ads.web_analytics_cohorts_v4'
      'bigquery_partitioning_date_column': 'partition_date'
    }
) }}
select *
from {{ source('ads', 'web_analytics_cohorts_v4') }}
