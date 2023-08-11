{{ config(
    schema='msaf',
    materialized='table',
    file_format='delta',
    meta = {
      'team': 'platform',
      'bigquery_load': 'true'
    }
) }}

    select
        1 as num_field,
        'a' as str_field