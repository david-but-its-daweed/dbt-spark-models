{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}


select '00' as id, 'NeedValidation' as status
union all
select '20' as id, 'Rejected' as status
union all
select '30' as id, 'ValidatedNoConversionAttempt' as status
union all
select '40' as id, 'ConversionFailed' as status
union all
select '50' as id, 'Converting' as status
union all
select '60' as id, 'Converted' as status
union all
select '70' as id, 'Lost' as status
