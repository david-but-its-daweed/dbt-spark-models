{{ config(
    schema='msaf',
    materialized='table',
    file_format='delta',
    persist_docs={"relation": true, "columns": false},
    meta = {
      'team': 'msaf',
    }
) }}
select
    1 as field1,
    'a' as field2
    ;