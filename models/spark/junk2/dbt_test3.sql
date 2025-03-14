{{ config(
    meta = {
        'model_owner' : '@msafonov'
    },
    schema='junk2',
    materialized='table',
    file_format='parquet',
) }}

select 1 as x
