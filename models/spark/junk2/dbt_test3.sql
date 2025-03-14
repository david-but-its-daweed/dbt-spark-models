{{ config(
    meta = {},
    schema='junk2',
    materialized='table',
    file_format='parquet',
) }}

select 1 as x
