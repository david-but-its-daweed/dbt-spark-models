{{ config(
    materialized='table',
    file_format='delta'
) }}
select 1 as x