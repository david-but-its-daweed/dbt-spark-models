{{ config(
    schema='junk2',
    materialized='incremental',
    file_format='delta',
) }}

{% if not is_incremental() %}
  select 1 as x from unknown_table
{% else %}
  select 2 as x
{% endif %}
