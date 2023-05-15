{{ config(
    schema='platform',
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by=['date'],
    file_format='parquet',
) }}

    with usage as (select *,
                      split(prefix, "/") as prefix_parts
               from {{source('platform', 's3_file_usage')}}
               where
                    {% if not is_incremental() %}
                      date = '{{ var("start_date_ymd") }}'
                    {% endif %}
    ),

     tables as (select *,
                       split(prefix, "/") as prefix_parts
                from {{source('platform', 'table_locations')}})

select usage.*,
       full_table_name,
       usage.prefix_parts  as usage_parts,
       tables.prefix_parts as tables_parts
from usage
         left join tables
                   on tables.bucket = usage.bucket and tables.prefix_parts[1] = usage.prefix_parts[1]
                       and (tables.prefix_parts[2] = usage.prefix_parts[2] or tables.prefix_parts[2] is null)
                       and (tables.prefix_parts[3] = usage.prefix_parts[3] or tables.prefix_parts[3] is null)
                       and (tables.prefix_parts[4] = usage.prefix_parts[4] or tables.prefix_parts[4] is null)
                       and (tables.prefix_parts[5] = usage.prefix_parts[5] or tables.prefix_parts[5] is null)
                       and (tables.prefix_parts[6] = usage.prefix_parts[6] or tables.prefix_parts[6] is null)
                       and (tables.prefix_parts[7] = usage.prefix_parts[7] or tables.prefix_parts[7] is null)
                       and (tables.prefix_parts[8] = usage.prefix_parts[8] or tables.prefix_parts[8] is null)
                       and (tables.prefix_parts[9] = usage.prefix_parts[9] or tables.prefix_parts[9] is null)
                       and (tables.prefix_parts[10] = usage.prefix_parts[10] or tables.prefix_parts[10] is null)
                       and (tables.prefix_parts[11] = usage.prefix_parts[11] or tables.prefix_parts[11] is null)
                       and (tables.prefix_parts[12] = usage.prefix_parts[12] or tables.prefix_parts[12] is null)
                       and (tables.prefix_parts[13] = usage.prefix_parts[13] or tables.prefix_parts[13] is null)
                       and (tables.prefix_parts[14] = usage.prefix_parts[14] or tables.prefix_parts[14] is null)
                       and (tables.prefix_parts[15] = usage.prefix_parts[15] or tables.prefix_parts[15] is null)
                       and (tables.prefix_parts[16] = usage.prefix_parts[16] or tables.prefix_parts[16] is null)
                       and (tables.prefix_parts[17] = usage.prefix_parts[17] or tables.prefix_parts[17] is null)
                       and (tables.prefix_parts[18] = usage.prefix_parts[18] or tables.prefix_parts[18] is null)
                       and (tables.prefix_parts[19] = usage.prefix_parts[19] or tables.prefix_parts[19] is null)
                       and (tables.prefix_parts[20] = usage.prefix_parts[20] or tables.prefix_parts[20] is null)
