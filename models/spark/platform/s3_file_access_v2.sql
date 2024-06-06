{{ config(
    meta = {
      'model_owner' : '@analytics.duty'
    },
    schema='platform',
    materialized='view',
) }}

with data as (select parts,
                     accesses,
                     date,
                     bucket,
                     operation,
                     `table`.db                        as db,
                     `table`.table_name                as table_name,
                     size(parts)                       as full_path_length,
                     explode(sequence(1, size(parts))) as path_length
              from {{ source("platform", "s3_accesses_with_tables")}} ),

     data_2 as (select array_join(slice(parts, 1, path_length), '/') as prefix,
                       accesses,
                       date,
                       bucket,
                       db,
                       table_name,
                       CASE
                           WHEN operation like 'REST.COPY.OBJECT_GET' then 'undefined'
                           WHEN operation like '%_GET' then 'undefined'
                           WHEN operation like 'REST.POST.MULTI_OBJECT_DELETE' then 'undefined'
                           WHEN operation like 'REST.GET.UPLOADS' then 'EUC1-Requests-Tier1'
                           WHEN operation like 'REST.PUT.%' then 'EUC1-Requests-Tier1'
                           WHEN operation like 'REST.COPY.%' then 'EUC1-Requests-Tier1'
                           WHEN operation like 'REST.POST.%' then 'EUC1-Requests-Tier1'
                           WHEN operation like 'REST.LIST.%' then 'EUC1-Requests-Tier1'
                           WHEN operation like 'REST.GET.BUCKET' then 'EUC1-Requests-Tier1'
                           WHEN operation like 'REST.GET.%' then 'EUC1-Requests-Tier2'
                           WHEN operation like 'REST.HEAD.OBJECT' then 'EUC1-Requests-Tier2'
                           ELSE 'undefined'
                           end as usage_type,
                       path_length,
                       full_path_length = path_length as is_file
                from data)

select prefix,
       date,
       bucket,
       db,
       table_name,
       usage_type,
       path_length,
       is_file,
       sum(accesses) as accesses
from data_2
group by prefix,
         date,
         bucket,
         db,
         table_name,
         usage_type,
         path_length,
         is_file
