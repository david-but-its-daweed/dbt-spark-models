{{ config(
    meta = {
      'model_owner' : '@analytics.duty'
    },
    schema='platform',
    materialized='view',
) }}

with data as (select parts,
                     storage_class,
                     size,
                     `date`,
                     bucket,
                     `table`.db                        as db,
                     `table`.table_name                as table_name,
                     size(parts)                       as full_path_length,
                     explode(sequence(1, size(parts))) as path_length
              from {{ source("platform", "s3_inventory_with_tables")}}),

data_2 as (select array_join(slice(parts, 1, path_length), '/') as prefix,
                  storage_class,
                  size,
                  date,
                  bucket,
                  db,
                  table_name,
                  path_length,
                  full_path_length = path_length                as is_file
           from data)

select prefix,
       storage_class,
       date,
       bucket,
       db,
       table_name,
       path_length,
       is_file,
       sum(size) as size,
       count(1)  as file_count
from data_2
group by prefix,
    storage_class,
    date,
    bucket,
    db,
    table_name,
    path_length,
    is_file
