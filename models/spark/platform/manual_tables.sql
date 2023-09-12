{{ config(
    meta = {
      'model_owner' : '@gburg'
    },
    schema='platform',
    materialized='view',
) }}

select split(table_name, '[.]')[0] as db,
       split(table_name, '[.]')[1] as table_name,
       table_name                as full_table_name,
       'spark'                   as type
from platform.manual_tables_seed
union all
select split(table_name, '[.]')[0] as db,
       split(table_name, '[.]')[1] as table_name,
       table_name                as full_table_name,
       'bq'                      as type
from platform.manual_tables_bq_seed

