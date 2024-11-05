{{ config(
    meta = {
      'model_owner' : '@analytics.duty'
    },
    schema='platform',
    materialized='view',
) }}

SELECT
    SPLIT(table_name, '[.]')[0] AS db,
    SPLIT(table_name, '[.]')[1] AS table_name,
    table_name AS full_table_name,
    'spark' AS type
FROM platform.manual_tables_seed
WHERE table_name IS NOT NULL

UNION ALL
SELECT
    SPLIT(table_name, '[.]')[0] AS db,
    SPLIT(table_name, '[.]')[1] AS table_name,
    table_name AS full_table_name,
    'bq' AS type
FROM platform.manual_tables_bq_seed
WHERE table_name IS NOT NULL

