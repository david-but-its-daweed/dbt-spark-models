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
    'spark' AS type,
    'manual' AS creatin_type
FROM platform.manual_tables_seed
WHERE table_name IS NOT NULL

UNION ALL
SELECT
    SPLIT(table_name, '[.]')[0] AS db,
    SPLIT(table_name, '[.]')[1] AS table_name,
    table_name AS full_table_name,
    'bq' AS type,
    'manual' AS creatin_type
FROM platform.manual_tables_bq_seed
WHERE table_name IS NOT NULL

UNION ALL

SELECT
    SPLIT(`table`.tableName, '[.]')[0] AS db,
    SPLIT(`table`.tableName, '[.]')[1] AS table_name,
    `table`.tableName AS full_table_name,
    `table`.tableType AS type,
    'seed' AS creatin_type
FROM platform.table_producers
WHERE
    1 = 1
    AND `table`.isSeed
    AND partition_date = (
        SELECT MAX(tp.partition_date)
        FROM platform.table_producers AS tp
        WHERE tp.partition_date >= CURRENT_DATE - INTERVAL 3 DAYS
    )
