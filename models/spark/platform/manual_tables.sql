{{ config(
    meta = {
      'model_owner' : '@analytics.duty'
    },
    schema='platform',
    materialized='table',
) }}

WITH table_producers AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY `table`.tableName, `table`.tableType ORDER BY partition_date DESC) AS rn
    FROM
        {{ source('platform', 'table_producers') }}
    WHERE
        partition_date >= CURRENT_DATE - INTERVAL 3 DAYS
)

SELECT
    SPLIT(`table`.tableName, '[.]')[0] AS db,
    SPLIT(`table`.tableName, '[.]')[1] AS table_name,
    `table`.tableName AS full_table_name,
    `table`.tableType AS type,
    'seed' AS creating_type
FROM table_producers
WHERE
    rn = 1
    AND `table`.isSeed

UNION ALL

SELECT
    SPLIT(table_name, '[.]')[0] AS db,
    SPLIT(table_name, '[.]')[1] AS table_name,
    table_name AS full_table_name,
    'spark' AS type,
    'manual' AS creating_type
FROM {{ ref("manual_tables_seed") }}
WHERE table_name IS NOT NULL

UNION ALL
SELECT
    SPLIT(table_name, '[.]')[0] AS db,
    SPLIT(table_name, '[.]')[1] AS table_name,
    table_name AS full_table_name,
    'bq' AS type,
    'manual' AS creating_type
FROM {{ source('platform', 'manual_tables_bq_seed') }}
WHERE table_name IS NOT NULL
