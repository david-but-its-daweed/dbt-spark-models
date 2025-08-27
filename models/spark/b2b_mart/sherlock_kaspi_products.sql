{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@daweed',
      'bigquery_load': 'true',
    }
) }}

WITH pools AS (
    SELECT
        _id AS pool_id,
        DATE(FROM_UNIXTIME(createdtime / 1000)) AS created_date,
        FROM_UNIXTIME(createdtime / 1000) AS created_time,
        authoremail AS email,
        name AS pool_name,
        processedtaskcount AS processed_task_cnt,
        taskcount AS task_cnt
    FROM {{ source('mongo', 'b2b_sherlock_sherlock_pools_daily_snapshot') }}
    WHERE
        country = 'KZ'
        AND DATE(FROM_UNIXTIME(createdtime / 1000)) >= '2025-06-24'
        AND authoremail IN ('daweed@joom.com', 'master@joom.com')
),

joompro_products AS (
    SELECT
        _id AS product_id,
        m1688.sherlockhistory[0].poolid AS pool_id,
        m1688.sherlockhistory[0].taskid AS task_id
    FROM {{ source('mongo', 'b2b_product_product_appendixes_daily_snapshot') }}
    WHERE
<<<<<<< HEAD
        m1688.sherlockhistory[0].poolid IS NOT NULL 
        AND m1688.sherlockhistory[0].taskid IS NOT NULL
=======
        m1688.sherlockhistory[0].poolid IS NOT NULL AND m1688.sherlockhistory[0].taskid IS NOT NULL
>>>>>>> Creating kaspi products table
)

SELECT DISTINCT
    DATE(FROM_UNIXTIME(ss.createdtime / 1000)) AS created_date,
    DATE(FROM_UNIXTIME(ss.updatedtime / 1000)) AS updated_date,

    FROM_UNIXTIME(ss.createdtime / 1000) AS created_time,
    FROM_UNIXTIME(ss.updatedtime / 1000) AS updated_time,

    ss.poolid AS pool_id,
    p.pool_name,

    ss._id AS task_id,
    ss.state AS task_status,
    jp.product_id IS NULL AS is_uploaded,

    p.processed_task_cnt,
    p.task_cnt,

    ss.externaltaskid AS kaspi_product_id,
    jp.product_id AS joompro_product_id
FROM {{ source('mongo', 'b2b_sherlock_sherlock_tasks_daily_snapshot') }} AS ss
INNER JOIN pools AS p
    ON ss.poolid = p.pool_id
LEFT JOIN joompro_products AS jp
    ON
        ss._id = jp.task_id
        AND ss.poolid = jp.pool_id