{{ config(
    schema='merchant',
    materialized='view',
    partition_by=['queued_date'],
    file_format='parquet',
    meta = {
      'team': 'merchant',
      'model_owner': '@abracadabrik',
      'bigquery_load': 'true',
      'bigquery_overwrite': 'true',
      'bigquery_partitioning_date_column': 'queued_date'
    },
) }}

WITH category_levels AS (

    SELECT
        category_id,
        level_1_category.name AS category_name
    FROM {{ source('mart', 'category_levels') }}

),

queued AS (

    SELECT
        payload['processId'] AS item_id,
        product_id,
        partition_date AS queued_date,
        event_ts_utc AS queued_ts
    FROM {{ source('mart', 'product_events') }}
    WHERE
        type = 'categorizationQueued'

),

canceled AS (

    SELECT
        payload['processId'] AS item_id,
        product_id,
        partition_date AS canceled_date,
        event_ts_utc AS canceled_ts
    FROM {{ source('mart', 'product_events') }}
    WHERE
        type = 'categorizationCanceled'

),

processed AS (

    SELECT
        payload['processId'] AS item_id,
        product_id,
        event_ts_utc AS processed_ts,
        payload['categoryId'] AS category_id,
        payload['moderatorId'] AS moderator_id
    FROM {{ source('mart', 'product_events') }}
    WHERE
        type = 'categorizationResult'

)

SELECT
    queued.item_id AS item_id,
    queued.product_id AS product_id,
    queued.queued_date AS queued_date,
    queued.queued_ts AS queued_ts,
    canceled.canceled_ts AS canceled_ts,
    processed.processed_ts AS processed_ts,
    CASE
        WHEN canceled.canceled_ts IS NOT NULL THEN 'canceled'
        WHEN processed.processed_ts IS NOT NULL THEN 'processed'
        ELSE 'queued'
    END AS processed_type,
    CASE
        WHEN canceled.canceled_ts IS NOT NULL THEN (UNIX_TIMESTAMP(canceled.canceled_ts) - UNIX_TIMESTAMP(queued.queued_ts)) / 3600
        WHEN processed.processed_ts IS NOT NULL THEN (UNIX_TIMESTAMP(processed.processed_ts) - UNIX_TIMESTAMP(queued.queued_ts)) / 3600
    END AS processed_time,
    CASE
        WHEN canceled.canceled_ts IS NOT NULL THEN DATE(canceled.canceled_ts)
        WHEN processed.processed_ts IS NOT NULL THEN DATE(processed.processed_ts)
        ELSE CURRENT_DATE() - 1
    END AS latest_date,
    processed.moderator_id AS moderator_id,
    processed.category_id AS category_id,
    category_levels.category_name AS category_name
FROM queued
LEFT JOIN canceled ON queued.item_id = canceled.item_id
LEFT JOIN processed ON queued.item_id = processed.item_id
LEFT JOIN category_levels ON processed.category_id = category_levels.category_id
