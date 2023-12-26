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
      'bigquery_partitioning_date_column': 'queued_date',
      'bigquery_known_gaps': ['2023-11-19', '2023-11-18']
    },
) }}

WITH category_levels AS (

    SELECT
        category_id,
        level_1_category.name AS category_name
    FROM {{ source('mart', 'category_levels') }}

),

product_categories AS (

    SELECT
        product_id,
        category_id
    FROM (
        SELECT
            product_id,
            category_id,
            effective_ts,
            FIRST(effective_ts) OVER (PARTITION BY product_id ORDER BY effective_ts DESC RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_effective_ts
        FROM {{ source('mart', 'dim_product') }}
    )
    WHERE
        effective_ts = latest_effective_ts
),

queued AS (

    SELECT
        product_events.payload['itemId'] AS item_id,
        product_events.product_id AS product_id,
        product_events.partition_date AS queued_date,
        product_events.event_ts_utc AS queued_ts,
        product_events.payload['productType'] AS product_type,
        product_events.payload.source AS source,
        product_events.payload.origin AS origin,
        category_levels.category_name AS category_name
    FROM {{ source('mart', 'product_events') }}
    LEFT JOIN product_categories ON product_events.product_id = product_categories.product_id
    LEFT JOIN category_levels ON product_categories.category_id = category_levels.category_id
    WHERE
        product_events.type = 'riskyModerationQueued'

),

canceled AS (

    SELECT
        payload['itemId'] AS item_id,
        product_id,
        partition_date AS canceled_date,
        event_ts_utc AS canceled_ts
    FROM {{ source('mart', 'product_events') }}
    WHERE
        type = 'riskyModerationCanceled'

),

processed AS (

    SELECT
        payload['itemId'] AS item_id,
        product_id,
        payload['moderatorId'] AS moderator_id,
        event_ts_utc AS processed_ts,
        payload['rejectReasons'] AS reject_reasons
    FROM {{ source('mart', 'product_events') }}
    WHERE
        type = 'riskyModerationResult'

)

SELECT
    queued.item_id AS item_id,
    queued.product_id AS product_id,
    queued.source AS source,
    queued.product_type AS product_type,
    queued.category_name AS category_name,
    queued.queued_date AS queued_date,
    queued.queued_ts AS queued_ts,
    canceled.canceled_ts AS canceled_ts,
    processed.processed_ts AS processed_ts,
    CASE
        WHEN canceled.canceled_ts IS NOT NULL THEN 'canceled'
        WHEN processed.processed_ts IS NOT NULL AND processed.reject_reasons IS NOT NULL THEN 'rejected'
        WHEN processed.processed_ts IS NOT NULL AND processed.reject_reasons IS NULL THEN 'approved'
        ELSE 'queued'
    END AS processed_type,
    CASE
        WHEN canceled.canceled_ts IS NOT NULL THEN (UNIX_TIMESTAMP(canceled.canceled_ts) - UNIX_TIMESTAMP(queued_ts)) / 3600
        WHEN processed.processed_ts IS NOT NULL THEN (UNIX_TIMESTAMP(processed.processed_ts) - UNIX_TIMESTAMP(queued_ts)) / 3600
    END AS processed_time,
    CASE
        WHEN canceled.canceled_ts IS NOT NULL THEN DATE(canceled.canceled_ts)
        WHEN processed.processed_ts IS NOT NULL THEN DATE(processed.processed_ts)
        ELSE CURRENT_DATE() - 1
    END AS latest_date,
    processed.moderator_id AS moderator_id,
    processed.reject_reasons AS reject_reasons,
    processed.reject_reasons[0]['code'] AS reject_reason
FROM queued
LEFT JOIN canceled ON queued.item_id = canceled.item_id
LEFT JOIN processed ON queued.item_id = processed.item_id
