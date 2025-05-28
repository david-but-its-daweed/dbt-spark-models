{{ config(
    schema = 'search',
    incremental_strategy='insert_overwrite',
    materialized='incremental',
    file_format = 'parquet',
    partition_by = ['date_msk'],
    meta = {
        'model_owner': '@itangaev',
        'team': 'search',
        'bigquery_load': 'true'
    }
) }}

WITH
search_extracts AS (
    SELECT
        device_id,
        DATE(FROM_UNIXTIME(event_ts / 1000)) AS event_date,
        payload.query AS text_query,
        ELEMENT_AT(
            FILTER(payload.queryFilters['categoryId'].categories, x -> x IS NOT NULL),
            1
        ) AS category_id_query,
        IF(type = 'search' AND text_query IS NOT NULL AND text_query <> '', 1, 0) AS is_text_search,
        IF(type = 'search' AND category_id_query IS NOT NULL, 1, 0) AS is_catalog_search,
        IF(type = 'search', 1, 0) AS is_any_search,
        IF(type = 'productOpenServer', 1, 0) AS is_open_product
    FROM
        {{ source('mart', 'device_events') }}
    WHERE
        type IN ('search')
        {% if is_incremental() %}
            AND partition_date >= DATE'{{ var("start_date_ymd") }}'
            AND partition_date < DATE'{{ var("end_date_ymd") }}'
        {% else %}
            AND partition_date >= CURRENT_DATE() - INTERVAL 90 DAYS
        {% endif %}
        AND DATE(FROM_UNIXTIME(event_ts / 1000)) >= partition_date
),

device_activity AS (
    SELECT
        device_id,
        event_date,
        IF(SUM(is_text_search) > 0, 1, 0) AS has_text_search,
        IF(SUM(is_catalog_search) > 0, 1, 0) AS has_catalog_search,
        IF(SUM(is_any_search) > 0, 1, 0) AS has_any_search,
        IF(SUM(is_open_product) > 0, 1, 0) AS has_open_product
    FROM search_extracts
    GROUP BY
        device_id, event_date
),

regions AS (
    SELECT
        country_code,
        region_name
    FROM {{ ref('gold_regions') }}
),

devices AS (
    SELECT *
    FROM {{ ref('gold_active_devices_with_ephemeral') }}
    WHERE
        {% if is_incremental() %}
            date_msk >= DATE'{{ var("start_date_ymd") }}'
            AND date_msk < DATE'{{ var("end_date_ymd") }}'
        {% else %}
            date_msk >= CURRENT_DATE() - INTERVAL 90 DAYS
        {% endif %}
)

SELECT
    devices.device_id,
    devices.date_msk,
    devices.platform,
    devices.app_entity,
    devices.country_code,
    IF(DATEDIFF(devices.date_msk, devices.join_date_msk) <= 30, 1, 0) AS is_new_device,
    devices.is_converted,
    devices.is_ephemeral_device,
    COALESCE(regions.region_name, 'Other') AS region_name,
    COALESCE(activity.has_text_search, 0) AS has_text_search,
    COALESCE(activity.has_catalog_search, 0) AS has_catalog_search,
    COALESCE(activity.has_any_search, 0) AS has_any_search,
    COALESCE(activity.has_open_product, 0) AS has_open_product
FROM devices
LEFT JOIN
    regions
    ON devices.country_code = regions.country_code
LEFT JOIN
    device_activity AS activity
    ON
        devices.device_id = activity.device_id
        AND devices.date_msk = activity.event_date