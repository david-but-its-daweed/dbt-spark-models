{{ config(
    schema='search',
    incremental_strategy='insert_overwrite',
    materialized='incremental',
    partition_by=['partition_date'],
    file_format='parquet',
    meta = {
      'model_owner' : '@itangaev',
      'team': 'search',
      'bigquery_load': 'true',
      'bigquery_partitioning_date_column': 'partition_date',
      'bigquery_fail_on_missing_partitions': 'false',
      'bigquery_check_counts': 'false'
    }
) }}
SELECT
    partition_date,
    device_id,
    user_id,
    TIMESTAMP(MILLIS_TO_TS(event_ts)) AS event_ts_utc,
    TIMESTAMP(MILLIS_TO_TS_MSK(event_ts)) AS event_ts_msk,
    LOWER(device_from_event.version.osType) AS os_type,
    payload.durationMs AS duration_ms,
    payload.enteredQuery AS entered_query,
    payload.initialQuery AS initial_query,
    payload.submittedQuery AS submitted_query,
    COALESCE(payload.position, payload.suggestPosition) AS position,
    COALESCE(payload.suggestCategory, payload.suggestedCategory.categoryId)
    AS suggest_category,
    payload.suggestedCategory.experiment AS suggest_category_catalog_version,
    payload.suggestClicked AS is_suggest_clicked,
    IF(
        payload.suggestClicked = TRUE
        OR LOWER(ELEMENT_AT(payload.searchInputActions.source, -1)) = 'suggestion',
        'suggestion',
        IF(
            LOWER(ELEMENT_AT(payload.searchInputActions.source, -1)) = 'recent',
            'recent',
            IF(
                LOWER(ELEMENT_AT(payload.searchInputActions.source, -1)) = 'popular',
                'popular',
                IF(
                    LOWER(ELEMENT_AT(payload.searchInputActions.source, -1)) IN ('deeplink', 'deeplinksuggestion', 'deeplink_suggestion'),
                    'deeplink',
                    'manual'
                )
            )
        )
    ) AS suggestion_type,
    payload.searchInputActions AS search_input_actions
FROM {{ source('mart', 'device_events') }}
WHERE
    `type` IN ('searchQueryInput')
    {% if is_incremental() %}
    AND partition_date >= DATE'{{ var("start_date_ymd") }}'
    AND partition_date < DATE'{{ var("end_date_ymd") }}'
{% else %}
        AND partition_date >= DATE '2024-01-01'
    {% endif %}