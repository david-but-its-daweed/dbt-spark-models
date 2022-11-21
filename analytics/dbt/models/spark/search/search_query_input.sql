{{ config(
    schema='search',
    materialized='incremental',
    partition_by=['partition_date'],
    file_format='delta',
    meta = {
      'team': 'search',
      'bigquery_load': 'true',
      'bigquery_partitioning_date_column': 'partition_date',
      'bigquery_fail_on_missing_partitions': 'false'
    }
) }}
SELECT
    partition_date,
    device_id,
    user_id,
    timestamp(millis_to_ts(event_ts)) as event_ts_utc,
    timestamp(millis_to_ts_msk(event_ts)) as event_ts_msk,
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
                    LOWER(ELEMENT_AT(payload.searchInputActions.source, -1)) in ('deeplink', 'deeplinksuggestion', 'deeplink_suggestion'),
                    'deeplink',
                    'manual'
                )
            )
        )
    ) AS suggestion_type,
    payload.searchInputActions AS search_input_actions
FROM {{ source('mart', 'device_events') }}
WHERE
    `type` in ('searchQueryInput')
{% if is_incremental() %}
    AND partition_date >= DATE'{{ var("start_date_ymd") }}'
    AND partition_date < DATE'{{ var("end_date_ymd") }}'
{% else %}
    AND partition_date >= DATE'2021-01-01'
{% endif %}