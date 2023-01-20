{{ config(
    schema='engagement',
    materialized='incremental',
    partition_by=['partition_date_msk'],
    file_format='delta',
    meta = {
      'team': 'clan',
      'bigquery_load': 'true',
      'bigquery_partitioning_date_column': 'partition_date_msk',
      'bigquery_fail_on_missing_partitions': 'false'
    }
) }}
SELECT
    device_id AS device_id,
    partition_date as partition_date_msk,
    type as event_type,
    count(*) as count
FROM {{ source('mart', 'device_events') }}
WHERE
    `type` IN (
        'orderParcelOpen', 'supportChatOpen', 'pushEnabled', 'pushPermissionOpen')
        
{% if is_incremental() %}
    AND partition_date >= date'{{ var("start_date_ymd") }}'
    AND partition_date < date'{{ var("end_date_ymd") }}'
{% else %}
    AND partition_date >= date'2020-01-01'
{% endif %}
GROUP BY 1, 2, 3
ORDER BY 2 ASC
