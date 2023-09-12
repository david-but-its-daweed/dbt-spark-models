{{ config(
    schema='customer_routing',
    incremental_strategy='insert_overwrite',
    materialized='incremental',
    file_format='parquet',
    partition_by=['partition_date_msk'],
    meta = {
      'model_owner' : '@ekutynina',
      'team': 'clan',
      'bigquery_load': 'true',
      'bigquery_partitioning_date_column': 'partition_date_msk',
      'bigquery_fail_on_missing_partitions': 'false'
    }
) }}


 SELECT DISTINCT
    device_id,
    real_user_id,
    partition_date AS partition_date_msk,
    FIRST(page_type) OVER (PARTITION BY device_id, DATE(event_ts_utc + INTERVAL 3 HOURS) ORDER BY event_ts_utc + INTERVAL 3 HOURS) AS first_page_type, 
    FIRST(source) OVER (PARTITION BY device_id, DATE(event_ts_utc + INTERVAL 3 HOURS) ORDER BY event_ts_utc + INTERVAL 3 HOURS) AS first_source,
    FIRST(medium) OVER (PARTITION BY device_id, DATE(event_ts_utc + INTERVAL 3 HOURS) ORDER BY event_ts_utc + INTERVAL 3 HOURS) AS first_medium,   
    FIRST(campaign) OVER (PARTITION BY device_id, DATE(event_ts_utc + INTERVAL 3 HOURS) ORDER BY event_ts_utc + INTERVAL 3 HOURS) AS first_campaign,  
    FIRST(campaign_type) OVER (PARTITION BY device_id, DATE(event_ts_utc + INTERVAL 3 HOURS) ORDER BY event_ts_utc + INTERVAL 3 HOURS) AS first_campaign_type
FROM  {{ source('ads', 'web_analytics_pageviews_with_segments') }} 
WHERE LOWER(os) LIKE "%web%"
    {% if is_incremental() %}
        AND partition_date >= date'{{ var("start_date_ymd") }}'
        AND partition_date < date'{{ var("end_date_ymd") }}'
    {% else %}
        AND partition_date >= date'2022-06-01'
    {% endif %}

