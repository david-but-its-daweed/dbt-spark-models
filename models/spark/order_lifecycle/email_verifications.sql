{{ config(
    schema='support',
    incremental_strategy='insert_overwrite',
    materialized='incremental',
    file_format='parquet',
    partition_by=['partition_date'],
    meta = {
       'team': 'analytics',
       'bigquery_load': 'true',
       'bigquery_overwrite': 'true',
       'bigquery_partitioning_date_column': 'partition_date',
       'alerts_channel': "#olc_dbt_alerts",
       'bigquery_fail_on_missing_partitions': 'false'
    }
) }}

WITH creation_timestamp AS (
   SELECT userId,
          MIN(createdTime) AS createdTime,
          MAX(createdTime) AS max_createdTime
   FROM mongo.user_email_credentials_daily_snapshot
   GROUP BY 1
),

final AS (
SELECT
   t.*,
   a.confirmed
FROM creation_timestamp AS t
LEFT JOIN mongo.user_email_credentials_daily_snapshot AS a ON t.userId = a.userId
                                                        AND t.max_createdTime = a.createdTime
)

SELECT
   t.*,
   CURRENT_DATE() AS partition_date
FROM final AS t
