{{ config(
     schema='support',
     materialized='table',
     partition_by=['partition_date'],
     file_format='delta',
     meta = {
       'team': 'analytics',
       'bigquery_load': 'true',
       'bigquery_overwrite': 'true',
       'bigquery_partitioning_date_column': 'partition_date',
       'alerts_channel': "#olc_dbt_alerts",
       'bigquery_fail_on_missing_partitions': 'false'
     }
 ) }}
 
 WITH created_tickets AS (
     SELECT
         t.payload.ticketId AS ticket_id,
         MAX(t.event_ts_msk) AS ts_created,
         MAX(t.partition_date) AS partition_date,
         MAX(t.payload.deviceId) AS device_id,
         MAX(t.payload.customerExternalId) AS user_id,
         MAX(t.payload.lang) AS language,
         MAX(t.payload.country) AS country,
         MAX(t.payload.messageSource) AS os,
         MAX(t.payload.isHidden) AS is_hidden
     FROM {{ source('mart', 'babylone_events') }} AS t
     WHERE t.`type` IN ('ticketCreateJoom', 'ticketCreate')
         AND t.partition_date >= '2022-01-01'
     GROUP BY 1
    )

SELECT
    a.payload.ticketId AS ticket_id,
    t.language,
    t.country,
    t.is_hidden,
    t.partition_date,
    t.ts_created AS event_ts_creation,
    a.event_ts_msk AS event_ts_transfer_to_queue,
    b.name AS queue
FROM {{ source('mart', 'babylone_events') }} AS a
LEFT JOIN created_tickets AS t ON t.ticket_id = a.payload.ticketId
JOIN {{ source('mongo', 'babylone_joom_queues_daily_snapshot') }} AS b 
    ON a.payload.stateQueueId = b._id
WHERE a.`type` = 'ticketChangeJoom'
      AND a.partition_date >= '2022-01-01'
      AND a.payload.stateOwner = 'Queue'
ORDER BY 1, 7
