{{ config(
    schema='support',
    materialized='incremental',
    partition_by=['partition_date'],
    file_format='delta',
    meta = {
      'team': 'clan',
      'bigquery_load': 'true',
      'bigquery_partitioning_date_column': 'partition_date',
      'bigquery_fail_on_missing_partitions': 'false'
    }
) }}

WITH users_with_first_order AS
                              (
                               SELECT user_id,
                                      MIN(created_time_utc) AS first_order_created_time_msk
                               FROM mart.fact_order_2020
                               GROUP BY 1
                              ),
                              
     ttfr AS
            (
             WITH t AS (
                        SELECT t.payload.ticketId AS ticket_id,
                               MIN(t.event_ts_msk) AS first_reply
                        FROM mart.babylone_events AS t
                        WHERE NOT t.payload.isAnnouncement
                                  AND t.`type` = 'ticketEntryAddJoom'
                                  AND t.payload.authorType != 'customer'
                        GROUP BY 1
                       )
             SELECT t.*, a.payload.authorType AS ttfr_author_type
             FROM t
             JOIN mart.babylone_events AS a ON a.payload.ticketId = t.ticket_id
                                                    AND a.event_ts_msk = t.first_reply
                                                    AND a.payload.authorType != 'customer'
                                                    AND a.`type` = 'ticketEntryAddJoom'
            )

SELECT t.partition_date AS partition_date, --date of creation of ticket
       t.event_ts_msk AS creation_ticket_ts_msk,
       t.payload.ticketId AS ticket_id,
       t.payload.customerExternalId AS user_id,
       t.payload.country AS country,
       t.payload.messageSource AS os,
       CASE WHEN a.first_order_created_time_msk IS NULL THEN 'no' ELSE 'yes' END AS has_success_payments,
       b.event_ts_msk AS resolution_ticket_ts_msk,
       t.payload.lang AS lang,
       (UNIX_SECONDS(TIMESTAMP(c.first_reply)) - UNIX_SECONDS(TIMESTAMP(t.event_ts_msk))) / 3600 AS ttfr,
       c.ttfr_author_type AS ttfr_author_type,
       c.first_reply,
       CASE WHEN b.event_ts_msk IS NULL THEN 'no' ELSE 'yes' END AS is_closed
FROM mart.babylone_events AS t
LEFT JOIN users_with_first_order AS a ON a.user_id = t.payload.customerExternalId
                                         AND t.event_ts_msk >= a.first_order_created_time_msk
LEFT JOIN mart.babylone_events AS b ON b.payload.ticketId = t.payload.ticketId
                                       AND b.payload.stateOwner IN ('Resolved', 'Rejected')
                                       AND b.`type` = 'ticketChangeJoom'
LEFT JOIN ttfr AS c ON c.ticket_id = t.payload.ticketId
WHERE t.`type` = 'ticketCreateJoom'
