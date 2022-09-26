{{ config(
     schema='support',
     materialized='table',
     partition_by=['partition_date'],
     file_format='delta',
     meta = {
       'team': 'analytics',
       'bigquery_load': 'true',
       'bigquery_overwrite': 'true',
       'bigquery_partitioning_date_column': 'partition_date'
     }
 ) }}

WITH csat_trigger AS 
                            (
                             SELECT t.partition_date,
                                    t.payload.ticketId AS ticket_id
                             FROM {{ source('mart', 'babylone_events') }} AS t
                             WHERE t.`type` = 'babyloneWidgetAction'
                                   AND t.partition_date > '2022-07-26' --csat was released 07/27
                                   AND t.payload.widgetType = 'did_we_help'
                                   AND t.payload.selectedOptionsIds[0] IS NULL
                             ),
                      
     flattened_tags AS (
                        SELECT t.partition_date,
                               t.ticket_id,
                               CASE WHEN t.agentIds IS NOT NULL THEN 'yes' ELSE 'no' END AS was_agent,
                               t.country,
                               t.language,
                               t.csat,
                               EXPLODE(t.tags) AS tag
                        FROM {{ source('support', 'support_mart_ticket_id_ext') }} AS t
                        WHERE t.partition_date >  '2022-07-26'
                              AND t.is_closed = 'yes'
                              )

                      
SELECT t.partition_date,
       t.ticket_id,
       CASE WHEN a.ticket_id IS NOT NULL THEN 'yes' ELSE 'no' END AS csat_was_triggered,
       t.was_agent,
       t.country,
       t.language,
       t.csat,
       t.tag
FROM flattened_tags AS t
LEFT JOIN csat_trigger AS a ON a.ticket_id = t.ticket_id
