{{ config(
     schema='support',
     materialized='incremental',
     partition_by=['partition_date'],
     file_format='delta',
     meta = {
       'team': 'analytics',
       'bigquery_load': 'true',
       'bigquery_overwrite': 'true',
       'bigquery_partitioning_date_column': 'partition_date'
     }
 ) }}


WITH users_with_first_order AS
                              (
                               SELECT user_id,
                                      MIN(created_time_utc) AS first_order_created_time_msk
                               FROM {{ source('mart', 'fact_order_2020') }})
                               GROUP BY 1
                              ),
                              
                              
                              ticket_create_events AS (

  SELECT t.event_ts_msk AS ts_created,
    t.partition_date AS partition_date,
    t.payload.ticketId AS ticket_id,
    t.payload.customerExternalId AS user_id,
    t.payload.lang AS language,
    t.payload.country AS country,
    t.payload.messageSource AS os
  FROM {{ source('mart', 'babylone_events') }}) AS t
  WHERE t.`type` = 'ticketCreateJoom'

),

 ticket_entry_add AS (

SELECT partition_date,
      event_ts_msk,
      ticket_id,
      author_id,
      author_type,
      entry_id,
      entry_type,
      MAX(event_ts_msk) OVER(PARTITION BY author_id, ticket_id, partition_date ORDER BY event_ts_msk ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ts,
      MIN(event_ts_msk) OVER(PARTITION BY author_id, ticket_id, partition_date ORDER BY event_ts_msk ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ts     
FROM (

  SELECT t.partition_date AS partition_date,
      t.event_ts_msk,
      t.payload.ticketId AS ticket_id,
      t.payload.authorId AS author_id,
      t.payload.authorType AS author_type,
      t.payload.entryId AS entry_id,
      t.payload.entryType AS entry_type
  FROM {{ source('mart', 'babylone_events') }}) AS t
  WHERE NOT t.payload.isAnnouncement
            AND t.`type` = 'ticketEntryAddJoom'

)),
    
first_entries AS (

    SELECT event_ts_msk,
      ticket_id,
      author_type,
      entry_id,
      FIRST_VALUE(event_ts_msk) OVER(PARTITION BY ticket_id, author_type ORDER BY event_ts_msk ASC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_entry_ts  
    FROM ticket_entry_add
    WHERE entry_type != 'privateNote'
      AND author_id != '000000000000050001000001'
      

), 

  messages_first_replies AS (
  SELECT ticket_id,
         MIN(IF(author_type != 'customer', first_entry_ts, NULL)) AS ts_first_replied
  FROM first_entries
  GROUP BY 1
  ),

ttfr AS (
SELECT tickets.ticket_id AS ticket_id,
       tickets.country AS country,
       partition_date,
       ((UNIX_SECONDS(TIMESTAMP(ts_first_replied)) - UNIX_SECONDS(TIMESTAMP(ts_created))) / 3600) as ttfr
FROM ticket_create_events as tickets
LEFT JOIN messages_first_replies
  ON tickets.ticket_id = messages_first_replies.ticket_id
     ),
     
ttfr_author_type AS (
                     SELECT DISTINCT ticket_id AS ticket_id,
                            FIRST_VALUE(author_type) OVER(PARTITION BY ticket_id ORDER BY event_ts_msk) AS ttfr_author_type  
                     FROM ticket_entry_add
                     WHERE entry_type != 'privateNote'
                           AND author_id != '000000000000050001000001'
                           AND author_type != 'customer'
                    ),
            
    first_queue AS (
                    SELECT DISTINCT(t.payload.ticketId) AS ticket_id,
                           FIRST_VALUE(a.name) OVER(PARTITION BY t.payload.ticketId ORDER BY t.event_ts_msk ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_queue
                    FROM {{ source('mart', 'babylone_events') }}) AS t
                    JOIN mongo.babylone_joom_queues_daily_snapshot AS a 
                         ON t.payload.stateQueueId = a._id
                    WHERE t.`type` = 'ticketChangeJoom'
                          AND t.payload.stateQueueId IS NOT NULL
                          
                   ),
                   
    all_queues AS (
                    WITH t AS (
                               SELECT DISTINCT(t.payload.ticketId) AS ticket_id,
                                      a.name AS queue
                               FROM {{ source('mart', 'babylone_events') }}) AS t
                               JOIN mongo.babylone_joom_queues_daily_snapshot AS a 
                                    ON t.payload.stateQueueId = a._id
                               WHERE t.`type` = 'ticketChangeJoom'
                                     AND t.payload.stateQueueId IS NOT NULL
                                )
                    SELECT t.ticket_id AS ticket_id,
                           COLLECT_LIST(DISTINCT t.queue) AS queues
                    FROM t AS t
                    GROUP BY 1
                   ),
    
    all_tags AS (
                 WITH t AS (
                            SELECT t.payload.ticketId AS ticket_id,
                                   EXPLODE(t.payload.tagIds) AS tag
                            FROM {{ source('mart', 'babylone_events') }}) AS t
                            WHERE t.payload.tagIds IS NOT NULL
                                  AND t.`type` IN ('ticketCreateJoom', 'ticketChangeJoom')
                            ),
           
                     base AS (
                              SELECT t.ticket_id AS ticket_id,
                                     a.name AS tag
                              FROM t AS t
                              JOIN mongo.babylone_joom_tags_daily_snapshot AS a ON t.tag = a._id
                              )
         
                SELECT t.ticket_id AS ticket_id,
                      COLLECT_LIST(DISTINCT t.tag) AS tags
                FROM base AS t
                GROUP BY 1
                ),
                
    all_parcels AS (
                 WITH t AS (
                            SELECT t.payload.ticketId AS ticket_id,
                                   EXPLODE(t.payload.parcelIds) AS parcelId
                            FROM {{ source('mart', 'babylone_events') }} AS t
                            WHERE t.payload.tagIds IS NOT NULL
                                  AND t.`type` IN ('ticketCreateJoom', 'ticketChangeJoom')
                            )
                SELECT t.ticket_id AS ticket_id,
                      COLLECT_LIST(DISTINCT t.parcelId) AS parcelIds
                FROM t AS t
                GROUP BY 1
                ),
                
    all_orders AS (
                 WITH t AS (
                            SELECT t.payload.ticketId AS ticket_id,
                                   EXPLODE(t.payload.orderIds) AS orderId
                            FROM {{ source('mart', 'babylone_events') }} AS t
                            WHERE t.payload.tagIds IS NOT NULL
                                  AND t.`type` IN ('ticketCreateJoom', 'ticketChangeJoom')
                            )
                SELECT t.ticket_id AS ticket_id,
                       COLLECT_LIST(DISTINCT t.orderId) AS orderIds
                FROM t AS t
                GROUP BY 1
                ),
                
    all_agents AS (
                   WITH t AS (
                              SELECT DISTINCT t.payload.ticketId AS ticket_id,
                                     t.payload.authorId AS author_id
                              FROM  {{ source('mart', 'babylone_events') }} AS t
                              WHERE t.payload.authorType = 'agent'
                                    AND t.`type` = 'ticketEntryAdd'
                             )
                             
                    SELECT t.ticket_id AS ticket_id,
                           COLLECT_LIST(DISTINCT t.author_id) AS agentIds
                    FROM t AS t
                    WHERE t.author_id != '000000000000050001000001'
                    GROUP BY 1
                  ),
                  
    responses AS (

 WITH ranking AS (
                                   SELECT t.payload.ticketId AS ticket_id,
                                          t.event_ts_msk AS event_ts_msk,
                                          CASE WHEN t.payload.authorType != 'customer' THEN 'support' ELSE 'customer' END AS author_type,
                                          ROW_NUMBER() OVER (PARTITION BY t.payload.ticketId ORDER BY t.event_ts_msk) AS num
                                   FROM {{ source('mart', 'babylone_events') }} AS t
                                   JOIN {{ source('mart', 'babylone_events') }} AS a ON a.payload.ticketId = t.payload.ticketId
                                                                     AND a.`type` = 'ticketCreateJoom'
                                   WHERE t.payload.entryType = 'message'
                                         AND t.`type` = 'ticketEntryAddJoom'
                                  ),
                                  
        clear_ranking (SELECT t.*
                       FROM ranking AS t
                       JOIN ranking AS a ON a.ticket_id = t.ticket_id
                                            AND a.num = (t.num - 1)
                                            AND a.author_type != t.author_type
                      ),
                                            
        base AS (
                 SELECT a.*
                 FROM clear_ranking AS a
                 UNION ALL
                          SELECT t.*
                          FROM ranking AS t
                          WHERE t.num = 1),
                          
        responses_to_support AS (
                  SELECT t.ticket_id, COUNT(t.author_type) AS responses_to_support
                  FROM base AS t
                  WHERE t.author_type = 'customer'
                  GROUP BY 1
                  ),
                  
         responses_to_customer AS (
                  SELECT t.ticket_id, COUNT(t.author_type) AS responses_to_customer
                  FROM base AS t
                  WHERE t.author_type = 'support'
                  GROUP BY 1
                  )
                  
                   SELECT COALESCE(t.ticket_id, a.ticket_id) AS ticket_id,
                          COALESCE(t.responses_to_support, 0) AS responses_to_support,
                          COALESCE(a.responses_to_customer, 0) AS responses_to_customer
                   FROM responses_to_support AS t
                   LEFT JOIN responses_to_customer AS a ON a.ticket_id = t.ticket_id
                  )
                  
                  ,
                 
        csat AS (
                 SELECT t.payload.ticketId AS ticket_id,
                        FIRST_VALUE(t.payload.selectedOptionsIds[0]) OVER(PARTITION BY t.payload.ticketId ORDER BY t.event_ts_msk ASC) AS csat
                 FROM {{ source('mart', 'babylone_events') }} AS t
                 WHERE t.`type` = 'babyloneWidgetAction'
                       AND t.payload.widgetType = 'did_we_help'
                       AND t.payload.selectedOptionsIds[0] IS NOT NULL
                ),
                
                
        resolution AS (
                       SELECT t.payload.ticketId AS ticket_id,
                              MIN(t.event_ts_msk) AS resolution_ticket_ts_msk
                       FROM {{ source('mart', 'babylone_events') }} AS t
                       WHERE t.payload.stateOwner IN ('Resolved', 'Rejected')
                             AND t.`type` = 'ticketChangeJoom'
                       GROUP BY 1),
                       
        button_place AS (
                      SELECT DISTINCT(t.payload.ticketId) AS ticket_id,
                              FIRST_VALUE(t.payload.buttonPlace) OVER(PARTITION BY t.payload.ticketId ORDER BY t.event_ts_msk ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS button_place
                      FROM {{ source('mart', 'babylone_events') }} AS t
                      WHERE t.`type` = 'ticketEntryAddJoom'
                            AND t.payload.authorType = 'customer'
                            AND t.payload.buttonPlace IS NOT NULL
                     )
                
SELECT t.partition_date AS partition_date,
       t.ts_created AS creation_ticket_ts_msk,
       t.ticket_id AS ticket_id,
       t.user_id AS user_id,
       t.country AS country,
       n.button_place AS button_place,
       --device_id ждёт https://joom.myjetbrains.com/youtrack/issue/SUPPORT-2861
       t.os AS os, --но вообще бы дёргать нормальную ось (тут проблемы с INTERNAL, присваиваемым ко всем скрытым тикетам)
       CASE WHEN a.first_order_created_time_msk IS NULL THEN 'no' ELSE 'yes' END AS has_success_payments,
       b.resolution_ticket_ts_msk AS resolution_ticket_ts_msk,
       t.language AS language,
       c.ttfr,
       m.ttfr_author_type AS ttfr_author_type,
       CASE WHEN b.resolution_ticket_ts_msk IS NULL THEN 'no' ELSE 'yes' END AS is_closed,
       d.first_queue,
       f.queues,
       e.tags,
       g.parcelIds,
       h.orderIds,
       i.agentIds,
       COALESCE(k.responses_to_support, 0) AS responses_to_support,
       COALESCE(k.responses_to_customer, 0) AS responses_to_customer,
       l.csat AS csat
FROM ticket_create_events AS t
LEFT JOIN users_with_first_order AS a ON a.user_id = t.user_id
                                         AND t.ts_created >= a.first_order_created_time_msk
LEFT JOIN resolution AS b ON b.ticket_id = t.ticket_id
LEFT JOIN ttfr AS c ON c.ticket_id = t.ticket_id
LEFT JOIN first_queue AS d ON d.ticket_id = t.ticket_id
LEFT JOIN all_tags AS e ON e.ticket_id = t.ticket_id
LEFT JOIN all_queues AS f ON f.ticket_id = t.ticket_id
LEFT JOIN all_parcels AS g ON g.ticket_id = t.ticket_id
LEFT JOIN all_orders AS h ON h.ticket_id = t.ticket_id
LEFT JOIN all_agents AS i ON i.ticket_id = t.ticket_id
LEFT JOIN responses AS k ON k.ticket_id = t.ticket_id
LEFT JOIN csat AS l ON l.ticket_id = t.ticket_id
LEFT JOIN ttfr_author_type AS m ON m.ticket_id = t.ticket_id
LEFT JOIN button_place AS n ON n.ticket_id = t.ticket_id
