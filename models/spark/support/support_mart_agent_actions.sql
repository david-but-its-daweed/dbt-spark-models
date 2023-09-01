{{ config(
     schema='support',
     materialized='table',
     location_root='s3://joom-analytics-mart/support/nonpartitioned/'
     file_format='delta',
     meta = {
       'team': 'analytics',
       'bigquery_load': 'true',
       'bigquery_overwrite': 'true',
       'alerts_channel': "#olc_dbt_alerts"
     }
 ) }}

--deleted entries to exclude from query
WITH
deletes AS (
    SELECT DISTINCT
        payload.entryId AS entry_id
    FROM {{ source('mart', 'babylone_events') }}
    WHERE 1=1 
        AND `type` = 'ticketEntryDeleted'
)
,

--agents' entries to exclude duplicates
ranking_pre_pre AS (
    SELECT 
          t.payload.ticketId AS ticket_id,
          t.payload.authorId AS author_id,
          b.email,
          t.payload.authorType AS author_type,
          t.payload.entryType AS entry_type,
          t.payload.text AS text,
          MIN(t.partition_date) AS partition_date,
          MIN(t.event_ts_msk) AS event_ts_msk
    FROM {{ source('mart', 'babylone_events') }} AS t
    LEFT JOIN deletes AS c ON t.payload.entryId = c.entry_id
    LEFT JOIN {{ source('mongo', 'babylone_joom_agents_daily_snapshot') }} AS b ON t.payload.authorId = b._id
    WHERE t.`type` = 'ticketEntryAddJoom'
          AND t.payload.authorType = 'agent'
          AND c.entry_id IS NULL
          AND t.payload.entryType = 'privateNote'
    GROUP BY 1, 2, 3, 4, 5, 6
    ORDER BY 1, 7, 8),
--agents' and customers' messages    
ranking_pre_pre_cust AS (
    SELECT 
          t.payload.ticketId AS ticket_id,
          t.payload.authorId AS author_id,
          b.email,
          t.payload.authorType AS author_type,
          t.payload.entryType AS entry_type,
          t.payload.text AS text,
          t.partition_date,
          t.event_ts_msk
    FROM {{ source('mart', 'babylone_events') }} AS t
    LEFT JOIN deletes AS c ON t.payload.entryId = c.entry_id
    LEFT JOIN {{ source('mongo', 'babylone_joom_agents_daily_snapshot') }} AS b ON t.payload.authorId = b._id
    WHERE t.`type` = 'ticketEntryAddJoom'
          AND (t.payload.authorType = 'customer' OR t.payload.entryType = 'message')
          AND c.entry_id IS NULL
    ORDER BY 1, 7, 8),
--all entries    
ranking_pre AS (
SELECT
    *
FROM ranking_pre_pre
UNION ALL
SELECT
    *
FROM ranking_pre_pre_cust
),
    
resolutions AS (
    SELECT 
          t.payload.ticketId AS ticket_id,
          t.payload.stateAgentId AS author_id,
          b.email,
          'Resolved' AS author_type,
          '-' AS entry_type,
          '-' AS text,
          MIN(t.partition_date) AS partition_date,
          MIN(t.event_ts_msk) AS event_ts_msk
    FROM {{ source('mart', 'babylone_events') }} AS t
    LEFT JOIN {{ source('mongo', 'babylone_joom_agents_daily_snapshot') }} AS b ON t.payload.stateAgentId = b._id
    WHERE t.`type` = 'ticketChangeJoom'
        AND t.payload.stateOwner IN ('Rejected', 'Resolved')
        AND t.payload.changedByType = 'agent'
    GROUP BY 1, 2, 3, 4, 5, 6
    ),
--all entries and closings    
final AS (
    SELECT
        partition_date,
        ticket_id,
        author_id,
        email,
        author_type,
        entry_type,
        text,
        event_ts_msk
    FROM ranking_pre
    UNION ALL
    SELECT
        DATE(partition_date),
        ticket_id,
        author_id,
        email,
        author_type,
        entry_type,
        text,
        event_ts_msk
    FROM resolutions
),

ordered_final_1 AS (
    SELECT
        *
    FROM final
    ORDER BY 2, 6
 ),

--previous entries for actions' conduct
pre_ranking AS (
SELECT
    partition_date,
    ticket_id,
    LAG(author_id) OVER (PARTITION BY ticket_id ORDER BY event_ts_msk) AS previous_author_id,
    author_id,
    LAG(author_type) OVER (PARTITION BY ticket_id ORDER BY event_ts_msk) AS previous_author_type,
    author_type,
    LAG(`text`) OVER (PARTITION BY ticket_id ORDER BY event_ts_msk) AS previous_text,
    `text`,
    LAG(event_ts_msk) OVER (PARTITION BY ticket_id ORDER BY event_ts_msk) AS previous_event_ts_msk,
    event_ts_msk,
    LAG(entry_type) OVER (PARTITION BY ticket_id ORDER BY event_ts_msk) AS previous_entry_type,
    entry_type
FROM ordered_final_1
ORDER BY 2, 9
),

agent_support_actions AS (
SELECT
    t.partition_date,
    t.ticket_id,
    t.previous_author_id,
    t.author_id,
    t.previous_author_type,
    t.author_type,
    t.previous_entry_type,
    t.entry_type,
    t.previous_event_ts_msk,
    t.event_ts_msk,
    t.previous_text,
    t.text,
    a.email AS email
FROM pre_ranking AS t
LEFT JOIN {{ source('mongo', 'babylone_joom_agents_daily_snapshot') }} AS a ON t.author_id = a._id
WHERE 1=1 AND
    ((t.previous_author_type = 'customer' OR t.previous_author_type IS NULL) AND t.author_id != '000000000000050001000001')
    OR (t.previous_author_type = 'agent' AND t.author_type = 'agent' AND t.previous_author_id != t.author_id AND t.previous_author_id != '000000000000050001000001' AND t.author_id != '000000000000050001000001')
    OR (t.previous_author_type = 'agent' AND t.author_type = 'agent' AND t.previous_author_id != t.author_id AND t.previous_author_id = '000000000000050001000001' AND t.previous_text LIKE '%escalate to agen%')
    OR (t.previous_author_type = 'agent' AND t.author_type = 'agent' AND t.previous_author_id = t.author_id AND ((UNIX_TIMESTAMP(t.event_ts_msk) - UNIX_TIMESTAMP(t.previous_event_ts_msk)) / 60 > 3)  AND t.author_id != '000000000000050001000001')
    OR (t.author_type = 'Resolved' AND t.previous_author_type = 'agent' AND t.previous_author_id != t.author_id AND t.author_id != '000000000000050001000001' AND t.previous_author_id != '000000000000050001000001')
    OR (t.author_type = 'Resolved' AND t.previous_author_type = 'agent' AND t.previous_author_id != t.author_id AND t.author_id != '000000000000050001000001' AND t.previous_author_id = '000000000000050001000001'
        AND t.previous_text LIKE '%escalate to agen%')
    OR (t.author_type = 'Resolved' AND t.previous_author_type = 'customer' AND t.author_id != '000000000000050001000001')
    OR (t.previous_author_type = 'agent' AND t.author_type = 'agent' AND t.previous_entry_type != t.entry_type AND t.previous_entry_type IN ('message', 'privateNote') AND t.entry_type IN ('message', 'privateNote'))
)

SELECT
    *
FROM agent_support_actions
