{{ config(
     schema='support',
     materialized='table',
     location_root='s3://joom-analytics-mart/support/nonpartitioned/',
     file_format='delta',
     meta = {
       'priority_weight': '150',
       'team': 'analytics',
       'bigquery_load': 'true',
       'bigquery_overwrite': 'true',
       'alerts_channel': "#olc_dbt_alerts",
       'model_owner' : '@ilypavlov'
     }
 ) }}

WITH
deletes AS (
    SELECT DISTINCT payload.entryid AS entry_id
    FROM {{ source('mart', 'babylone_events') }}
    WHERE
        1 = 1
        AND type = 'ticketEntryDeleted'
)
,

--agents' entries to exclude duplicates
ranking_pre_pre AS (
    SELECT
        t.payload.ticketid AS ticket_id,
        t.payload.authorid AS author_id,
        b.email,
        t.payload.authortype AS author_type,
        t.payload.entrytype AS entry_type,
        t.payload.text AS text,
        MIN(t.partition_date) AS partition_date,
        MIN(t.event_ts_msk) AS event_ts_msk
    FROM {{ source('mart', 'babylone_events') }} AS t
    LEFT JOIN deletes AS c ON t.payload.entryid = c.entry_id
    LEFT JOIN {{ source('mongo', 'babylone_joom_agents_daily_snapshot') }} AS b ON t.payload.authorid = b._id
    WHERE
        t.type = 'ticketEntryAddJoom'
        AND t.payload.authortype = 'agent'
        AND c.entry_id IS NULL
        AND t.payload.entrytype = 'privateNote'
    GROUP BY 1, 2, 3, 4, 5, 6
    ORDER BY 1, 7, 8
),

--agents' and customers' messages    
ranking_pre_pre_cust AS (
    SELECT
        t.payload.ticketid AS ticket_id,
        t.payload.authorid AS author_id,
        b.email,
        t.payload.authortype AS author_type,
        t.payload.entrytype AS entry_type,
        t.payload.text AS text,
        t.partition_date,
        t.event_ts_msk
    FROM {{ source('mart', 'babylone_events') }} AS t
    LEFT JOIN deletes AS c ON t.payload.entryid = c.entry_id
    LEFT JOIN {{ source('mongo', 'babylone_joom_agents_daily_snapshot') }} AS b ON t.payload.authorid = b._id
    WHERE
        t.type = 'ticketEntryAddJoom'
        AND (t.payload.authortype = 'customer' OR t.payload.entrytype = 'message')
        AND c.entry_id IS NULL
    ORDER BY 1, 7, 8
),

--all entries    
ranking_pre AS (
    SELECT *
    FROM ranking_pre_pre
    UNION ALL
    SELECT *
    FROM ranking_pre_pre_cust
),

resolutions AS (
    SELECT
        t.payload.ticketid AS ticket_id,
        t.payload.stateagentid AS author_id,
        b.email,
        'Resolved' AS author_type,
        '-' AS entry_type,
        '-' AS text,
        MIN(t.partition_date) AS partition_date,
        MIN(t.event_ts_msk) AS event_ts_msk
    FROM {{ source('mart', 'babylone_events') }} AS t
    LEFT JOIN {{ source('mongo', 'babylone_joom_agents_daily_snapshot') }} AS b ON t.payload.stateagentid = b._id
    WHERE
        t.type = 'ticketChangeJoom'
        AND t.payload.stateowner IN ('Rejected', 'Resolved')
        AND t.payload.changedbytype = 'agent'
    GROUP BY 1, 2, 3, 4, 5, 6
),

--button resolve
button_resolutions AS (
    SELECT
        t.payload.ticketid AS ticket_id,
        t.payload.stateagentid AS author_id,
        b.email,
        'Button_Resolved' AS author_type,
        '-' AS entry_type,
        '-' AS text,
        MIN(t.partition_date) AS partition_date,
        MIN(t.event_ts_msk) AS event_ts_msk
    FROM {{ source('mart', 'babylone_events') }} AS t
    LEFT JOIN {{ source('mongo', 'babylone_joom_agents_daily_snapshot') }} AS b ON t.payload.stateagentid = b._id
    WHERE
        t.type = 'ticketChangeJoom'
        AND t.payload.changedbytype = 'agent'
        AND t.payload.preresolved
        AND t.partition_date >= '2023-10-06'
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
        DATE(partition_date) AS partition_date,
        ticket_id,
        author_id,
        email,
        author_type,
        entry_type,
        text,
        event_ts_msk
    FROM resolutions
    UNION ALL
    SELECT
        DATE(partition_date) AS partition_date,
        ticket_id,
        author_id,
        email,
        author_type,
        entry_type,
        text,
        event_ts_msk
    FROM button_resolutions
),

ordered_final_1 AS (
    SELECT *
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
        LAG(text) OVER (PARTITION BY ticket_id ORDER BY event_ts_msk) AS previous_text,
        text,
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
    WHERE
        1 = 1
        AND ((t.previous_author_type = 'customer' OR t.previous_author_type IS NULL) AND t.author_id != '000000000000050001000001')
        OR (
            t.previous_author_type = 'agent'
            AND t.author_type = 'agent'
            AND t.previous_author_id != t.author_id
            AND t.previous_author_id != '000000000000050001000001'
            AND t.author_id != '000000000000050001000001'
        )
        OR (
            t.previous_author_type = 'agent'
            AND t.author_type = 'agent'
            AND t.previous_author_id != t.author_id
            AND t.previous_author_id = '000000000000050001000001'
            AND t.previous_text LIKE '%escalate to agen%'
        )
        OR (
            t.previous_author_type = 'agent'
            AND t.author_type = 'agent'
            AND t.previous_author_id = t.author_id
            AND ((UNIX_TIMESTAMP(t.event_ts_msk) - UNIX_TIMESTAMP(t.previous_event_ts_msk)) / 60 > 3)
            AND t.author_id != '000000000000050001000001'
        )
        OR (
            t.author_type = 'Resolved'
            AND t.previous_author_type = 'agent'
            AND t.previous_author_id != t.author_id
            AND t.author_id != '000000000000050001000001'
            AND t.previous_author_id != '000000000000050001000001'
        )
        OR (
            t.author_type = 'Resolved'
            AND t.previous_author_type = 'agent'
            AND t.previous_author_id != t.author_id
            AND t.author_id != '000000000000050001000001'
            AND t.previous_author_id = '000000000000050001000001'
            AND t.previous_text LIKE '%escalate to agen%'
        )
        OR (t.author_type = 'Resolved' AND t.previous_author_type = 'customer' AND t.author_id != '000000000000050001000001')
        OR (
            t.previous_author_type = 'agent'
            AND t.author_type = 'agent'
            AND t.previous_entry_type != t.entry_type
            AND t.previous_entry_type IN ('message', 'privateNote')
            AND t.entry_type IN ('message', 'privateNote')
        )
        OR t.author_type = 'Button_Resolved'
),

ranking_pre_pre_queue AS (
    SELECT
        t.payload.ticketid AS ticket_id,
        b.email,
        t.payload.stateagentid AS state_agent_id,
        t.payload.stateowner AS state_owner,
        t.payload.statequeueid AS state_queue_id,
        t.payload.changedbytype AS changed_by_type,
        t.partition_date,
        t.event_ts_msk
    FROM {{ source('mart', 'babylone_events') }} AS t
    LEFT JOIN {{ source('mongo', 'babylone_joom_agents_daily_snapshot') }} AS b ON t.payload.stateagentid = b._id
    WHERE t.type = 'ticketChangeJoom'
    ORDER BY 1, 7, 8
),

ranking_pre_queue AS (
    SELECT
        partition_date,
        ticket_id,
        LAG(email) OVER (PARTITION BY ticket_id ORDER BY event_ts_msk) AS previous_email,
        email,
        LAG(state_agent_id) OVER (PARTITION BY ticket_id ORDER BY event_ts_msk) AS previous_state_agent_id,
        state_agent_id,
        LAG(state_owner) OVER (PARTITION BY ticket_id ORDER BY event_ts_msk) AS previous_state_owner,
        state_owner,
        LAG(state_queue_id) OVER (PARTITION BY ticket_id ORDER BY event_ts_msk) AS previous_state_queue_id,
        state_queue_id,
        LAG(changed_by_type) OVER (PARTITION BY ticket_id ORDER BY event_ts_msk) AS previous_changed_by_type,
        changed_by_type,
        LAG(event_ts_msk) OVER (PARTITION BY ticket_id ORDER BY event_ts_msk) AS previous_event_ts_msk,
        event_ts_msk
    FROM ranking_pre_pre_queue
    ORDER BY 2, 14
),

escalations_to_queue AS (
    SELECT
        t.partition_date,
        t.ticket_id,
        NULL AS previous_author_id,
        t.previous_state_agent_id AS author_id,
        NULL AS previous_author_type,
        t.previous_state_owner AS author_type,
        NULL AS previous_entry_type,
        'эскалация в очередь' AS entry_type,
        NULL AS previous_event_ts_msk,
        t.previous_event_ts_msk AS event_ts_msk,
        'эскалация в очередь' AS previous_text,
        'эскалация в очередь' AS text,
        t.previous_email AS email
    FROM ranking_pre_queue AS t
    WHERE
        1 = 1
        AND t.previous_changed_by_type = 'agent'
        AND t.previous_state_owner = 'Agent'
        AND t.changed_by_type = 'agent'
        AND t.state_owner = 'Queue'
        AND t.previous_state_queue_id IS NOT NULL
        AND t.state_queue_id != t.previous_state_queue_id
),

agent_support_actions_t AS (
    SELECT *
    FROM agent_support_actions
    UNION ALL
    SELECT *
    FROM escalations_to_queue
)

SELECT *
FROM agent_support_actions_t
