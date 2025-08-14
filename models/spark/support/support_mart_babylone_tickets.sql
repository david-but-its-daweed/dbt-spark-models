{{ config(
     schema='support',
     materialized='table',
     file_format='delta',
     meta = {
       'priority_weight': '150',
       'team': 'analytics',
       'bigquery_load': 'true',
       'bigquery_overwrite': 'true',
       'alerts_channel': "#support-etl-monitoring",
       'model_owner' : '@operational.analytics.duty'
     }
 ) }}

WITH support_mart_tickets AS (
    SELECT
        ticket_id,
        language,
        country,
        created_time_utc,
        resolved_time_utc,
        queues_list,
        last_queue_name,
        is_hidden,
        communication_channel,
        tags,
        subjects,
        subject_categories
    FROM
        {{ source('support_mart', 'tickets') }}
    WHERE
        created_time_utc >= CURRENT_DATE - INTERVAL '2' YEAR
),

creations_marketplace AS (
    SELECT
        ticket_id,
        'marketplace' AS business_unit,
        'creation' AS event,
        language,
        country,
        created_time_utc AS `timestamp`
    FROM
        support_mart_tickets
),

transfer_to_bot AS (
    SELECT
        payload.ticketId AS ticket_id,
        'marketplace' AS business_unit,
        'transfer_to_bot' AS event,
        MIN(event_ts_msk) AS `timestamp`
    FROM
        {{ source('mart', 'babylone_events') }}
    WHERE
        `type` = 'ticketChangeJoom'
        AND partition_date >= DATE '2024-01-01'
        AND payload.stateOwner = 'Automation'
    GROUP BY
        1,
        2,
        3
),

transfer_to_queue AS (
    SELECT
        payload.ticketId AS ticket_id,
        'marketplace' AS business_unit,
        'transfer_to_queue' AS event,
        MIN(event_ts_msk) AS `timestamp`
    FROM
        {{ source('mart', 'babylone_events') }}
    WHERE
        `type` = 'ticketChangeJoom'
        AND partition_date >= DATE '2024-01-01'
        AND payload.stateOwner = 'Queue'
    GROUP BY
        1,
        2,
        3
),

transfer_to_agent AS (
    SELECT
        payload.ticketId AS ticket_id,
        'marketplace' AS business_unit,
        'transfer_to_agent' AS event,
        MIN(event_ts_msk) AS `timestamp`
    FROM
        {{ source('mart', 'babylone_events') }}
    WHERE
        `type` = 'ticketChangeJoom'
        AND partition_date >= DATE '2024-01-01'
        AND payload.stateOwner = 'Agent'
    GROUP BY
        1,
        2,
        3
),

ticket_entry_add AS (
    SELECT
        partition_date,
        event_ts_msk,
        ticket_id,
        author_id,
        author_type,
        entry_id,
        entry_type,
        MAX(event_ts_msk) OVER (
            PARTITION BY
                author_id,
                ticket_id,
                partition_date
            ORDER BY
                event_ts_msk ASC
            ROWS BETWEEN UNBOUNDED PRECEDING
            AND UNBOUNDED FOLLOWING
        ) AS last_ts,
        MIN(event_ts_msk) OVER (
            PARTITION BY
                author_id,
                ticket_id,
                partition_date
            ORDER BY
                event_ts_msk ASC
            ROWS BETWEEN UNBOUNDED PRECEDING
            AND UNBOUNDED FOLLOWING
        ) AS first_ts
    FROM
        (
            SELECT
                t.partition_date,
                t.event_ts_msk,
                t.payload.ticketId AS ticket_id,
                t.payload.authorId AS author_id,
                t.payload.authorType AS author_type,
                t.payload.entryId AS entry_id,
                t.payload.entryType AS entry_type
            FROM
                {{ source('mart', 'babylone_events') }} AS t
            WHERE
                NOT t.payload.isAnnouncement
                AND t.partition_date >= DATE '2024-01-01'
                AND t.`type` = 'ticketEntryAddJoom'
        )
),

first_entries AS (
    SELECT
        event_ts_msk,
        ticket_id,
        author_type,
        entry_id,
        FIRST_VALUE(event_ts_msk) OVER (
            PARTITION BY
                ticket_id,
                author_type
            ORDER BY
                event_ts_msk ASC
            ROWS BETWEEN UNBOUNDED PRECEDING
            AND UNBOUNDED FOLLOWING
        ) AS first_entry_ts,
        LAST_VALUE(event_ts_msk) OVER (
            PARTITION BY
                ticket_id,
                author_type
            ORDER BY
                event_ts_msk ASC
            ROWS BETWEEN UNBOUNDED PRECEDING
            AND UNBOUNDED FOLLOWING
        ) AS last_entry_ts
    FROM
        ticket_entry_add
    WHERE
        entry_type != 'privateNote'
        AND author_id != '000000000000050001000001'
),

messages_first_replies AS (
    SELECT
        ticket_id,
        'marketplace' AS business_unit,
        'agent_first_reply' AS event,
        MIN(IF(author_type = 'agent', first_entry_ts, NULL)) AS `timestamp`
    FROM
        first_entries
    GROUP BY
        1,
        2,
        3
),

closing_marketplace AS (
    SELECT
        ticket_id,
        'marketplace' AS business_unit,
        'closing' AS event,
        resolved_time_utc AS `timestamp`
    FROM
        support_mart_tickets
    WHERE
        resolved_time_utc IS NOT NULL
),

messages_last_replies AS (
    SELECT
        t.ticket_id,
        'marketplace' AS business_unit,
        'agent_last_reply' AS event,
        MIN(
            IF(t.author_type = 'agent', t.last_entry_ts, NULL)
        ) AS `timestamp`
    FROM
        first_entries AS t
    INNER JOIN closing_marketplace AS a ON t.ticket_id = a.ticket_id
    GROUP BY
        1,
        2,
        3
),

all_queues AS (
    SELECT
        ticket_id,
        queues_list AS queues
    FROM
        support_mart_tickets
),

current_queue AS (
    SELECT
        ticket_id,
        last_queue_name AS current_queue
    FROM
        support_mart_tickets
),

hidden_tickets AS (
    SELECT ticket_id
    FROM
        support_mart_tickets
    WHERE
        NOT is_hidden
),

all_tags AS (
    SELECT
        ticket_id,
        -- в исходном коде не была различия между тегами/сабджектами/категориями, поэтому сохраняю эту же логику
        EXPLODE(
            ARRAY_DISTINCT(
                CONCAT(
                    COALESCE(tags, ARRAY()),
                    COALESCE(subjects, ARRAY()),
                    COALESCE(subject_categories, ARRAY())
                )
            )
        ) AS tag
    FROM
        support_mart_tickets
),

all_markers AS (
    SELECT
        t.payload.ticketId AS ticket_id,
        EXPLODE(t.payload.quickReplyMarkers) AS marker
    FROM
        {{ source('mart', 'babylone_events') }} AS t
    WHERE
        t.payload.quickReplyMarkers IS NOT NULL
        AND t.`type` = 'ticketEntryAddJoom'
        AND t.partition_date >= DATE '2024-01-01'
),

channel AS (
    SELECT
        ticket_id,
        communication_channel AS channel
    FROM
        support_mart_tickets
),

bot_result AS (
    SELECT
        ticket_id,
        CASE WHEN sm.agentIds IS NULL THEN 'no' ELSE 'yes' END AS was_escalated
    FROM {{ ref('support_mart_ticket_id_ext') }} AS sm
    INNER JOIN transfer_to_bot
        USING (ticket_id)
),

scenario AS (
    SELECT DISTINCT
        payload.ticketId AS ticket_id,
        payload.reactionState AS reaction_state
    FROM
        {{ source('mart', 'babylone_events') }}
    WHERE
        `type` = 'botReaction'
        AND partition_date >= DATE '2024-01-01'
),

base AS (
    SELECT *
    FROM
        closing_marketplace
    UNION ALL
    SELECT
        ticket_id,
        business_unit,
        event,
        `timestamp`
    FROM
        creations_marketplace
    UNION ALL
    SELECT *
    FROM
        transfer_to_bot
    UNION ALL
    SELECT *
    FROM
        transfer_to_queue
    UNION ALL
    SELECT *
    FROM
        transfer_to_agent
    UNION ALL
    SELECT *
    FROM
        messages_first_replies
    UNION ALL
    SELECT *
    FROM
        messages_last_replies
),

csat_prebase AS (
    SELECT
        t.payload.ticketId AS ticket_id,
        LAST_VALUE(t.payload.selectedOptionsIds[0]) OVER (PARTITION BY t.payload.ticketId ORDER BY t.event_ts_msk ASC) AS csat
    FROM {{ source('mart', 'babylone_events') }} AS t
    WHERE
        t.`type` = 'babyloneWidgetAction'
        AND t.partition_date >= DATE '2024-01-01'
        AND t.payload.widgetType = 'did_we_help'
        AND t.payload.selectedOptionsIds[0] IS NOT NULL
),

csat AS (
    SELECT
        t.ticket_id,
        MIN(t.csat) AS csat
    FROM csat_prebase AS t
    GROUP BY 1
),

csat_was_triggered AS (
    SELECT DISTINCT t.payload.ticketId AS ticket_id
    FROM {{ source('mart', 'babylone_events') }} AS t
    WHERE
        t.`type` = 'babyloneWidgetAction'
        AND t.partition_date >= DATE '2024-01-01'
        AND t.payload.widgetType = 'did_we_help'
        AND t.payload.selectedOptionsIds[0] IS NULL
)

SELECT DISTINCT
    t.ticket_id,
    t.business_unit,
    t.event,
    i.country,
    i.language,
    CAST(t.`timestamp` AS TIMESTAMP) AS `timestamp`,
    DATE(t.`timestamp`) AS partition_date,
    CASE WHEN a.queues[0] == 'Limbo' THEN a.queues[1] ELSE a.queues[0] END AS first_queue,
    CASE WHEN b.current_queue == 'Limbo' THEN (
        CASE WHEN a.queues[0] == 'Limbo' THEN a.queues[1] ELSE a.queues[0] END
    ) ELSE b.current_queue END AS current_queue,
    CASE WHEN c.ticket_id IS NOT NULL THEN 'no' ELSE 'yes' END AS is_hidden,
    d.tag,
    e.marker AS marker_from_quickreply,
    f.channel,
    g.was_escalated,
    h.reaction_state,
    CASE WHEN l.ticket_id IS NULL THEN 'no' ELSE 'yes' END AS csat_was_triggered,
    k.csat
FROM
    base AS t
LEFT JOIN all_queues AS a ON t.ticket_id = a.ticket_id
LEFT JOIN current_queue AS b ON t.ticket_id = b.ticket_id
LEFT JOIN hidden_tickets AS c ON t.ticket_id = c.ticket_id
LEFT JOIN all_tags AS d ON t.ticket_id = d.ticket_id
LEFT JOIN all_markers AS e ON t.ticket_id = e.ticket_id
LEFT JOIN channel AS f ON t.ticket_id = f.ticket_id
LEFT JOIN bot_result AS g ON t.ticket_id = g.ticket_id
LEFT JOIN scenario AS h ON t.ticket_id = h.ticket_id
LEFT JOIN creations_marketplace AS i ON t.ticket_id = i.ticket_id
LEFT JOIN csat AS k ON t.ticket_id = k.ticket_id
LEFT JOIN csat_was_triggered AS l ON t.ticket_id = l.ticket_id
WHERE t.`timestamp` IS NOT NULL
