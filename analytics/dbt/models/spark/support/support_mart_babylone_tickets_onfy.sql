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

WITH creations_onfy AS (
  SELECT
    payload.ticketId AS ticket_id,
    payload.country AS country,
    payload.lang AS language,
    'onfy' AS business_unit,
    'creation' AS event,
    MIN(event_ts_msk) AS `timestamp`
  FROM
      mart.onfy_babylone_events
  WHERE
    `type` = 'ticketCreate'
     AND event_ts_msk IS NOT NULL
  GROUP BY
    1,
    2,
    3,
    4,
    5
),
transfer_to_bot AS (
  SELECT
    payload.ticketId AS ticket_id,
    'onfy' AS business_unit,
    'transfer_to_bot' AS event,
    MIN(event_ts_msk) AS `timestamp`
  FROM
    mart.onfy_babylone_events
  WHERE
    `type` = 'ticketChange'
    AND payload.stateOwner = 'Automation'
  GROUP BY
    1,
    2,
    3
),
transfer_to_queue AS (
  SELECT
    payload.ticketId AS ticket_id,
    'onfy' AS business_unit,
    'transfer_to_queue' AS event,
    MIN(event_ts_msk) AS `timestamp`
  FROM
    mart.onfy_babylone_events
  WHERE
    `type` = 'ticketChange'
    AND payload.stateOwner = 'Queue'
  GROUP BY
    1,
    2,
    3
),
transfer_to_agent AS (
  SELECT
    payload.ticketId AS ticket_id,
    'onfy' AS business_unit,
    'transfer_to_agent' AS event,
    MIN(event_ts_msk) AS `timestamp`
  FROM
    mart.onfy_babylone_events
  WHERE
    `type` = 'ticketChange'
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
    MAX(event_ts_msk) OVER(
      PARTITION BY author_id,
      ticket_id,
      partition_date
      ORDER BY
        event_ts_msk ASC ROWS BETWEEN UNBOUNDED PRECEDING
        AND UNBOUNDED FOLLOWING
    ) AS last_ts,
    MIN(event_ts_msk) OVER(
      PARTITION BY author_id,
      ticket_id,
      partition_date
      ORDER BY
        event_ts_msk ASC ROWS BETWEEN UNBOUNDED PRECEDING
        AND UNBOUNDED FOLLOWING
    ) AS first_ts
  FROM
    (
      SELECT
        t.partition_date AS partition_date,
        t.event_ts_msk,
        t.payload.ticketId AS ticket_id,
        t.payload.authorId AS author_id,
        t.payload.authorType AS author_type,
        t.payload.entryId AS entry_id,
        t.payload.entryType AS entry_type
      FROM
        mart.onfy_babylone_events AS t
      WHERE
        NOT t.payload.isAnnouncement
        AND t.`type` = 'ticketEntryAdd'
    )
),
first_entries AS (
  SELECT
    event_ts_msk,
    ticket_id,
    author_type,
    entry_id,
    FIRST_VALUE(event_ts_msk) OVER(
      PARTITION BY ticket_id,
      author_type
      ORDER BY
        event_ts_msk ASC ROWS BETWEEN UNBOUNDED PRECEDING
        AND UNBOUNDED FOLLOWING
    ) AS first_entry_ts,
    LAST_VALUE(event_ts_msk) OVER(
      PARTITION BY ticket_id,
      author_type
      ORDER BY
        event_ts_msk ASC ROWS BETWEEN UNBOUNDED PRECEDING
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
    'onfy' AS business_unit,
    'agent_first_reply' AS event,
    MIN(IF(author_type = 'agent', first_entry_ts, NULL)) AS `timestamp`
  FROM
    first_entries
  GROUP BY
    1,
    2,
    3
),
closing_onfy AS (
  SELECT
    payload.ticketId AS ticket_id,
    'onfy' AS business_unit,
    'closing' AS event,
    MIN(event_ts_msk) AS `timestamp`
  FROM
    mart.onfy_babylone_events
  WHERE
    `type` = 'ticketChange'
    AND payload.stateOwner IN ('Rejected', 'Resolved')
  GROUP BY
    1,
    2,
    3
),
messages_last_replies AS (
  SELECT
    t.ticket_id,
    'onfy' AS business_unit,
    'agent_last_reply' AS event,
    MIN(
      IF(t.author_type = 'agent', t.last_entry_ts, NULL)
    ) AS `timestamp`
  FROM
    first_entries AS t
    JOIN closing_onfy AS a ON t.ticket_id = a.ticket_id
  GROUP BY
    1,
    2,
    3
),
all_queues AS (
  WITH t AS (
    SELECT
      DISTINCT t.payload.ticketId AS ticket_id,
      a.name AS queue
    FROM
     mart.onfy_babylone_events AS t
      JOIN {{ source('mongo', 'babylone_onfy_queues_daily_snapshot') }} AS a ON t.payload.stateQueueId = a._id
    WHERE
      t.`type` = 'ticketChange'
      AND t.payload.stateQueueId IS NOT NULL
  )
  SELECT
    t.ticket_id AS ticket_id,
    COLLECT_LIST(DISTINCT t.queue) AS queues
  FROM
    t AS t
  GROUP BY
    1
),
current_queue AS (
  SELECT
    DISTINCT t.payload.ticketId AS ticket_id,
    LAST_VALUE(a.name) OVER(
      PARTITION BY t.payload.ticketId
      ORDER BY
        t.event_ts_msk ASC ROWS BETWEEN UNBOUNDED PRECEDING
        AND UNBOUNDED FOLLOWING
    ) AS current_queue
  FROM
    mart.onfy_babylone_events AS t
    JOIN {{ source('mongo', 'babylone_onfy_queues_daily_snapshot') }} AS a ON t.payload.stateQueueId = a._id
  WHERE
    t.`type` = 'ticketChange'
    AND t.payload.stateQueueId IS NOT NULL
),
hidden_tickets AS (
  SELECT
    DISTINCT t.payload.ticketId AS ticket_id
  FROM
    mart.onfy_babylone_events AS t
  WHERE
    t.`type` IN ('ticketCreate', 'ticketChange')
    AND t.payload.isHidden IS FALSE
),
all_tags AS (
  WITH t AS (
    SELECT
      t.payload.ticketId AS ticket_id,
      EXPLODE(t.payload.tagIds) AS tag
    FROM
      mart.onfy_babylone_events AS t
    WHERE
      t.payload.tagIds IS NOT NULL
      AND t.`type` IN ('ticketCreate', 'ticketChange')
  )
  SELECT
    t.ticket_id AS ticket_id,
    a.name AS tag
  FROM
    t
    JOIN {{ source('mongo', 'babylone_onfy_tags_daily_snapshot') }} AS a ON t.tag = a._id
),
all_markers AS (
  SELECT
    t.payload.ticketId AS ticket_id,
    EXPLODE(t.payload.quickReplyMarkers) AS marker
  FROM
    mart.onfy_babylone_events AS t
  WHERE
    t.payload.quickReplyMarkers IS NOT NULL
    AND t.`type` = 'ticketEntryAdd'
),
channel AS (
  SELECT
    t.payload.ticketId AS ticket_id,
    MIN(t.payload.channel) AS channel
  FROM
    mart.onfy_babylone_events AS t
  WHERE
    t.`type` = 'ticketCreate'
  GROUP BY
    1
),
bot_result AS (
  SELECT
    ticket_id,
    CASE WHEN agentIds IS NULL THEN 'no' ELSE 'yes' END AS was_escalated
  FROM
    {{ ref('support_mat_ticket_id_ext_onfy') }}
    JOIN transfer_to_bot USING (ticket_id)
),
scenario AS (
  SELECT
    DISTINCT payload.ticketId AS ticket_id,
    payload.reactionState AS reaction_state
  FROM
    mart.onfy_babylone_events
  WHERE
    `type` = 'botReaction'
),
base AS (
  SELECT
    *
  FROM
    closing_onfy
  UNION ALL
  SELECT
    ticket_id,
    business_unit,
    event,
    `timestamp`
  FROM
    creations_onfy
  UNION ALL
  SELECT
    *
  FROM
    transfer_to_bot
  UNION ALL
  SELECT
    *
  FROM
    transfer_to_queue
  UNION ALL
  SELECT
    *
  FROM
    transfer_to_agent
  UNION ALL
  SELECT
    *
  FROM
    messages_first_replies
  UNION ALL
  SELECT
    *
  FROM
    messages_last_replies
),

final AS (
  SELECT
     DISTINCT t.ticket_id,
     t.business_unit,
     i.country,
     i.language,
     t.event,
     TIMESTAMP(t.`timestamp`) AS `timestamp`,
     DATE(t.`timestamp`) AS partition_date,
     CASE WHEN a.queues [0] == 'Limbo' THEN a.queues [1] ELSE a.queues [0] END AS first_queue,
     CASE WHEN b.current_queue == 'Limbo' THEN (
      CASE WHEN a.queues [0] == 'Limbo' THEN a.queues [1] ELSE a.queues [0] END
    ) ELSE b.current_queue END AS current_queue,
    CASE WHEN c.ticket_id IS NOT NULL THEN 'no' ELSE 'yes' END AS is_hidden,
    d.tag,
    e.marker AS marker_from_quickreply,
    f.channel,
    g.was_escalated,
    h.reaction_state
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
    LEFT JOIN creations_onfy AS i ON t.ticket_id = i.ticket_id
  WHERE t.`timestamp` IS NOT NULL
)

SELECT
    *
FROM
  final
WHERE partition_date IS NOT NULL
