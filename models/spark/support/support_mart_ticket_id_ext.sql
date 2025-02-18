{{ config(
     schema='support',
     materialized='table',
     file_format='delta',
     meta = {
       'priority_weight': '150',
       'team': 'analytics',
       'bigquery_load': 'true',
       'bigquery_overwrite': 'true',
       'alerts_channel': "#olc_dbt_alerts",
       'model_owner' : '@operational.analytics.duty'
     }
 ) }}

WITH users_with_first_order AS (
    SELECT
        user_id,
        MIN(created_time_utc) AS first_order_created_time_msk
    FROM {{ source('mart', 'fact_order_2020') }}
    GROUP BY 1
),

ticket_create_events AS (
    SELECT
        t.payload.ticketId AS ticket_id,
        MIN(t.event_ts_msk) AS ts_created,
        MIN(t.partition_date) AS partition_date,
        MIN(t.payload.authorType) AS author_of_ticket,
        MIN(t.payload.authorId) AS author_id,
        MIN(t.payload.deviceId) AS device_id,
        MIN(t.payload.customerExternalId) AS user_id,
        MIN(t.payload.lang) AS language,
        MIN(IF(t.payload.country NOT IN ('', 'Unknown'), t.payload.country, NULL)) AS country,
        MIN(t.payload.messageSource) AS os,
        MIN(t.payload.isHidden) AS is_hidden
    FROM {{ source('mart', 'babylone_events') }} AS t
    WHERE t.`type` IN ('ticketCreateJoom', 'ticketCreate')
    GROUP BY 1
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
        MAX(event_ts_msk) OVER (PARTITION BY author_id, ticket_id, partition_date ORDER BY event_ts_msk ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ts,
        MIN(event_ts_msk) OVER (PARTITION BY author_id, ticket_id, partition_date ORDER BY event_ts_msk ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ts
    FROM (
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
            AND t.`type` = 'ticketEntryAddJoom'
    )
),

first_entries AS (
    SELECT
        event_ts_msk,
        ticket_id,
        author_type,
        entry_id,
        FIRST_VALUE(event_ts_msk) OVER (PARTITION BY ticket_id, author_type ORDER BY event_ts_msk ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_entry_ts
    FROM ticket_entry_add
    WHERE
        entry_type != 'privateNote'
        AND author_id != '000000000000050001000001'
),

messages_first_replies AS (
    SELECT
        ticket_id,
        MIN(IF(author_type != 'customer', first_entry_ts, NULL)) AS ts_first_replied
    FROM first_entries
    GROUP BY 1
),

ttfr AS (
    SELECT
        tickets.ticket_id,
        tickets.country,
        tickets.partition_date,
        ((UNIX_SECONDS(TIMESTAMP(replies.ts_first_replied)) - UNIX_SECONDS(TIMESTAMP(tickets.ts_created))) / 3600) AS ttfr
    FROM ticket_create_events AS tickets
    LEFT JOIN messages_first_replies AS replies
        ON tickets.ticket_id = replies.ticket_id
),

ttfr_author_type AS (
    SELECT DISTINCT
        ticket_id,
        FIRST_VALUE(author_type) OVER (PARTITION BY ticket_id ORDER BY event_ts_msk) AS ttfr_author_type
    FROM ticket_entry_add
    WHERE
        entry_type != 'privateNote'
        AND author_id != '000000000000050001000001'
        AND author_type != 'customer'
),

first_queue AS (
    SELECT DISTINCT
        t.payload.ticketId AS ticket_id,
        FIRST_VALUE(a.name) OVER (PARTITION BY t.payload.ticketId ORDER BY t.event_ts_msk ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_queue
    FROM {{ source('mart', 'babylone_events') }} AS t
    INNER JOIN mongo.babylone_joom_queues_daily_snapshot AS a
        ON t.payload.stateQueueId = a._id
    WHERE
        t.`type` = 'ticketChangeJoom'
        AND t.payload.stateQueueId IS NOT NULL
),

first_queue_not_limbo AS (
    SELECT DISTINCT
        t.payload.ticketId AS ticket_id,
        FIRST_VALUE(a.name) OVER (PARTITION BY t.payload.ticketId ORDER BY t.event_ts_msk ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_queue_not_limbo
    FROM {{ source('mart', 'babylone_events') }} AS t
    INNER JOIN mongo.babylone_joom_queues_daily_snapshot AS a
        ON
            t.payload.stateQueueId = a._id
            AND a._id != 'limbo'
    WHERE
        t.`type` = 'ticketChangeJoom'
        AND t.payload.stateQueueId IS NOT NULL
),

current_queue AS (
    SELECT DISTINCT
        t.payload.ticketId AS ticket_id,
        LAST_VALUE(a.name) OVER (PARTITION BY t.payload.ticketId ORDER BY t.event_ts_msk ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS current_queue
    FROM {{ source('mart', 'babylone_events') }} AS t
    INNER JOIN mongo.babylone_joom_queues_daily_snapshot AS a
        ON t.payload.stateQueueId = a._id
    WHERE
        t.`type` = 'ticketChangeJoom'
        AND t.payload.stateQueueId IS NOT NULL
),

last_agent AS (
    SELECT DISTINCT
        t.payload.ticketId AS ticket_id,
        LAST_VALUE(t.payload.stateAgentId) OVER (PARTITION BY t.payload.ticketId ORDER BY t.event_ts_msk ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_agent
    FROM {{ source('mart', 'babylone_events') }} AS t
    WHERE
        t.`type` = 'ticketChangeJoom'
        AND t.payload.stateAgentId IS NOT NULL
),

all_queues AS (
    WITH t AS (
        SELECT DISTINCT
            t.payload.ticketId AS ticket_id,
            t.event_ts_msk,
            a.name AS queue
        FROM {{ source('mart', 'babylone_events') }} AS t
        INNER JOIN mongo.babylone_joom_queues_daily_snapshot AS a
            ON t.payload.stateQueueId = a._id
        WHERE
            t.`type` = 'ticketChangeJoom'
            AND t.payload.stateQueueId IS NOT NULL
        ORDER BY t.event_ts_msk
    )

    SELECT
        t.ticket_id,
        COLLECT_LIST(DISTINCT t.queue) AS queues
    FROM t AS t
    GROUP BY 1
),

all_tags AS (
    WITH t AS (
        SELECT
            t.payload.ticketId AS ticket_id,
            t.event_ts_msk,
            EXPLODE(t.payload.tagIds) AS tag
        FROM {{ source('mart', 'babylone_events') }} AS t
        WHERE
            t.payload.tagIds IS NOT NULL
            AND t.`type` IN ('ticketCreateJoom', 'ticketChangeJoom')
        ORDER BY t.event_ts_msk
    ),

    base AS (
        SELECT
            t.ticket_id,
            a.name AS tag
        FROM t AS t
        INNER JOIN mongo.babylone_joom_tags_daily_snapshot AS a ON t.tag = a._id
    )

    SELECT
        t.ticket_id,
        COLLECT_LIST(DISTINCT t.tag) AS tags
    FROM base AS t
    GROUP BY 1
),

all_parcels AS (
    WITH t AS (
        SELECT
            t.payload.ticketId AS ticket_id,
            t.event_ts_msk,
            EXPLODE(t.payload.parcelIds) AS parcelId
        FROM {{ source('mart', 'babylone_events') }} AS t
        WHERE
            t.payload.tagIds IS NOT NULL
            AND t.`type` IN ('ticketCreateJoom', 'ticketChangeJoom')
        ORDER BY t.event_ts_msk
    )

    SELECT
        t.ticket_id,
        COLLECT_LIST(DISTINCT t.parcelId) AS parcelIds
    FROM t AS t
    GROUP BY 1
),

all_orders AS (
    WITH t AS (
        SELECT
            t.payload.ticketId AS ticket_id,
            t.event_ts_msk,
            EXPLODE(t.payload.orderIds) AS orderId
        FROM {{ source('mart', 'babylone_events') }} AS t
        WHERE
            t.payload.tagIds IS NOT NULL
            AND t.`type` IN ('ticketCreateJoom', 'ticketChangeJoom')
        ORDER BY t.event_ts_msk
    )

    SELECT
        t.ticket_id,
        COLLECT_LIST(DISTINCT t.orderId) AS orderIds
    FROM t AS t
    GROUP BY 1
),

all_agents AS (
    WITH t AS (
        SELECT DISTINCT
            t.payload.ticketId AS ticket_id,
            t.payload.authorId AS author_id
        FROM {{ source('mart', 'babylone_events') }} AS t
        WHERE t.`type` = 'ticketEntryAdd'
        UNION DISTINCT
        SELECT DISTINCT
            t.payload.ticketId AS ticket_id,
            t.payload.stateAgentId AS author_id --stateAgentId
        FROM {{ source('mart', 'babylone_events') }} AS t
        WHERE t.`type` = 'ticketChangeJoom'
    )

    SELECT
        t.ticket_id,
        COLLECT_LIST(DISTINCT t.author_id) AS agentIds
    FROM t AS t
    WHERE t.author_id != '000000000000050001000001'
    GROUP BY 1
),

responses AS (
    WITH ranking AS (
        SELECT
            t.payload.ticketId AS ticket_id,
            t.event_ts_msk,
            CASE WHEN t.payload.authorType != 'customer' THEN 'support' ELSE 'customer' END AS author_type,
            ROW_NUMBER() OVER (PARTITION BY t.payload.ticketId ORDER BY t.event_ts_msk) AS num
        FROM {{ source('mart', 'babylone_events') }} AS t
        INNER JOIN {{ source('mart', 'babylone_events') }} AS a
            ON
                t.payload.ticketId = a.payload.ticketId
                AND a.`type` = 'ticketCreateJoom'
        WHERE
            t.payload.entryType = 'message'
            AND t.`type` = 'ticketEntryAddJoom'
    ),

    clear_ranking AS (
        SELECT t.*
        FROM ranking AS t
        INNER JOIN ranking AS a ON
            t.ticket_id = a.ticket_id
            AND a.num = (t.num - 1)
            AND t.author_type != a.author_type
    ),

    base AS (
        SELECT a.*
        FROM clear_ranking AS a
        UNION ALL
        SELECT t.*
        FROM ranking AS t
        WHERE t.num = 1
    ),

    responses_to_support AS (
        SELECT
            t.ticket_id,
            COUNT(t.author_type) AS responses_to_support
        FROM base AS t
        WHERE t.author_type = 'customer'
        GROUP BY 1
    ),

    responses_to_customer AS (
        SELECT
            t.ticket_id,
            COUNT(t.author_type) AS responses_to_customer
        FROM base AS t
        WHERE t.author_type = 'support'
        GROUP BY 1
    )

    SELECT
        COALESCE(t.ticket_id, a.ticket_id) AS ticket_id,
        COALESCE(t.responses_to_support, 0) AS responses_to_support,
        COALESCE(a.responses_to_customer, 0) AS responses_to_customer
    FROM responses_to_support AS t
    LEFT JOIN responses_to_customer AS a ON t.ticket_id = a.ticket_id
),

csat AS (
    SELECT
        ticket_id,
        csat
    FROM (
        SELECT
            t.payload.ticketId AS ticket_id,
            t.payload.selectedOptionsIds[0] AS csat,
            ROW_NUMBER() OVER (PARTITION BY t.payload.ticketId ORDER BY t.event_ts_msk DESC) AS rn
        FROM {{ source('mart', 'babylone_events') }} AS t
        WHERE
            t.`type` = 'babyloneWidgetAction'
            AND t.payload.widgetType = 'did_we_help'
            AND t.payload.selectedOptionsIds[0] IS NOT NULL
    )
    WHERE rn = 1
),

csat_was_triggered AS (
    SELECT DISTINCT t.payload.ticketId AS ticket_id
    FROM {{ source('mart', 'babylone_events') }} AS t
    WHERE
        t.`type` = 'babyloneWidgetAction'
        AND t.payload.widgetType = 'did_we_help'
        AND t.payload.selectedOptionsIds[0] IS NULL
),

resolution AS (
    SELECT
        t.payload.ticketId AS ticket_id,
        MIN(t.event_ts_msk) AS resolution_ticket_ts_msk
    FROM {{ source('mart', 'babylone_events') }} AS t
    WHERE
        t.payload.stateOwner IN ('Resolved', 'Rejected')
        AND t.`type` = 'ticketChangeJoom'
    GROUP BY 1
),

button_place AS (
    SELECT DISTINCT
        t.payload.ticketId AS ticket_id,
        FIRST_VALUE(t.payload.buttonPlace) OVER (PARTITION BY t.payload.ticketId ORDER BY t.event_ts_msk ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS button_place
    FROM {{ source('mart', 'babylone_events') }} AS t
    WHERE
        t.`type` = 'ticketEntryAddJoom'
        AND t.payload.authorType = 'customer'
        AND t.payload.buttonPlace IS NOT NULL
)

SELECT
    t.partition_date,
    t.ts_created AS creation_ticket_ts_msk,
    t.author_of_ticket,
    y.email AS author_email,
    t.device_id,
    t.ticket_id,
    t.user_id,
    t.country,
    CASE WHEN ((t.is_hidden IS TRUE) AND (i.agentIds IS NOT NULL)) THEN FALSE ELSE t.is_hidden END AS is_hidden,
    n.button_place,
    t.os, --но вообще бы дёргать нормальную ось (тут проблемы с INTERNAL, присваиваемым ко всем скрытым тикетам)
    CASE WHEN a.first_order_created_time_msk IS NULL THEN 'no' ELSE 'yes' END AS has_success_payments,
    b.resolution_ticket_ts_msk,
    t.language,
    c.ttfr,
    m.ttfr_author_type,
    CASE WHEN b.resolution_ticket_ts_msk IS NULL THEN 'no' ELSE 'yes' END AS is_closed,
    CASE WHEN p.current_queue == 'Limbo' THEN (CASE WHEN f.queues[0] == 'Limbo' THEN f.queues[1] ELSE f.queues[0] END) ELSE p.current_queue END AS current_queue,
    f.queues,
    --f.queues[0] AS first_queue,
    --CASE WHEN f.queues[0] == 'Limbo' THEN f.queues[1] ELSE f.queues[0] END AS first_queue_not_limbo,
    r.first_queue,
    s.first_queue_not_limbo,
    e.tags,
    g.parcelIds,
    h.orderIds,
    i.agentIds,
    q.last_agent,
    COALESCE(k.responses_to_support, 0) AS responses_to_support,
    COALESCE(k.responses_to_customer, 0) AS responses_to_customer,
    CASE WHEN o.ticket_id IS NULL THEN 'no' ELSE 'yes' END AS csat_was_triggered,
    l.csat
FROM ticket_create_events AS t
LEFT JOIN users_with_first_order AS a
    ON
        t.user_id = a.user_id
        AND t.ts_created >= a.first_order_created_time_msk
LEFT JOIN resolution AS b ON t.ticket_id = b.ticket_id
LEFT JOIN ttfr AS c ON t.ticket_id = c.ticket_id
LEFT JOIN all_tags AS e ON t.ticket_id = e.ticket_id
LEFT JOIN all_queues AS f ON t.ticket_id = f.ticket_id
LEFT JOIN all_parcels AS g ON t.ticket_id = g.ticket_id
LEFT JOIN all_orders AS h ON t.ticket_id = h.ticket_id
LEFT JOIN all_agents AS i ON t.ticket_id = i.ticket_id
LEFT JOIN responses AS k ON t.ticket_id = k.ticket_id
LEFT JOIN csat AS l ON t.ticket_id = l.ticket_id
LEFT JOIN ttfr_author_type AS m ON t.ticket_id = m.ticket_id
LEFT JOIN button_place AS n ON t.ticket_id = n.ticket_id
LEFT JOIN csat_was_triggered AS o ON t.ticket_id = o.ticket_id
LEFT JOIN current_queue AS p ON t.ticket_id = p.ticket_id
LEFT JOIN last_agent AS q ON t.ticket_id = q.ticket_id
LEFT JOIN first_queue AS r ON t.ticket_id = r.ticket_id
LEFT JOIN first_queue_not_limbo AS s ON t.ticket_id = s.ticket_id
LEFT JOIN {{ source('mongo', 'babylone_joom_agents_daily_snapshot') }} AS y ON t.author_id = y._id

