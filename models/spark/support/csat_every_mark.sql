{{ config(
     schema='support',
     materialized='table',
     location_root='s3://joom-analytics-mart/support/nonpartitioned/',
     file_format='delta',
     meta = {
       'team': 'analytics',
       'bigquery_load': 'true',
       'bigquery_overwrite': 'true',
       'alerts_channel': "#support-etl-monitoring",
       'model_owner' : '@operational.analytics.duty'
     }
 ) }}

WITH tickets AS (
    SELECT
        t.ticket_id,
        CONCAT(
            'https://babylone.joom.it/app/tickets/',
            t.ticket_id
        ) AS url,
        t.partition_date,
        DATE(t.resolution_ticket_ts_msk) AS resolution_date,
        t.country,
        t.os AS platform,
        t.language,
        t.is_closed,
        t.first_queue,
        t.current_queue,
        CASE WHEN t.last_agent IS NULL THEN 'no' ELSE 'yes' END AS was_agent_old,
        t.csat_was_triggered,
        CASE WHEN t.csat_was_triggered = 'no' THEN 0 ELSE 1 END AS csat_was_triggered_binary
    FROM
        {{ ref('support_mart_ticket_id_ext') }} AS t
    WHERE
        1 = 1
        AND t.is_hidden IS FALSE
),

csat AS (
    SELECT
        t.payload.ticketId AS ticket_id,
        t.event_ts_msk AS csat_ts_msk,
        1 AS trigger_csat,
        t.payload.selectedOptionsIds[0] AS csat
    FROM
        {{ source('mart', 'babylone_events') }} AS t
    WHERE
        1 = 1
        AND t.`type` = 'babyloneWidgetAction'
        AND t.payload.widgetType = 'did_we_help'
),

first_messages AS (
    SELECT
        t.payload.ticketId AS ticket_id,
        MIN(t.event_ts_msk) AS agent_message_event_ts_msk
    FROM
        {{ source('mart', 'babylone_events') }} AS t
    WHERE
        1 = 1
        AND t.`type` = 'ticketEntryAddJoom'
        AND t.payload.authorType = 'agent'
        AND t.payload.authorId != '000000000000050001000001'
    GROUP BY
        1
),

tmp AS (
    SELECT
        t.*,
        a.csat_ts_msk,
        a.trigger_csat,
        a.csat,
        b.agent_message_event_ts_msk,
        CASE WHEN (
            (b.agent_message_event_ts_msk IS NULL)
            OR (b.agent_message_event_ts_msk > a.csat_ts_msk)
        ) THEN 'no' ELSE 'yes' END AS was_agent
    FROM
        tickets AS t
    LEFT JOIN csat AS a ON t.ticket_id = a.ticket_id
    LEFT JOIN first_messages AS b ON t.ticket_id = b.ticket_id
    ORDER BY
        t.ticket_id,
        a.csat_ts_msk
)

SELECT *
FROM
    tmp
WHERE
    1 = 1
    AND csat IS NOT NULL

