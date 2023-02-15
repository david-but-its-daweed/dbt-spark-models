
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
 
WITH prebase_marketplace AS (
SELECT
    payload.ticketId AS ticket_id,
    payload.changedByType AS state_owner,
    EXPLODE(payload.tagIds) AS tag,
    partition_date,
    event_ts_msk AS event_ts_msk
FROM {{ source('mart', 'babylone_events') }}
WHERE `type` = 'ticketChangeJoom' 
AND partition_date > '2021-12-31'
AND payload.changedByType IN ('agent', 'automation')
)
,

t_marketplace AS (
    SELECT
        t.ticket_id,
        a.name AS tag,
        a.type AS tag_type,
        b.name AS subject_category,
        t.state_owner AS state_owner,
        t.partition_date,
        MIN(t.event_ts_msk) AS event_ts_msk
    FROM prebase_marketplace AS t
    LEFT JOIN {{ source('mongo', 'babylone_joom_tags_daily_snapshot') }} AS a ON t.tag = a._id
    LEFT JOIN {{ source('mongo', 'babylone_joom_tag_categories_daily_snapshot') }} AS b ON a.categoryId = b._id
    GROUP BY 1, 2, 3, 4, 5, 6
 )
,

tt_marketplace AS (
    SELECT
        t.ticket_id,
        t.tag,
        t.tag_type,
        t.subject_category,
        t.partition_date,
        t.event_ts_msk,
        FIRST_VALUE(t.state_owner) OVER (PARTITION BY t.ticket_id, t.tag, t.tag_type, t.subject_category, t.partition_date ORDER BY t.event_ts_msk) AS state_owner
    FROM t_marketplace AS t
),

base_marketplace AS (
SELECT
    t.ticket_id,
    t.tag,
    t.tag_type,
    t.subject_category,
    t.state_owner,
    t.partition_date,
    MIN(t.event_ts_msk) AS event_ts_msk
FROM tt_marketplace AS t
GROUP BY 1, 2, 3, 4, 5, 6
    ),

marketplace AS (
SELECT
    'marketplace' AS bu,
    t.ticket_id,
    t.state_owner,
    t.tag,
    t.tag_type,
    t.subject_category,
    TIMESTAMP(t.event_ts_msk) AS event_ts_msk,
    t.partition_date,
    a.first_queue,
    a.current_queue,
    a.country,
    a.language
FROM base_marketplace AS t
LEFT JOIN {{ ref('support_mart_ticket_id_ext') }} AS a ON t.ticket_id = a.ticket_id
),

prebase_jl AS (
SELECT
    payload.ticketId AS ticket_id,
    payload.changedByType AS state_owner,
    EXPLODE(payload.tagIds) AS tag,
    partition_date,
    event_ts_msk AS event_ts_msk
FROM mart.logistics_babylone_events
WHERE `type` = 'ticketChange' 
AND partition_date > '2021-12-31'
AND payload.changedByType IN ('agent', 'automation')
)
,

t_jl AS (
    SELECT
        t.ticket_id,
        a.name AS tag,
        a.type AS tag_type,
        b.name AS subject_category,
        t.state_owner AS state_owner,
        t.partition_date,
        MIN(t.event_ts_msk) AS event_ts_msk
    FROM prebase_jl AS t
    LEFT JOIN {{ source('mongo', 'babylone_logistics_tags_daily_snapshot') }} AS a ON t.tag = a._id
    LEFT JOIN {{ source('mongo', 'babylone_logistics_tag_categories_daily_snapshot') }} AS b ON a.categoryId = b._id
    GROUP BY 1, 2, 3, 4, 5, 6
 )
,

tt_jl AS (
    SELECT
        t.ticket_id,
        t.tag,
        t.tag_type,
        t.subject_category,
        t.partition_date,
        t.event_ts_msk,
        FIRST_VALUE(t.state_owner) OVER (PARTITION BY t.ticket_id, t.tag, t.tag_type, t.subject_category, t.partition_date ORDER BY t.event_ts_msk) AS state_owner
    FROM t_jl AS t
),

base_jl AS (
SELECT
    t.ticket_id,
    t.tag,
    t.tag_type,
    t.subject_category,
    t.state_owner,
    t.partition_date,
    MIN(t.event_ts_msk) AS event_ts_msk
FROM tt_jl AS t
GROUP BY 1, 2, 3, 4, 5, 6
    ),

jl AS (
SELECT
    'logistics' AS bu,
    t.ticket_id,
    t.state_owner,
    t.tag,
    t.tag_type,
    t.subject_category,
    TIMESTAMP(t.event_ts_msk) AS event_ts_msk,
    t.partition_date,
    a.first_queue,
    a.current_queue,
    a.country,
    a.language
FROM base_jl AS t
LEFT JOIN {{ ref('support_mart_ticket_id_ext_jl') }} AS a ON t.ticket_id = a.ticket_id
),

prebase_joompay AS (
SELECT
    payload.ticketId AS ticket_id,
    payload.changedByType AS state_owner,
    EXPLODE(payload.tagIds) AS tag,
    partition_date,
    event_ts_msk AS event_ts_msk
FROM mart.narwhal_babylone_events
WHERE `type` = 'ticketChange' 
AND partition_date > '2021-12-31'
AND payload.changedByType IN ('agent', 'automation')
)
,

t_joompay AS (
    SELECT
        t.ticket_id,
        a.name AS tag,
        a.type AS tag_type,
        b.name AS subject_category,
        t.state_owner AS state_owner,
        t.partition_date,
        MIN(t.event_ts_msk) AS event_ts_msk
    FROM prebase_joompay AS t
    LEFT JOIN {{ source('mongo', 'babylone_narwhal_tags_daily_snapshot') }} AS a ON t.tag = a._id
    LEFT JOIN {{ source('mongo', 'babylone_narwhal_tag_categories_daily_snapshot') }} AS b ON a.categoryId = b._id
    GROUP BY 1, 2, 3, 4, 5, 6
 )
,

tt_joompay AS (
    SELECT
        t.ticket_id,
        t.tag,
        t.tag_type,
        t.subject_category,
        t.event_ts_msk,
        t.partition_date,
        FIRST_VALUE(t.state_owner) OVER (PARTITION BY t.ticket_id, t.tag, t.tag_type, t.subject_category, t.partition_date ORDER BY t.event_ts_msk) AS state_owner
    FROM t_joompay AS t
    ORDER BY 1
),

base_joompay AS (
SELECT
    t.ticket_id,
    t.tag,
    t.tag_type,
    t.subject_category,
    t.state_owner,
    t.partition_date,
    MIN(t.event_ts_msk) AS event_ts_msk
FROM tt_joompay AS t
GROUP BY 1,2,3,4,5,6
    ),

joompay AS (
SELECT
    'joompay' AS bu,
    t.ticket_id,
    t.state_owner,
    t.tag,
    t.tag_type,
    t.subject_category,
    TIMESTAMP(t.event_ts_msk) AS event_ts_msk,
    t.partition_date,
    a.first_queue,
    a.current_queue,
    a.country,
    a.language
FROM base_joompay AS t
LEFT JOIN {{ ref('support_mart_ticket_id_ext_joompay') }} AS a ON t.ticket_id = a.ticket_id
),

prebase_onfy AS (
SELECT
    payload.ticketId AS ticket_id,
    payload.changedByType AS state_owner,
    EXPLODE(payload.tagIds) AS tag,
    event_ts_msk AS event_ts_msk,
    partition_date
FROM mart.onfy_babylone_events
WHERE `type` = 'ticketChange' 
AND partition_date > '2021-12-31'
AND payload.changedByType IN ('agent', 'automation')
)
,

t_onfy AS (
    SELECT
        t.ticket_id,
        a.name AS tag,
        a.type AS tag_type,
        b.name AS subject_category,
        t.state_owner AS state_owner,
        t.partition_date,
        MIN(t.event_ts_msk) AS event_ts_msk
    FROM prebase_jl AS t
    LEFT JOIN {{ source('mongo', 'babylone_onfy_tags_daily_snapshot') }} AS a ON t.tag = a._id
    LEFT JOIN {{ source('mongo', 'babylone_onfy_tag_categories_daily_snapshot') }} AS b ON a.categoryId = b._id
    GROUP BY 1, 2, 3, 4, 5, 6
 )
,

tt_onfy AS (
    SELECT
        t.ticket_id,
        t.tag,
        t.tag_type,
        t.subject_category,
        t.event_ts_msk,
        t.partition_date,
        FIRST_VALUE(t.state_owner) OVER (PARTITION BY t.ticket_id, t.tag, t.tag_type, t.subject_category, t.partition_date ORDER BY t.event_ts_msk) AS state_owner
    FROM t_onfy AS t
),

base_onfy AS (
SELECT
    t.ticket_id,
    t.tag,
    t.tag_type,
    t.subject_category,
    t.state_owner,
    t.partition_date,
    MIN(t.event_ts_msk) AS event_ts_msk
FROM tt_onfy AS t
GROUP BY 1, 2, 3, 4, 5, 6
    ),

onfy AS (
SELECT
    'onfy' AS bu,
    t.ticket_id,
    t.state_owner,
    t.tag,
    t.tag_type,
    t.subject_category,
    TIMESTAMP(t.event_ts_msk) AS event_ts_msk,
    t.partition_date,
    a.first_queue,
    a.current_queue,
    a.country,
    a.language
FROM base_onfy AS t
LEFT JOIN {{ ref('support_mart_ticket_id_ext_onfy') }} AS a ON t.ticket_id = a.ticket_id
)


SELECT
    *
FROM marketplace
UNION DISTINCT
SELECT
    *
FROM jl
UNION DISTINCT
SELECT
    *
FROM joompay
UNION DISTINCT
SELECT
    *
FROM onfy
