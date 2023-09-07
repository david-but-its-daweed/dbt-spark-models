{{ config(
     schema='support',
     materialized='table',
     location_root='s3://joom-analytics-mart/support/nonpartitioned/',
     file_format='delta',
     meta = {
       'team': 'analytics',
       'bigquery_load': 'true',
       'bigquery_overwrite': 'true',
       'alerts_channel': "#olc_dbt_alerts",
       'model_owner' : '@ilypavlov'
     }
 ) }}
 
WITH last_ticket_review_id AS (
    SELECT
        payload.ticketId AS ticketId,
        payload.reviewedAgentId AS reviewedAgentId,
        MAX(payload.publishedAtTs) AS publishedAtTs
    FROM {{ source('mart', 'babylone_events') }}
    WHERE partition_date >= '2022-01-01'
        AND `type` = 'qaTicketReview'
        AND payload.operation = 'publish'
    GROUP BY 1, 2
),

scores AS (
    SELECT
        t.payload.ticketId,
        t.payload.reviewedAgentId,
        DATE(FROM_UNIXTIME(a.publishedAtTs / 1000)) AS partition_date,
        EXPLODE(t.payload.results.score) AS score,
        t.payload.results.maxScore
    FROM {{ source('mart', 'babylone_events') }} AS t
    JOIN last_ticket_review_id AS a
        ON t.payload.ticketId = a.ticketId
        AND t.payload.reviewedAgentId = a.reviewedAgentId
        AND t.payload.publishedAtTs = a.publishedAtTs
    WHERE t.partition_date >= '2022-01-01'
        AND t.`type` = 'qaTicketReview'
        AND t.payload.operation = 'publish'
),

base AS (
    SELECT
        ticketId,
        reviewedAgentId,
        partition_date,
        score,
        EXPLODE(maxScore) AS maxScore
    FROM scores
)

SELECT
    ticketId,
    reviewedAgentId,
    partition_date,
    SUM(COALESCE(score, 0)) / SUM(COALESCE(maxScore, 0)) AS qa_performance
FROM base
WHERE partition_date IS NOT NULL
GROUP BY 1, 2, 3
