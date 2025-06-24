{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'model_owner' : '@daweed',
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}

WITH session_landings AS (
    SELECT
        user_id,
        session_id,
        events.event_landing AS landing,
        COUNT(*) AS cnt
    FROM
        {{ source('b2b_mart', 'ss_events_by_session') }}
        LATERAL VIEW EXPLODE(events_in_session) AS events
    WHERE
        events.event_landing IS NOT NULL
    GROUP BY
        1, 2, 3
),

final AS (
    SELECT
        user_id,
        session_id,
        landing,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY cnt DESC, landing ASC) AS rn
    FROM session_landings
    WHERE landing IS NOT NULL
)

SELECT
    user_id,
    session_id,
    landing AS country_code
FROM final
WHERE rn = 1