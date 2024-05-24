{{ config(
    schema='b2b_mart',
    materialized='view',
    partition_by={
         "field": "event_msk_date"
    },
    meta = {
      'model_owner' : '@kirill_melnikov',
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}

SELECT
  user['userId'] AS user_id,
  type,
  event_ts_utc,
  CAST(event_ts_utc AS DATE) AS event_utc_date,
  event_ts_msk,
  CAST(event_ts_msk AS DATE) AS event_msk_date,
  payload.pageUrl AS pageUrl,
  CASE
    WHEN type = 'signIn' AND payload.signInType = 'phone' AND payload.success = TRUE THEN 1
  ELSE
  0
END
  AS autorisation,
  CASE
    WHEN type = 'selfServiceRegistrationFinished' THEN 1
  ELSE
  0
END
  AS registration,
  ROW_NUMBER() OVER (PARTITION BY user['userId'], type ORDER BY event_ts_msk) AS authentication_number
FROM {{ source('b2b_mart', 'device_events') }}
WHERE
  partition_date >= '2024-04-06'
  AND ( type = 'signIn'
    OR type = 'selfServiceRegistrationFinished')
