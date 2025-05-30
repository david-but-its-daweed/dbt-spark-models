{{ config(
    schema='joompro_analytics_internal_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'bigquery_load': 'true',
      'bigquery_project_id': 'joom-analytics-joompro-public',
      'bigquery_check_counts': 'false',
      'priority_weight': '150'
    }
) }}

WITH payments AS (
    SELECT entity_id as deal_id, min(event_ts_msk) AS paid_date 
    FROM {{ ref("fact_issues_statuses") }}
    WHERE status = "PaymentToMerchant"
    GROUP BY entity_id
)

SELECT
    f.*,
    p.paid_date
FROM {{ ref("fact_deals_with_requests") }} AS f
LEFT JOIN payments AS p ON p.deal_id = f.deal_id
WHERE f.utm_source = "pulse"
