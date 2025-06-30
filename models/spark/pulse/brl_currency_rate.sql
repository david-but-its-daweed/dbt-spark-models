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


SELECT
    1000000 / rate AS rate,
    EXPLODE(
        SEQUENCE(
            effective_date,
            LEAST(next_effective_date - INTERVAL 1 DAY, CURRENT_DATE()), INTERVAL 1 DAY
        )
    ) AS date
FROM {{ source('mart','dim_currency_rate') }}
WHERE currency_code = "BRL"
