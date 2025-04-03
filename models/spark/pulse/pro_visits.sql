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
        CASE WHEN interaction.utm_source = "pulse" AND interaction.utm_source IS NOT NULL THEN True ELSE False END AS pulse,
        interaction.utm_source,
        CASE WHEN interaction.utm_source = "pulse" AND interaction.utm_medium = "extension" THEN True ELSE False END AS extension,
        interaction.utm_medium,
        interaction.utm_campaign,
        interaction.user_id,
        interaction.visit_date
FROM {{ ref("fact_marketing_utm_interactions") }} AS interaction
