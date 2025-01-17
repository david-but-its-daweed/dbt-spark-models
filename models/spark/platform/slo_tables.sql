{{ config(
    schema='platform_slo',
    materialized='view',
    meta = {
      'model_owner' : '@analytics.duty',
      'bigquery_load': 'true',
      'airflow_pool': 'k8s_platform_hourly_spark_tasks'
    },
    tags=['data_readiness']
) }}
SELECT DISTINCT
    sds.slo_id,
    hap.full_table_name AS table_name,
    "bq" AS table_type,
    "holistics" AS slo_type
FROM {{ ref("slo_details") }} AS sds
INNER JOIN {{ ref('holistics_api_data') }} AS hap
    ON TRIM(sds.business_name) = TRIM(hap.dashboard_title)
WHERE
    hap.full_table_name IS NOT NULL
    -- Exclude slo tables
    AND hap.full_table_name NOT IN (
        "platform_slo.slo_details",
        "models.slo_report_links",
        "platform_slo.data_readiness_summary",
        "platform_slo.slo_tables"
    )
    -- Exclude manual tables
    AND NOT hap.is_manual
UNION ALL
SELECT
    slo_id,
    table_name,
    table_type,
    "tables" AS slo_type
FROM {{ ref("slo_tables_seed") }}
WHERE
    1 = 1
    AND (table_name, table_type) NOT IN (SELECT
        mt.table_name,
        mt.`type`
    FROM {{ ref('manual_tables') }} AS mt)